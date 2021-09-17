//! TODO #24 Fix the index implementation to use the locking layer

// Okay so more thinking, my approach needs to change
/*
    For adds, I'll find the leaf page using write locks, dropping as I go.

    Once found, I'll add and then follow the parents up until everything fits


*/

use super::index_formats::{BTreeBranchError, BTreeLeafError, BTreeNode, BTreeNodeError};
use super::page_formats::PageOffset;
use super::page_formats::{ItemIdData, PageId, PageType};
use super::row_formats::ItemPointer;
use super::{LockCacheManager, LockCacheManagerError, SelfEncodedSize};
use crate::{
    constants::PAGE_SIZE,
    engine::{
        io::index_formats::BTreeLeaf,
        objects::{Index, SqlTuple},
    },
};
use bytes::{Buf, BufMut, BytesMut};
use std::convert::TryFrom;
use std::mem::size_of;
use std::num::TryFromIntError;
use std::ops::Range;
use thiserror::Error;
use tokio::sync::OwnedRwLockWriteGuard;

//TODO Support something other than btrees
//TODO Support searching on a non primary index column

#[derive(Clone, Debug)]
pub struct IndexManager {
    lock_cache_manager: LockCacheManager,
}

impl IndexManager {
    pub fn new(lock_cache_manager: LockCacheManager) -> IndexManager {
        IndexManager { lock_cache_manager }
    }

    pub async fn add(
        &self,
        index_def: &Index,
        new_key: SqlTuple,
        item_ptr: ItemPointer,
    ) -> Result<(), IndexManagerError> {
        let page_id = PageId {
            resource_key: index_def.id,
            page_type: PageType::Data,
        };

        //Initial Special Case of an Empty Root
        let (mut current_page, mut current_offset) =
            self.get_root_page_for_write(index_def).await?;
        if let None = current_page.as_mut() {
            let root = BTreeLeaf::new();
            if !root.can_fit(&new_key) {
                return Err(IndexManagerError::KeyTooLarge(new_key.encoded_size()));
            }

            let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
            root.serialize(&mut buffer);
            current_page.replace(buffer);
            self.lock_cache_manager
                .update_page(page_id, current_offset, current_page)
                .await?;
            return Ok(());
        }

        //Next the goal is to get to the leaf
        if let Some(s) = current_page.as_mut() {
            let mut current_node = BTreeNode::parse(s, index_def)?;

            let mut found_leaf;
            loop {
                match current_node {
                    BTreeNode::Branch(b) => {
                        let next_page_offset = b.search(&new_key..&new_key)?;
                        current_page = self
                            .lock_cache_manager
                            .get_page_for_update(page_id, next_page_offset)
                            .await?;
                        current_offset = *next_page_offset;

                        let s = current_page
                            .as_mut()
                            .ok_or_else(|| IndexManagerError::NodeEmpty(current_offset))?;
                        current_node = BTreeNode::parse(s, index_def)?;
                        continue;
                    }
                    BTreeNode::Leaf(mut l) => {
                        found_leaf = l;
                        break;
                    }
                }
            }

            //If the key fits in the leaf, we add it and are done
            if found_leaf.can_fit(&new_key) {
                found_leaf.add(new_key, item_ptr);

                let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
                found_leaf.serialize(&mut buffer);

                current_page.replace(buffer);
                self.lock_cache_manager
                    .update_page(page_id, current_offset, current_page)
                    .await?;
                return Ok(());
            }

            //Doesn't fit so we have to split and work back up to the loop
            let left_node_offset = self.lock_cache_manager.get_offset_non_zero(page_id).await?;
            let right_node_offset = self.lock_cache_manager.get_offset_non_zero(page_id).await?;

            let mut left_node_page = self
                .lock_cache_manager
                .get_page_for_update(page_id, &left_node_offset)
                .await?;
            let mut right_node_page = self
                .lock_cache_manager
                .get_page_for_update(page_id, &right_node_offset)
                .await?;

            let (new_split, new_right_node) =
                found_leaf.add_and_split(left_node_offset, right_node_offset, new_key, item_ptr)?;

            let mut parent_node_offset = found_leaf
                .parent_node
                .ok_or_else(IndexManagerError::ParentNodeEmpty)?;

            let mut left_node_buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
            found_leaf.serialize(&mut left_node_buffer);

            let mut right_node_buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
            new_right_node.serialize(&mut right_node_buffer);

            left_node_page.replace(left_node_buffer);
            right_node_page.replace(right_node_buffer);

            self.lock_cache_manager
                .update_page(page_id, left_node_offset, left_node_page)
                .await?;
            self.lock_cache_manager
                .update_page(page_id, right_node_offset, right_node_page)
                .await?;

            //Now its time to fix the tree
            loop {
                let parent_page = self
                    .lock_cache_manager
                    .get_page_for_update(page_id, &parent_node_offset)
                    .await?;
                if parent_node_offset == PageOffset(0) {
                    //We've hit the top of the system so we'll have to remake the root page
                }
            }
        }
        Ok(())
    }

    async fn get_root_page_for_write(
        &self,
        index_def: &Index,
    ) -> Result<(OwnedRwLockWriteGuard<Option<BytesMut>>, PageOffset), IndexManagerError> {
        let page_id = PageId {
            resource_key: index_def.id,
            page_type: PageType::Data,
        };

        let mut first_page_handle = self
            .lock_cache_manager
            .get_page_for_update(page_id, &PageOffset(0))
            .await?;

        let (root_offset, changed) = match first_page_handle.as_mut() {
            Some(mut s) => {
                let root_offset = usize::try_from(s.get_uint_le(size_of::<usize>()))?;
                if root_offset == 0 {
                    //This is wrong, recreate it
                    let root_offset = self.lock_cache_manager.get_offset_non_zero(page_id).await?;

                    s.clear();
                    root_offset.serialize(&mut s);

                    (root_offset, true)
                } else {
                    (PageOffset(root_offset), false)
                }
            }
            None => {
                let root_offset = self.lock_cache_manager.get_offset_non_zero(page_id).await?;

                let mut first_page_buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
                root_offset.serialize(&mut first_page_buffer);
                let new_page = vec![0; PAGE_SIZE as usize - size_of::<usize>()];
                first_page_buffer.extend_from_slice(&new_page);

                first_page_handle.replace(first_page_buffer);

                (root_offset, true)
            }
        };

        //Now we know where root is, let's get it
        let root_page_handle = self
            .lock_cache_manager
            .get_page_for_update(page_id, &root_offset)
            .await?;

        if changed {
            self.lock_cache_manager
                .update_page(page_id, PageOffset(0), first_page_handle)
                .await?
        }

        Ok((root_page_handle, root_offset))
    }
    /*
    pub async fn search_for_key(
        &self,
        index_def: &Index,
        key: &SqlTuple,
    ) -> Result<Option<Vec<ItemPointer>>, IndexManagerError> {
        let (root_node, root_offset) = self.get_root_node(index_def).await?;
        match root_node {
            BTreeNode::Branch(b) => {
                todo!("blah")
            }
            BTreeNode::Leaf(l) => match l.nodes.get(key) {
                Some(s) => Ok(Some(s.clone())),
                None => Ok(None),
            },
        }
    }*/

    ///This function provides a mapping given an oversized bucket of how the leaf should be split
    /// Returns:
    /// * Left node range
    /// * Node for lifting up to the parent (will be the same as the last left entry in the list)
    /// * Right node range
    fn map_split_node(
        old_nodes_count: usize,
    ) -> Result<(Range<usize>, usize, Range<usize>), IndexManagerError> {
        if old_nodes_count < 2 {
            return Err(IndexManagerError::UnableToSplit(old_nodes_count));
        }

        let mut midpoint = old_nodes_count / 2;
        if old_nodes_count % 2 == 0 {
            midpoint += 1;
        }

        Ok((
            (0..midpoint - 1),
            midpoint - 1,
            (midpoint..old_nodes_count - 1),
        ))
    }

    /// This provides the requested node based on the page, if it exists
    async fn get_node(
        &self,
        index_def: &Index,
        offset: &PageOffset,
    ) -> Result<BTreeNode, IndexManagerError> {
        let page_id = PageId {
            resource_key: index_def.id,
            page_type: PageType::Data,
        };

        let page_handle = self.lock_cache_manager.get_page(page_id, offset).await?;
        let page_buffer = page_handle.clone();

        match page_buffer {
            Some(page) => Ok(BTreeNode::parse(&mut page.freeze(), index_def)?),
            None => Err(IndexManagerError::NoSuchNode(*offset)),
        }
    }
}

#[derive(Debug, Error)]
pub enum IndexManagerError {
    #[error(transparent)]
    BTreeBranchError(#[from] BTreeBranchError),
    #[error(transparent)]
    BTreeLeafError(#[from] BTreeLeafError),
    #[error(transparent)]
    BTreeNodeError(#[from] BTreeNodeError),
    #[error(
        "Another process made the root index page first, maybe the developer should make locking."
    )]
    ConcurrentCreationError(),
    #[error("Key too large size: {0}, maybe the developer should fix this.")]
    KeyTooLarge(usize),
    #[error(transparent)]
    LockCacheManagerError(#[from] LockCacheManagerError),
    #[error("Node does not exists {0}")]
    NoSuchNode(PageOffset),
    #[error("Node {0} empty")]
    NodeEmpty(PageOffset),
    #[error("Parent Node empty")]
    ParentNodeEmpty(),
    #[error("Root Node Empty")]
    RootNodeEmpty(),
    #[error("Unable to search, the stack is empty")]
    StackEmpty(),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error("Unable to split a node of size {0}")]
    UnableToSplit(usize),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::{
        constants::Nullable,
        engine::{
            io::{page_formats::UInt12, FileManager},
            objects::{
                types::{BaseSqlTypes, BaseSqlTypesMapper, SqlTypeDefinition},
                Attribute,
            },
        },
    };

    use super::*;

    fn get_key_and_ptr(num: usize) -> (SqlTuple, ItemPointer) {
        (
            SqlTuple(vec![
                Some(BaseSqlTypes::Text("test".to_string())),
                Some(BaseSqlTypes::Integer(num as u32)),
            ]),
            ItemPointer::new(PageOffset(num), UInt12::new(0).unwrap()),
        )
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let lm = LockCacheManager::new(fm);
        let im = IndexManager::new(lm);

        let index = Index {
            id: Uuid::new_v4(),
            name: "test".to_string(),
            columns: Arc::new(SqlTypeDefinition::new(&[
                Attribute::new(
                    "foo".to_string(),
                    BaseSqlTypesMapper::Text,
                    Nullable::NotNull,
                    None,
                ),
                Attribute::new(
                    "bar".to_string(),
                    BaseSqlTypesMapper::Integer,
                    Nullable::NotNull,
                    None,
                ),
            ])),
            unique: true,
        };

        for i in 0..5000 {
            let (key, ptr) = get_key_and_ptr(i);
            im.add(&index, key, ptr).await?;
        }

        Ok(())
    }
}
