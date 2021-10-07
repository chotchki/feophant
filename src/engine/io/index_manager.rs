//! TODO #24 Fix the index implementation to use the locking layer

// Okay so more thinking, my approach needs to change
/*
    lock leaf

    split _ cow

    lock left and right leaves

    check up splitting as we go

    write down, unlocking as we go

*/

use super::block_layer::lock_cache_manager::{LockCacheManager, LockCacheManagerError};
use super::format_traits::Parseable;
use super::index_formats::{
    BTreeBranchError, BTreeFirstPage, BTreeFirstPageError, BTreeLeafError, BTreeNode,
    BTreeNodeError,
};
use super::page_formats::PageOffset;
use super::page_formats::{PageId, PageType};
use super::row_formats::ItemPointer;
use crate::engine::io::format_traits::Serializable;
use crate::engine::io::index_formats::BTreeBranch;
use crate::engine::objects::{Index, SqlTuple};
use std::num::TryFromIntError;
use thiserror::Error;

mod find_leaf;
use find_leaf::find_leaf;
use find_leaf::FindLeafError;

mod split_leaf;
use split_leaf::split_leaf;
use split_leaf::SplitLeafError;

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

        debug!("Adding {:?}", new_key);

        //Find the target leaf
        let (mut page, page_offset, mut leaf) =
            find_leaf(&self.lock_cache_manager, index_def, &new_key).await?;

        debug!("target offset {0}", page_offset);
        //If the key fits in the leaf, we add it and are done
        if leaf.can_fit(&new_key) {
            debug!("fits");

            leaf.add(new_key, item_ptr)?;

            leaf.serialize_and_pad(&mut page);

            self.lock_cache_manager
                .update_page(page_id, page_offset, page)
                .await?;
            return Ok(());
        }

        debug!("expand");

        //Lock the leafs left and right if they exist
        let left_neighbor = leaf.left_node;
        let left_page = match left_neighbor {
            Some(s) => Some(
                self.lock_cache_manager
                    .get_page_for_update(page_id, &s)
                    .await?,
            ),
            None => None,
        };

        let right_neighbor = leaf.right_node;
        let right_page = match right_neighbor {
            Some(s) => Some(
                self.lock_cache_manager
                    .get_page_for_update(page_id, &s)
                    .await?,
            ),
            None => None,
        };

        //Doesn't fit so we have to split and work back up to the loop
        let (mut split_key, mut parent_node_offset, new_left_offset, new_right_offset) =
            split_leaf(&self.lock_cache_manager, index_def, leaf, new_key, item_ptr).await?;

        if let Some(mut s) = left_page {
            if let Some(s2) = s.as_mut() {
                if let BTreeNode::Leaf(mut l) = BTreeNode::parse(&mut s2.clone(), index_def)? {
                    l.right_node = Some(new_left_offset);
                    l.serialize_and_pad(&mut s);
                    self.lock_cache_manager
                        .update_page(page_id, left_neighbor.unwrap(), s)
                        .await?;
                } else {
                    return Err(IndexManagerError::UnexpectedBranch(left_neighbor.unwrap()));
                }
            }
        }

        if let Some(mut s) = right_page {
            if let Some(s2) = s.as_mut() {
                if let BTreeNode::Leaf(mut l) = BTreeNode::parse(&mut s2.clone(), index_def)? {
                    l.left_node = Some(new_right_offset);
                    l.serialize_and_pad(&mut s);
                    self.lock_cache_manager
                        .update_page(page_id, right_neighbor.unwrap(), s)
                        .await?;
                } else {
                    return Err(IndexManagerError::UnexpectedBranch(right_neighbor.unwrap()));
                }
            }
        }

        //Now its time to fix the tree
        loop {
            let mut parent_page = self
                .lock_cache_manager
                .get_page_for_update(page_id, &parent_node_offset)
                .await?;
            if parent_node_offset == PageOffset(0) {
                //We've hit the top of the system so we'll have to remake the root page
                let new_root_offset = self.lock_cache_manager.get_offset_non_zero(page_id).await?;

                let mut new_root_page = self
                    .lock_cache_manager
                    .get_page_for_update(page_id, &new_root_offset)
                    .await?;

                let new_root =
                    BTreeBranch::new(PageOffset(0), new_left_offset, split_key, new_right_offset);

                new_root.serialize_and_pad(&mut new_root_page);
                self.lock_cache_manager
                    .update_page(page_id, new_root_offset, new_root_page)
                    .await?;

                let first_page = BTreeFirstPage {
                    root_offset: new_root_offset,
                };
                first_page.serialize_and_pad(&mut parent_page);
                self.lock_cache_manager
                    .update_page(page_id, PageOffset(0), parent_page)
                    .await?;

                return Ok(());
            }
            if let Some(s) = parent_page.as_mut() {
                if let BTreeNode::Branch(mut b) = BTreeNode::parse(&mut s.clone(), index_def)? {
                    if b.can_fit(&split_key) {
                        b.add(new_left_offset, split_key, new_right_offset)?;

                        b.serialize_and_pad(&mut parent_page);

                        self.lock_cache_manager
                            .update_page(page_id, parent_node_offset, parent_page)
                            .await?;

                        return Ok(());
                    } else {
                        //Need to split the branch and move up a level
                        let (middle_key, new_right) =
                            b.add_and_split(new_left_offset, split_key, new_right_offset)?;

                        let new_right_offset =
                            self.lock_cache_manager.get_offset_non_zero(page_id).await?;
                        let mut new_right_page = self
                            .lock_cache_manager
                            .get_page_for_update(page_id, &new_right_offset)
                            .await?;
                        new_right.serialize_and_pad(&mut new_right_page);
                        self.lock_cache_manager
                            .update_page(page_id, new_right_offset, new_right_page)
                            .await?;

                        b.serialize_and_pad(&mut parent_page);
                        self.lock_cache_manager
                            .update_page(page_id, PageOffset(0), parent_page)
                            .await?;

                        parent_node_offset = b.parent_node;
                        split_key = middle_key;

                        continue;
                    }
                } else {
                    return Err(IndexManagerError::UnexpectedLeaf(parent_node_offset));
                }
            } else {
                return Err(IndexManagerError::NodeEmpty(parent_node_offset));
            }
        }
    }

    pub async fn search_for_key(
        &self,
        index_def: &Index,
        key: &SqlTuple,
    ) -> Result<Option<Vec<ItemPointer>>, IndexManagerError> {
        let page_id = PageId {
            resource_key: index_def.id,
            page_type: PageType::Data,
        };
        let first_page = self
            .lock_cache_manager
            .get_page(page_id, &PageOffset(0))
            .await?;

        if let Some(s) = first_page.as_ref() {
            let first_node = BTreeFirstPage::parse(&mut s.clone())?;

            let mut current_offset = first_node.root_offset;
            loop {
                debug!("scan {0}", current_offset);
                let node = self.get_node(index_def, &current_offset).await?;
                match node {
                    BTreeNode::Branch(b) => {
                        current_offset = *b.search(key..key)?;
                        continue;
                    }
                    BTreeNode::Leaf(l) => match l.nodes.get(key) {
                        Some(s) => return Ok(Some(s.clone())),
                        None => {
                            return Ok(None);
                        }
                    },
                }
            }
        } else {
            Ok(None)
        }
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
            Some(mut page) => Ok(BTreeNode::parse(&mut page, index_def)?),
            None => Err(IndexManagerError::NoSuchNode(*offset)),
        }
    }
}

#[derive(Debug, Error)]
pub enum IndexManagerError {
    #[error(transparent)]
    BTreeBranchError(#[from] BTreeBranchError),
    #[error(transparent)]
    BTreeFirstPageError(#[from] BTreeFirstPageError),
    #[error(transparent)]
    BTreeLeafError(#[from] BTreeLeafError),
    #[error(transparent)]
    BTreeNodeError(#[from] BTreeNodeError),
    #[error(
        "Another process made the root index page first, maybe the developer should make locking."
    )]
    ConcurrentCreationError(),
    #[error(transparent)]
    FindLeafError(#[from] FindLeafError),
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
    #[error(transparent)]
    SplitLeafError(#[from] SplitLeafError),
    #[error("Unable to search, the stack is empty")]
    StackEmpty(),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error("Unable to split a node of size {0}")]
    UnableToSplit(usize),
    #[error("Unexpect Branch at offset {0}")]
    UnexpectedBranch(PageOffset),
    #[error("Unexpect Leaf at offset {0}")]
    UnexpectedLeaf(PageOffset),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::{
        constants::Nullable,
        engine::{
            io::{block_layer::file_manager::FileManager, page_formats::UInt12},
            objects::{
                types::{BaseSqlTypes, BaseSqlTypesMapper, SqlTypeDefinition},
                Attribute,
            },
        },
    };

    use super::*;
    use log::LevelFilter;
    use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode};

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
        CombinedLogger::init(vec![TermLogger::new(
            LevelFilter::Debug,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )])?;

        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let lm = LockCacheManager::new(fm.clone());
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

        for i in 0..1000 {
            let (key, ptr) = get_key_and_ptr(i);
            im.add(&index, key, ptr).await?;
        }

        let (key, ptr) = get_key_and_ptr(999);
        assert_eq!(Some(vec![ptr]), im.search_for_key(&index, &key).await?);

        fm.shutdown().await?;

        Ok(())
    }
}
