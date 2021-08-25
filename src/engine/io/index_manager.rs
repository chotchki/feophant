//! TODO #24 Fix the index implementation to use the locking layer

use super::index_formats::{BTreeLeafError, BTreeNode, BTreeNodeError};
use super::page_formats::PageOffset;
use super::page_formats::{ItemIdData, PageId, PageType};
use super::{FileManager, FileManagerError, LockCacheManager, LockCacheManagerError};
use crate::engine::io::SelfEncodedSize;
use crate::{
    constants::PAGE_SIZE,
    engine::{
        io::index_formats::BTreeLeaf,
        objects::{Index, SqlTuple},
    },
};
use bytes::{Buf, BufMut, BytesMut};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::mem::size_of;
use std::num::TryFromIntError;
use std::ops::Range;
use std::sync::Arc;
use thiserror::Error;

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
        item_ptr: ItemIdData,
    ) -> Result<(), IndexManagerError> {
        let mut search_stack = vec![];
        search_stack.push(self.get_root_node(index_def).await?);

        loop {
            let current_node = search_stack
                .pop()
                .ok_or_else(IndexManagerError::StackEmpty)?;

            match current_node.0 {
                BTreeNode::Branch(b) => {
                    //Now we need to scan to find our next traversal, the keys MUST be in order
                    for k in 0..=b.keys.len() {
                        let target_page = b.pointers[k];
                        if k == b.keys.len() {
                            //Prior loops hit all the other options, so down the right pointer we go
                            let next_node = self.get_node(index_def, &target_page).await?;
                            search_stack.push((BTreeNode::Branch(b), current_node.1));
                            search_stack.push((next_node, target_page));
                            break;
                        } else if new_key <= b.keys[k] {
                            let next_node = self.get_node(index_def, &target_page).await?;
                            search_stack.push((BTreeNode::Branch(b), current_node.1));
                            search_stack.push((next_node, target_page));
                            break;
                        }
                    }
                }
                BTreeNode::Leaf(l) => {
                    //Okay we've gotten to the bottom of the tree, so time to do the adds
                    search_stack.push((BTreeNode::Leaf(l), current_node.1));
                    break;
                }
            }
        }

        //At this point we should have a vec that traverses root->child->leaf of some depth
        //Now we do the add and whatever adjustments we need to do to fix the parents
        loop {
            let current_node = search_stack
                .pop()
                .ok_or_else(IndexManagerError::StackEmpty)?;

            match current_node.0 {
                BTreeNode::Branch(b) => {}
                BTreeNode::Leaf(mut l) => {
                    let page_id = PageId {
                        resource_key: index_def.id,
                        page_type: PageType::Data,
                    };

                    if l.can_fit(&new_key) {

                        //return Ok(self
                        //    .file_manager
                        //    .update_page(&page_id, &current_node.1, l.serialize()?)
                        //    .await?);
                    }

                    //If we're here, we have a key that doesn't fit into the leaf so we need to split it.
                    let mut new_nodes = l.nodes;
                    match new_nodes.get_mut(&new_key) {
                        Some(iids) => iids.push(item_ptr),
                        None => {
                            new_nodes.insert(new_key, vec![item_ptr]);
                        }
                    }

                    break;
                }
            }
        }

        Ok(())
    }

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

    // 1 2 3 4 5
    // 2 + 1?

    // 1 2 3 4 5 6
    // 3 + 1

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

        let page_handle = self.lock_cache_manager.get_page(page_id, *offset).await?;
        let page_buffer = page_handle.clone();

        match page_buffer {
            Some(page) => Ok(BTreeNode::parse(&mut page.freeze(), index_def)?),
            None => Err(IndexManagerError::NoSuchNode(*offset)),
        }
    }

    /// This provides the root node and makes it if its doesn't exist
    /// TODO - When locking is implemented, postgres has a far more elegant way to handle this
    ///     The first page becomes a pointer to root since root might not be page 1.
    async fn get_root_node(
        &self,
        index_def: &Index,
    ) -> Result<(BTreeNode, PageOffset), IndexManagerError> {
        let page_id = PageId {
            resource_key: index_def.id,
            page_type: PageType::Data,
        };
        let first_page_handle = self
            .lock_cache_manager
            .get_page(page_id, PageOffset(0))
            .await?;

        if let Some(s) = first_page_handle.as_ref() {
            let mut first_page = s.clone();
            return self
                .parse_root_page(index_def, &mut first_page, page_id)
                .await;
        }

        //We have to make it and handle the race window
        drop(first_page_handle);

        let mut new_first_page_handle = self
            .lock_cache_manager
            .get_page_for_update(page_id, PageOffset(0))
            .await?;

        if let Some(s) = new_first_page_handle.as_mut() {
            return self.parse_root_page(index_def, s, page_id).await;
        }

        let root_offset = self.lock_cache_manager.get_offset(page_id).await?;

        let mut new_page_buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        new_page_buffer.put_uint_le(u64::try_from(root_offset.0)?, size_of::<usize>());
        let new_page = vec![0; PAGE_SIZE as usize - size_of::<usize>()];

        new_page_buffer.extend_from_slice(&new_page);
        new_first_page_handle.replace(new_page_buffer);
        self.lock_cache_manager
            .add_page(page_id, PageOffset(0), new_first_page_handle)
            .await?;

        //Now make the root node and save it
        let mut root_handle = self
            .lock_cache_manager
            .get_page_for_update(page_id, root_offset)
            .await?;
        if let Some(s) = root_handle.as_mut() {
            return self.parse_root_page(index_def, s, page_id).await;
        }

        let root_node = BTreeLeaf::new();

        let mut root_buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        root_node.serialize(&mut root_buffer)?;
        root_handle.replace(root_buffer);

        self.lock_cache_manager
            .update_page(page_id, root_offset, root_handle)
            .await?;
        return Ok((BTreeNode::Leaf(root_node), root_offset));
    }

    async fn parse_root_page(
        &self,
        index_def: &Index,
        first_page: &mut BytesMut,
        page_id: PageId,
    ) -> Result<(BTreeNode, PageOffset), IndexManagerError> {
        let root_offset = usize::try_from(first_page.get_uint_le(size_of::<usize>()))?;
        let root_handle = self
            .lock_cache_manager
            .get_page(page_id, PageOffset(root_offset))
            .await?;
        let mut root_page = root_handle
            .as_ref()
            .ok_or(IndexManagerError::RootNodeEmpty())?
            .clone()
            .freeze();
        return Ok((
            BTreeNode::parse(&mut root_page, index_def)?,
            PageOffset(root_offset),
        ));
    }
}

#[derive(Debug, Error)]
pub enum IndexManagerError {
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
    #[error("Root Node Empty")]
    RootNodeEmpty(),
    #[error("Unable to search, the stack is empty")]
    StackEmpty(),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error("Unable to split a node of size {0}")]
    UnableToSplit(usize),
}
