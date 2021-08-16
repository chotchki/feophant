//! Start with a single bucket

//! Insert key and pointer to record
//!     read root
//!         search through buckets,

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use super::index_formats::{BTreeLeafError, BTreeNode, BTreeNodeError};
use super::page_formats::PageOffset;
use super::page_formats::{ItemIdData, PageId, PageType};
use super::{FileManager, FileManagerError};
use crate::engine::io::SelfEncodedSize;
use crate::{
    constants::PAGE_SIZE,
    engine::{
        io::index_formats::BTreeLeaf,
        objects::{Index, SqlTuple},
    },
};
use bytes::BytesMut;
use thiserror::Error;
use uuid::Uuid;

//TODO Support something other than btrees
//TODO Support searching on a non primary index column

#[derive(Clone, Debug)]
pub struct IndexManager {
    file_manager: Arc<FileManager>,
}

impl IndexManager {
    pub fn new(file_manager: Arc<FileManager>) -> IndexManager {
        IndexManager { file_manager }
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
                        return Ok(self
                            .file_manager
                            .update_page(&page_id, l.serialize()?, &current_node.1)
                            .await?);
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

        match self.file_manager.get_page(&page_id, offset).await? {
            Some(mut page) => Ok(BTreeNode::parse(&mut page, index_def)?),
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
        match self.get_node(index_def, &PageOffset(1)).await {
            Ok(o) => Ok((o, PageOffset(1))),
            Err(IndexManagerError::NoSuchNode(_)) => {
                let page_id = PageId {
                    resource_key: index_def.id,
                    page_type: PageType::Data,
                };

                //Page zero with no data in it
                self.make_root_page(&page_id).await?;

                let root_node = BTreeLeaf {
                    parent_node: None,
                    left_node: None,
                    right_node: None,
                    nodes: BTreeMap::new(),
                };

                let page_num = self
                    .file_manager
                    .add_page(&page_id, root_node.serialize()?)
                    .await?;
                if page_num != PageOffset(1) {
                    return Err(IndexManagerError::ConcurrentCreationError());
                }

                Ok((BTreeNode::Leaf(root_node), page_num))
            }
            Err(e) => Err(e),
        }
    }

    async fn make_root_page(&self, index: &PageId) -> Result<(), IndexManagerError> {
        let mut root_page_buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        let root_page = vec![0; PAGE_SIZE as usize];
        root_page_buffer.extend_from_slice(&root_page);
        let page_num = self
            .file_manager
            .add_page(index, root_page_buffer.freeze())
            .await?;

        if page_num != PageOffset(0) {
            return Err(IndexManagerError::ConcurrentCreationError());
        }

        Ok(())
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
    #[error(transparent)]
    FileManagerError(#[from] FileManagerError),
    #[error("Key too large size: {0}, maybe the developer should fix this.")]
    KeyTooLarge(usize),
    #[error("Node does not exists {0}")]
    NoSuchNode(PageOffset),
    #[error("Unable to search, the stack is empty")]
    StackEmpty(),
    #[error("Unable to split a node of size {0}")]
    UnableToSplit(usize),
}
