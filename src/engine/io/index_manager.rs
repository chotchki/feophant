//! Start with a single bucket

//! Insert key and pointer to record
//!     read root
//!         search through buckets,

use std::collections::BTreeMap;

use super::index_formats::{BTreeError, BTreeNode};
use super::IOManager;
use super::{page_formats::ItemIdData, IOManagerError};
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
    io_manager: IOManager,
}

impl IndexManager {
    pub fn new(io_manager: IOManager) -> IndexManager {
        IndexManager { io_manager }
    }

    pub async fn add(
        &self,
        index_def: &Index,
        new_key: SqlTuple,
        item_ptr: ItemIdData,
    ) -> Result<(), IndexManagerError> {
        //TODO NOT THREADSAFE but will bomb out if it fails
        let mut root_node = match self.io_manager.get_page(&index_def.id, 1).await {
            Some(mut s) => BTreeNode::parse(&mut s, &index_def)?,
            None => {
                //Page zero with no data in it
                self.make_root_page(&index_def.id).await?;

                let root_node = BTreeLeaf {
                    parent_node: None,
                    left_node: None,
                    right_node: None,
                    nodes: BTreeMap::new(),
                };

                let page_num = self
                    .io_manager
                    .add_page(&index_def.id, root_node.serialize()?)
                    .await?;
                if page_num != 1 {
                    return Err(IndexManagerError::ConcurrentCreationError());
                }

                BTreeNode::Leaf(root_node)
            }
        };

        //Have the root node, now have to start searching
        match root_node {
            BTreeNode::Branch(branch) => {
                //Can't add to a branch, have to find a leaf
            }
            BTreeNode::Leaf(mut leaf) => {
                if leaf.can_fit(&new_key) {
                    leaf.add(new_key, item_ptr)?;
                } else {
                    return Err(IndexManagerError::KeyTooLarge(new_key.encoded_size()));
                }
            }
        }

        Ok(())
    }

    async fn make_root_page(&self, index: &Uuid) -> Result<(), IndexManagerError> {
        let mut root_page_buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        let root_page = vec![0; PAGE_SIZE as usize];
        root_page_buffer.extend_from_slice(&root_page);
        let page_num = self
            .io_manager
            .add_page(index, root_page_buffer.freeze())
            .await?;

        if page_num != 0 {
            return Err(IndexManagerError::ConcurrentCreationError());
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum IndexManagerError {
    #[error(transparent)]
    BTreeError(#[from] BTreeError),
    #[error(
        "Another process made the root index page first, maybe the developer should make locking."
    )]
    ConcurrentCreationError(),
    #[error(transparent)]
    IOManagerError(#[from] IOManagerError),
    #[error("Key too large size: {0}, maybe the developer should fix this.")]
    KeyTooLarge(usize),
}
