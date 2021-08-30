//! File format for indexes is as follows (and does not follow postgres I don't think)
//! 1 byte: Type - Branch:0, Leaf: 1
//! byte sizeof<usize>: parent_node page, 0 if none
//! byte sizeof<usize>: left_node page, 0 if none
//! byte sizeof<usize>: right_node page, 0 if none
//! byte +: Pointers to Nodes(Branch) OR TablePages(Leaf)
//!
//! Branch Format:
//! * Count of keys in packed 7 bit numbers. Most signifigant bit says if the next byte should be considered
//! * For each key, a null mask and then the key.
//! * Pointers to pages with the next layer of leafs / nodes. We assume this is one more than the count of keys.
//!
//! Leaf Format:
//! * Count of keys in packed 7 bit numbers. Most signifigant bit says if the next byte should be considered
//! * For each key, a null mask, the key and then a pointer to the tuple.
//!
//! Please read https://en.wikipedia.org/wiki/B%2B_tree for links to the papers for implementation guidance.
//! I also found drawing the insertion process to be of tremendous help.
//!
//! Note: Min size for all indexes is 2x PAGE_SIZE since the root page is used to mean None. This will change
//! since the root page will have a pointer so we can lock and split the root node.

use crate::engine::io::page_formats::{ItemIdDataError, PageOffset};
use crate::engine::io::row_formats::NullMaskError;
use crate::engine::io::{parse_size, ConstEncodedSize, SizeError};
use crate::engine::objects::types::{BaseSqlTypes, BaseSqlTypesError};
use crate::engine::{
    io::{page_formats::ItemIdData, row_formats::NullMask},
    objects::{Index, SqlTuple},
};
use bytes::{Buf, BufMut};
use std::collections::BTreeMap;
use std::mem::size_of;
use std::{convert::TryFrom, num::TryFromIntError};
use thiserror::Error;

use super::{BTreeBranch, BTreeLeaf};

#[derive(Clone, Debug)]
pub enum BTreeNode {
    Branch(BTreeBranch),
    Leaf(BTreeLeaf),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NodeType {
    Branch = 1,
    Leaf = 0,
}

impl BTreeNode {
    pub fn write_node(
        buffer: &mut impl BufMut,
        node: Option<PageOffset>,
    ) -> Result<(), BTreeNodeError> {
        match node {
            Some(pn) => {
                let pn_u64 = u64::try_from(pn.0)?;
                buffer.put_uint_le(pn_u64, size_of::<usize>())
            }
            None => buffer.put_uint_le(0, size_of::<usize>()),
        }
        Ok(())
    }

    pub fn write_sql_tuple(buffer: &mut impl BufMut, tuple: &SqlTuple) {
        let nulls = NullMask::serialize(&tuple);
        buffer.put(nulls);

        tuple.serialize(buffer);
    }

    pub fn parse(buffer: &mut impl Buf, index_def: &Index) -> Result<BTreeNode, BTreeNodeError> {
        if buffer.remaining() < size_of::<u8>() {
            return Err(BTreeNodeError::MissingNodeTypeData(
                size_of::<u8>(),
                buffer.remaining(),
            ));
        }
        let node_type = buffer.get_u8();

        let parent_node = Self::parse_page(buffer)?;
        let left_node = Self::parse_page(buffer)?;
        let right_node = Self::parse_page(buffer)?;

        if node_type == NodeType::Leaf as u8 {
            let bucket_count = parse_size(buffer)?;
            let mut buckets = BTreeMap::new();

            for _ in 0..bucket_count {
                let bucket = Self::parse_sql_tuple(buffer, index_def)?;

                let item_count = parse_size(buffer)?;
                let mut items = vec![];
                for _ in 0..item_count {
                    let item_id = ItemIdData::parse(buffer)?;
                    items.push(item_id);
                }

                buckets.insert(bucket, items);
            }

            return Ok(BTreeNode::Leaf(BTreeLeaf {
                parent_node,
                left_node,
                right_node,
                nodes: buckets,
            }));
        } else {
            let keys_count = parse_size(buffer)?;
            let mut keys = Vec::with_capacity(keys_count);

            for _ in 0..keys_count {
                let key = Self::parse_sql_tuple(buffer, index_def)?;
                keys.push(key);
            }

            let pointers_count = keys_count + 1;

            let mut pointers = Vec::with_capacity(pointers_count);
            for _ in 0..pointers_count {
                if buffer.remaining() < PageOffset::encoded_size() {
                    return Err(BTreeNodeError::MissingPointerData(
                        size_of::<usize>(),
                        buffer.remaining(),
                    ));
                }
                let pointer = buffer.get_uint_le(PageOffset::encoded_size());
                let pointer = PageOffset(usize::try_from(pointer)?);

                pointers.push(pointer);
            }

            return Ok(BTreeNode::Branch(BTreeBranch {
                parent_node,
                left_node,
                right_node,
                keys,
                pointers,
            }));
        }
    }

    fn parse_page(buffer: &mut impl Buf) -> Result<Option<PageOffset>, BTreeNodeError> {
        if buffer.remaining() < size_of::<PageOffset>() {
            return Err(BTreeNodeError::MissingPointerData(
                size_of::<PageOffset>(),
                buffer.remaining(),
            ));
        }
        let value = buffer.get_uint_le(size_of::<PageOffset>());
        let mut node = None;
        if value != 0 {
            node = Some(PageOffset(usize::try_from(value)?));
        }
        Ok(node)
    }

    fn parse_sql_tuple(
        buffer: &mut impl Buf,
        index_def: &Index,
    ) -> Result<SqlTuple, BTreeNodeError> {
        let nulls = NullMask::parse(buffer, index_def.columns.len())?;
        let mut bucket = vec![];
        for c in 0..index_def.columns.len() {
            if nulls[c] {
                bucket.push(None);
            } else {
                let key = BaseSqlTypes::deserialize(&index_def.columns[c].1, buffer)?;
                bucket.push(Some(key));
            }
        }

        Ok(SqlTuple(bucket))
    }
}

#[derive(Debug, Error)]
pub enum BTreeNodeError {
    #[error(transparent)]
    BaseSqlTypesError(#[from] BaseSqlTypesError),
    #[error("Buffer too short to parse")]
    BufferTooShort(),
    #[error(transparent)]
    ItemIdDataError(#[from] ItemIdDataError),
    #[error("Key too large size: {0}")]
    KeyTooLarge(usize),
    #[error("Missing Data for Node Type need {0}, have {1}")]
    MissingNodeTypeData(usize, usize),
    #[error("Missing Data for Pointer need {0}, have {1}")]
    MissingPointerData(usize, usize),
    #[error(transparent)]
    NullMaskError(#[from] NullMaskError),
    #[error(transparent)]
    SizeError(#[from] SizeError),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
}
