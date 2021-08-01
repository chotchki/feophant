//! File format for indexes is as follows (and does not follow postgres I don't think)
//! 1 byte: Type - Branch:0, Leaf: 1
//! byte sizeof<usize>: parent_node page, 0 if none
//! byte sizeof<usize>: left_node page, 0 if none
//! byte sizeof<usize>: right_node page, 0 if none
//! byte +: Pointers to Nodes(Branch) OR TablePages(Leaf)

//! Pointer format is:
//! * Key (Array of values) -> Types are stored in Index Config
//! ** Count of keys in packed 7 bit numbers. Most signifigant bit says if the next byte should be considered
//! ** Each key then has the following
//! *** Null Mask
//! *** Serialized Columns

//! If a branch:
//! * sizeof<usize> bytes pointing to child page on the index
//! If a leaf:
//! * sizeof<usize> bytes pointing to table page
//! * 2 bytes pointing into count into page

use crate::engine::io::page_formats::ItemIdDataError;
use crate::engine::io::row_formats::NullMaskError;
use crate::engine::io::{encode_size, parse_size, SizeError};
use crate::engine::objects::types::{BaseSqlTypes, BaseSqlTypesError};
use crate::engine::{
    io::{page_formats::ItemIdData, row_formats::NullMask},
    objects::{Index, SqlTuple},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem::size_of;
use std::{convert::TryFrom, num::TryFromIntError};
use thiserror::Error;

#[derive(Clone, Debug)]
pub enum BTreeNode {
    Branch(BTreeBranch),
    Leaf(BTreeLeaf),
}

#[derive(Clone, Debug, PartialEq)]
pub struct BTreeBranch {
    pub parent_node: Option<BTreePage>,
    pub left_node: Option<BTreePage>,
    pub right_node: Option<BTreePage>,
    pub nodes: Vec<(SqlTuple, BTreePage)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BTreeLeaf {
    pub parent_node: Option<BTreePage>,
    pub left_node: Option<BTreePage>,
    pub right_node: Option<BTreePage>,
    pub nodes: Vec<(SqlTuple, ItemIdData)>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NodeType {
    Node,
    Leaf,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BTreePage(pub usize);

impl BTreeNode {
    fn write_node(buffer: &mut impl BufMut, node: Option<BTreePage>) -> Result<(), BTreeError> {
        match node {
            Some(pn) => {
                let pn_u64 = u64::try_from(pn.0)?;
                buffer.put_uint_le(pn_u64, size_of::<usize>())
            }
            None => buffer.put_uint_le(0, size_of::<usize>()),
        }
        Ok(())
    }

    fn write_sql_tuple(buffer: &mut impl BufMut, tuple: &SqlTuple) {
        let nulls = NullMask::serialize(&tuple, false);
        buffer.put(nulls);

        tuple.serialize(buffer);
    }

    pub fn parse(buffer: &mut impl Buf, index_def: &Index) -> Result<BTreeNode, BTreeError> {
        if buffer.remaining() < size_of::<u8>() {
            return Err(BTreeError::MissingNodeTypeData(
                size_of::<u8>(),
                buffer.remaining(),
            ));
        }
        let node_type = buffer.get_u8();

        let parent_node = Self::parse_page(buffer)?;
        let left_node = Self::parse_page(buffer)?;
        let right_node = Self::parse_page(buffer)?;

        if node_type == 0 {
            let bucket_count = parse_size(buffer)?;
            let mut buckets = Vec::with_capacity(bucket_count);

            for b in 0..bucket_count {
                let bucket = Self::parse_sql_tuple(buffer, index_def)?;

                let item_id = ItemIdData::parse(buffer)?;
                buckets.push((bucket, item_id));
            }

            return Ok(BTreeNode::Leaf(BTreeLeaf {
                parent_node,
                left_node,
                right_node,
                nodes: buckets,
            }));
        } else {
            let bucket_count = parse_size(buffer)?;
            let mut buckets = Vec::with_capacity(bucket_count);

            for b in 0..bucket_count {
                let bucket = Self::parse_sql_tuple(buffer, index_def)?;

                if buffer.remaining() < size_of::<usize>() {
                    return Err(BTreeError::MissingPointerData(
                        size_of::<usize>(),
                        buffer.remaining(),
                    ));
                }
                let pointer = buffer.get_uint_le(size_of::<usize>());
                let pointer = BTreePage(usize::try_from(pointer)?);

                buckets.push((bucket, pointer));
            }

            return Ok(BTreeNode::Branch(BTreeBranch {
                parent_node,
                left_node,
                right_node,
                nodes: buckets,
            }));
        }
    }

    fn parse_page(buffer: &mut impl Buf) -> Result<Option<BTreePage>, BTreeError> {
        if buffer.remaining() < size_of::<usize>() {
            return Err(BTreeError::MissingPointerData(
                size_of::<usize>(),
                buffer.remaining(),
            ));
        }
        let value = buffer.get_uint_le(size_of::<usize>());
        let mut node = None;
        if value != 0 {
            node = Some(BTreePage(usize::try_from(value)?));
        }
        Ok(node)
    }

    fn parse_sql_tuple(buffer: &mut impl Buf, index_def: &Index) -> Result<SqlTuple, BTreeError> {
        let nulls = NullMask::parse(buffer, index_def.columns.len())?;
        let mut bucket = vec![];
        for c in 0..index_def.columns.len() {
            if nulls[c] {
                bucket.push(None);
            } else {
                let key = BaseSqlTypes::deserialize(&index_def.columns[c].sql_type, buffer)?;
                bucket.push(Some(key));
            }
        }

        Ok(SqlTuple(bucket))
    }
}

impl BTreeBranch {
    //Am worried that this will be VERY expensive
    //pub fn can_fit(key_size: SqlTuple) -> bool {}

    pub fn serialize(&self) -> Result<Bytes, BTreeError> {
        let mut buffer = BytesMut::new();
        buffer.put_u8(1);

        BTreeNode::write_node(&mut buffer, self.parent_node)?;
        BTreeNode::write_node(&mut buffer, self.left_node)?;
        BTreeNode::write_node(&mut buffer, self.right_node)?;

        encode_size(&mut buffer, self.nodes.len());

        for (key, pointer) in self.nodes.iter() {
            BTreeNode::write_sql_tuple(&mut buffer, key);

            let pointer_u64 = u64::try_from(pointer.0)?;
            buffer.put_uint_le(pointer_u64, size_of::<usize>());
        }

        Ok(buffer.freeze())
    }
}

impl BTreeLeaf {
    //pub fn can_fit(key_size: SqlTuple) -> bool {}

    pub fn serialize(&self) -> Result<Bytes, BTreeError> {
        let mut buffer = BytesMut::new();
        buffer.put_u8(0);

        BTreeNode::write_node(&mut buffer, self.parent_node)?;
        BTreeNode::write_node(&mut buffer, self.left_node)?;
        BTreeNode::write_node(&mut buffer, self.right_node)?;

        encode_size(&mut buffer, self.nodes.len());

        for (key, item_id) in self.nodes.iter() {
            BTreeNode::write_sql_tuple(&mut buffer, key);

            buffer.put(item_id.serialize());
        }

        Ok(buffer.freeze())
    }
}

#[derive(Debug, Error)]
pub enum BTreeError {
    #[error(transparent)]
    BaseSqlTypesError(#[from] BaseSqlTypesError),
    #[error("Buffer too short to parse")]
    BufferTooShort(),
    #[error(transparent)]
    ItemIdDataError(#[from] ItemIdDataError),
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

#[cfg(test)]
mod tests {
    use crate::{
        constants::{Nullable, TableDefinitions},
        engine::{
            io::page_formats::UInt12,
            objects::{types::BaseSqlTypesMapper, Attribute, Table},
        },
    };
    use uuid::Uuid;

    use super::*;

    fn get_index() -> Index {
        let tbl_uuid = Uuid::new_v4();
        let attrs = vec![
            Attribute::new(
                "foo".to_string(),
                BaseSqlTypesMapper::Integer,
                Nullable::Null,
                None,
            ),
            Attribute::new(
                "bar".to_string(),
                BaseSqlTypesMapper::Text,
                Nullable::NotNull,
                None,
            ),
        ];

        let tbl = Table::new(tbl_uuid, "Test Table".to_string(), attrs);

        Index {
            id: Uuid::new_v4(),
            pg_class_id: Uuid::new_v4(),
            name: "TestIndex".to_string(),
            table: TableDefinitions::VALUES[0].value(),
            columns: tbl.attributes,
            unique: true,
        }
    }

    #[test]
    fn test_btree_branch_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let test = BTreeBranch {
            parent_node: None,
            left_node: Some(BTreePage(1)),
            right_node: Some(BTreePage(2)),
            nodes: vec![
                (
                    SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test".to_string()))]),
                    BTreePage(3),
                ),
                (
                    SqlTuple(vec![
                        Some(BaseSqlTypes::Integer(5)),
                        Some(BaseSqlTypes::Text("Test2".to_string())),
                    ]),
                    BTreePage(3),
                ),
            ],
        };

        let mut test_serial = test.clone().serialize()?;
        let test_parse = BTreeNode::parse(&mut test_serial, &get_index())?;

        match test_parse {
            BTreeNode::Branch(b) => assert_eq!(test, b),
            _ => assert!(false),
        }

        Ok(())
    }

    #[test]
    fn test_btree_leaf_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let test = BTreeLeaf {
            parent_node: None,
            left_node: Some(BTreePage(1)),
            right_node: Some(BTreePage(2)),
            nodes: vec![
                (
                    SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test".to_string()))]),
                    ItemIdData::new(UInt12::new(1)?, UInt12::new(2)?),
                ),
                (
                    SqlTuple(vec![
                        Some(BaseSqlTypes::Integer(5)),
                        Some(BaseSqlTypes::Text("Test2".to_string())),
                    ]),
                    ItemIdData::new(UInt12::new(3)?, UInt12::new(4)?),
                ),
            ],
        };

        let mut test_serial = test.clone().serialize()?;
        let test_parse = BTreeNode::parse(&mut test_serial, &get_index())?;

        match test_parse {
            BTreeNode::Leaf(l) => assert_eq!(test, l),
            _ => assert!(false),
        }

        Ok(())
    }
}
