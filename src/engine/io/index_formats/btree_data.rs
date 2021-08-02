//! File format for indexes is as follows (and does not follow postgres I don't think)
//! 1 byte: Type - Branch:0, Leaf: 1
//! byte sizeof<usize>: parent_node page, 0 if none
//! byte sizeof<usize>: left_node page, 0 if none
//! byte sizeof<usize>: right_node page, 0 if none
//! byte +: Pointers to Nodes(Branch) OR TablePages(Leaf)
//!
//! Pointer format is:
//! * Key (Array of values) -> Types are stored in Index Config
//! ** Count of keys in packed 7 bit numbers. Most signifigant bit says if the next byte should be considered
//! ** Each key then has the following
//! *** Null Mask
//! *** Serialized Columns
//!
//! If a branch:
//! * sizeof<usize> bytes pointing to child page on the index
//! If a leaf:
//! * count of leafs in packed 7bit numbers. Most signifigant bit says if the next byte should be considered
//! * sizeof<usize> bytes pointing to table page
//! * 2 bytes pointing into count into page
//!
//! Note: Min size for all indexes is 2x PAGE_SIZE since the root page is used to mean None

use crate::constants::PAGE_SIZE;
use crate::engine::io::page_formats::ItemIdDataError;
use crate::engine::io::row_formats::NullMaskError;
use crate::engine::io::{
    encode_size, expected_encoded_size, parse_size, ConstEncodedSize, EncodedSize, SelfEncodedSize,
    SizeError,
};
use crate::engine::objects::types::{BaseSqlTypes, BaseSqlTypesError};
use crate::engine::{
    io::{page_formats::ItemIdData, row_formats::NullMask},
    objects::{Index, SqlTuple},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::{BTreeMap, BinaryHeap};
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
    pub nodes: BTreeMap<SqlTuple, BTreePage>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BTreeLeaf {
    pub parent_node: Option<BTreePage>,
    pub left_node: Option<BTreePage>,
    pub right_node: Option<BTreePage>,
    pub nodes: BTreeMap<SqlTuple, Vec<ItemIdData>>,
}

//TODO delete
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NodeType {
    Branch = 1,
    Leaf = 0,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
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
        let nulls = NullMask::serialize(&tuple);
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
            let bucket_count = parse_size(buffer)?;
            let mut buckets = BTreeMap::new();

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

                buckets.insert(bucket, pointer);
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
        if buffer.remaining() < size_of::<BTreePage>() {
            return Err(BTreeError::MissingPointerData(
                size_of::<BTreePage>(),
                buffer.remaining(),
            ));
        }
        let value = buffer.get_uint_le(size_of::<BTreePage>());
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
    pub fn can_fit(&self, new_keys: SqlTuple) -> bool {
        let current_size = 1 + //Type
        (size_of::<BTreePage>() * 3) + //Pointers
        expected_encoded_size(self.nodes.len() + 1) + //Length assuming inserted
        self.nodes.iter().fold(0, |acc, (tup, _)| acc + 
            NullMask::encoded_size(&tup) +  //Null
            tup.encoded_size() + //Keys
            size_of::<BTreePage>()); //Pointer to rowdata
        
        let new_size = NullMask::encoded_size(&new_keys) +  //Null
        new_keys.encoded_size() + //Keys
        ItemIdData::encoded_size();

        current_size + new_size <= PAGE_SIZE as usize 
    }
    
    pub fn serialize(&self) -> Result<Bytes, BTreeError> {
        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        buffer.put_u8(NodeType::Branch as u8);

        BTreeNode::write_node(&mut buffer, self.parent_node)?;
        BTreeNode::write_node(&mut buffer, self.left_node)?;
        BTreeNode::write_node(&mut buffer, self.right_node)?;

        encode_size(&mut buffer, self.nodes.len());

        for (key, pointer) in self.nodes.iter() {
            BTreeNode::write_sql_tuple(&mut buffer, key);

            let pointer_u64 = u64::try_from(pointer.0)?;
            buffer.put_uint_le(pointer_u64, size_of::<BTreePage>());
        }

        //Zero pad to page size
        if buffer.len() < PAGE_SIZE as usize {
            let free_space = vec![0; PAGE_SIZE as usize - buffer.len()];
            buffer.extend_from_slice(&free_space);
        }

        Ok(buffer.freeze())
    }
}

impl BTreeLeaf {
    
    pub fn add(&mut self, key: SqlTuple, item_ptr: ItemIdData) -> Result<(), BTreeError> {
        if !self.can_fit(&key){
            return Err(BTreeError::KeyTooLarge(key.encoded_size()));
        }

        match self.nodes.get_mut(&key) {
            Some(iids) => iids.push(item_ptr),
            None => {self.nodes.insert(key, vec![item_ptr]);}
        }

        Ok(())
    }


    pub fn can_fit(&self, new_key: &SqlTuple) -> bool {
        let mut new_key_present = self.nodes.contains_key(&new_key);

        let mut new_size = 1 + (size_of::<BTreePage>() * 3); //Type plus pointers 

        //The bucket length may change size
        if new_key_present {
            new_size += expected_encoded_size(self.nodes.len());
        } else {
            new_size += expected_encoded_size(self.nodes.len() + 1);

            new_size += NullMask::encoded_size(&new_key);
            new_size += new_key.encoded_size();
            new_size += expected_encoded_size(1); //New Item Id
            new_size += ItemIdData::encoded_size()
        }

        for (tup, iids) in self.nodes.iter() {
            new_size += NullMask::encoded_size(&tup);
            new_size += tup.encoded_size();

            if new_key_present && tup == new_key {
                new_size += expected_encoded_size(iids.len() + 1);
                new_size += ItemIdData::encoded_size() * (iids.len() + 1);
            } else {
                new_size += expected_encoded_size(iids.len());
                new_size += ItemIdData::encoded_size() * iids.len();
            }
        }

        new_size <= PAGE_SIZE as usize 
    }

    pub fn serialize(&self) -> Result<Bytes, BTreeError> {
        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        buffer.put_u8(NodeType::Leaf as u8);

        BTreeNode::write_node(&mut buffer, self.parent_node)?;
        BTreeNode::write_node(&mut buffer, self.left_node)?;
        BTreeNode::write_node(&mut buffer, self.right_node)?;

        encode_size(&mut buffer, self.nodes.len());

        for (key, iids) in self.nodes.iter() {
            BTreeNode::write_sql_tuple(&mut buffer, key);

            encode_size(&mut buffer, iids.len());
            for iid in iids {
                iid.serialize(&mut buffer);
            }
        }


        //Zero pad to page size
        if buffer.len() < PAGE_SIZE as usize {
            let free_space = vec![0; PAGE_SIZE as usize - buffer.len()];
            buffer.extend_from_slice(&free_space);
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
        let mut nodes = BTreeMap::new();
        nodes.insert(SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test".to_string()))]),  BTreePage(3));
        nodes.insert(SqlTuple(vec![Some(BaseSqlTypes::Integer(5)), Some(BaseSqlTypes::Text("Test2".to_string()))]), BTreePage(3));

        let test = BTreeBranch {
            parent_node: None,
            left_node: Some(BTreePage(1)),
            right_node: Some(BTreePage(2)),
            nodes
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
        let mut nodes = BTreeMap::new();
        nodes.insert(SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test".to_string()))]), vec![ ItemIdData::new(UInt12::new(1)?, UInt12::new(2)?)]);
        nodes.insert(SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test2".to_string()))]), vec![ ItemIdData::new(UInt12::new(3)?, UInt12::new(4)?)]);

        let test = BTreeLeaf {
            parent_node: None,
            left_node: Some(BTreePage(1)),
            right_node: Some(BTreePage(2)),
            nodes,
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
