use super::{
    btree_node::{BTreeNodeError, NodeType},
    BTreeNode,
};
use crate::{
    constants::PAGE_SIZE,
    engine::{
        io::{
            encode_size, expected_encoded_size,
            page_formats::{ItemIdData, ItemIdDataError, PageOffset},
            row_formats::{NullMask, NullMaskError},
            ConstEncodedSize, EncodedSize, SelfEncodedSize, SizeError,
        },
        objects::{types::BaseSqlTypesError, SqlTuple},
    },
};
use bytes::{BufMut, Bytes, BytesMut};
use std::{collections::BTreeMap, num::TryFromIntError};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct BTreeLeaf {
    pub parent_node: Option<PageOffset>,
    pub left_node: Option<PageOffset>,
    pub right_node: Option<PageOffset>,
    pub nodes: BTreeMap<SqlTuple, Vec<ItemIdData>>,
}

impl BTreeLeaf {
    pub fn add(&mut self, key: SqlTuple, item_ptr: ItemIdData) -> Result<(), BTreeLeafError> {
        if !self.can_fit(&key) {
            return Err(BTreeLeafError::KeyTooLarge(key.encoded_size()));
        }

        match self.nodes.get_mut(&key) {
            Some(iids) => iids.push(item_ptr),
            None => {
                self.nodes.insert(key, vec![item_ptr]);
            }
        }

        Ok(())
    }

    pub fn can_fit(&self, new_key: &SqlTuple) -> bool {
        let mut new_key_present = self.nodes.contains_key(&new_key);

        let mut new_size = 1 + (PageOffset::encoded_size() * 3); //Type plus pointers

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

    pub fn serialize(&self) -> Result<Bytes, BTreeLeafError> {
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
pub enum BTreeLeafError {
    #[error(transparent)]
    BaseSqlTypesError(#[from] BaseSqlTypesError),
    #[error(transparent)]
    BTreeNodeError(#[from] BTreeNodeError),
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
    use super::*;
    use crate::{
        constants::{Nullable, TableDefinitions},
        engine::{
            io::page_formats::UInt12,
            objects::{
                types::{BaseSqlTypes, BaseSqlTypesMapper},
                Attribute, Index, Table,
            },
        },
    };
    use uuid::Uuid;

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
    fn test_btree_leaf_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test".to_string()))]),
            vec![ItemIdData::new(UInt12::new(1)?, UInt12::new(2)?)],
        );
        nodes.insert(
            SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test2".to_string()))]),
            vec![ItemIdData::new(UInt12::new(3)?, UInt12::new(4)?)],
        );

        let test = BTreeLeaf {
            parent_node: None,
            left_node: Some(PageOffset(1)),
            right_node: Some(PageOffset(2)),
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
