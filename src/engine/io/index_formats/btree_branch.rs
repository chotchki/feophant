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
use std::{convert::TryFrom, num::TryFromIntError};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct BTreeBranch {
    pub parent_node: Option<PageOffset>,
    pub left_node: Option<PageOffset>,
    pub right_node: Option<PageOffset>,
    pub keys: Vec<SqlTuple>,
    pub pointers: Vec<PageOffset>,
}

impl BTreeBranch {
    //TODO An add function doesn't seem to make sense

    pub fn can_fit(&self, new_keys: SqlTuple) -> bool {
        let current_size = 1 + //Type
        (PageOffset::encoded_size() * 3) + //Pointers
        expected_encoded_size(self.keys.len() + 1) + //Length assuming inserted
        self.keys.iter().fold(0, |acc, tup| acc +
            NullMask::encoded_size(&tup) +
            tup.encoded_size()) + //Keys
         NullMask::encoded_size(&new_keys) +  //New key null mask
        new_keys.encoded_size() + //New Key
        ItemIdData::encoded_size() * (self.keys.len() + 1); //Pointers to nodes

        current_size <= PAGE_SIZE as usize
    }

    pub fn serialize(&self) -> Result<Bytes, BTreeBranchError> {
        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        buffer.put_u8(NodeType::Branch as u8);

        BTreeNode::write_node(&mut buffer, self.parent_node)?;
        BTreeNode::write_node(&mut buffer, self.left_node)?;
        BTreeNode::write_node(&mut buffer, self.right_node)?;

        encode_size(&mut buffer, self.keys.len());

        for key in self.keys.iter() {
            BTreeNode::write_sql_tuple(&mut buffer, key);
        }

        for pointer in self.pointers.iter() {
            let pointer_u64 = u64::try_from(pointer.0)?;
            buffer.put_uint_le(pointer_u64, PageOffset::encoded_size());
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
pub enum BTreeBranchError {
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
    use std::sync::Arc;

    use super::*;
    use crate::{
        constants::Nullable,
        engine::objects::{
            types::{BaseSqlTypes, BaseSqlTypesMapper, SqlTypeDefinition},
            Attribute, Index, Table,
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

        Index {
            id: Uuid::new_v4(),
            name: "TestIndex".to_string(),
            columns: Arc::new(SqlTypeDefinition::new(&attrs)),
            unique: true,
        }
    }

    #[test]
    fn test_btree_branch_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let keys = vec![
            SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test".to_string()))]),
            SqlTuple(vec![
                Some(BaseSqlTypes::Integer(5)),
                Some(BaseSqlTypes::Text("Test2".to_string())),
            ]),
        ];

        let pointers = vec![PageOffset(3), PageOffset(3), PageOffset(3)];

        let test = BTreeBranch {
            parent_node: None,
            left_node: Some(PageOffset(1)),
            right_node: Some(PageOffset(2)),
            keys,
            pointers,
        };

        let mut test_serial = test.clone().serialize()?;
        let test_parse = BTreeNode::parse(&mut test_serial, &get_index())?;

        match test_parse {
            BTreeNode::Branch(b) => assert_eq!(test, b),
            _ => assert!(false),
        }

        Ok(())
    }
}
