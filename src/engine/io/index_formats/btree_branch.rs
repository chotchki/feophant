use super::{
    btree_node::{BTreeNodeError, NodeType},
    split_branch, BTreeNode, SplitBranchError,
};
use super::{index_search_start, IndexSearchError};
use crate::{
    constants::PAGE_SIZE,
    engine::{
        io::{
            encode_size, expected_encoded_size,
            format_traits::Serializable,
            page_formats::{ItemIdData, ItemIdDataError, PageOffset},
            row_formats::{NullMask, NullMaskError},
            ConstEncodedSize, EncodedSize, SelfEncodedSize, SizeError,
        },
        objects::{types::BaseSqlTypesError, SqlTuple},
    },
};
use bytes::BufMut;
use std::{num::TryFromIntError, ops::RangeBounds};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct BTreeBranch {
    pub parent_node: PageOffset,
    pub keys: Vec<SqlTuple>,
    pub pointers: Vec<PageOffset>,
}

impl BTreeBranch {
    pub fn new(
        parent_node: PageOffset,
        left_pointer: PageOffset,
        key: SqlTuple,
        right_pointer: PageOffset,
    ) -> BTreeBranch {
        BTreeBranch {
            parent_node,
            keys: vec![key],
            pointers: vec![left_pointer, right_pointer],
        }
    }

    pub fn add(
        &mut self,
        left_pointer: PageOffset,
        key: SqlTuple,
        right_pointer: PageOffset,
    ) -> Result<(), BTreeBranchError> {
        if !self.can_fit(&key) {
            return Err(BTreeBranchError::KeyTooLarge(key.encoded_size()));
        }

        //   0   2   3   4   5
        // a   c   d   e   f   g

        //   0   1   2   3   4   5
        // a   b   c   d   e   f   g

        //Find where the new key fits
        let mut new_key_loc = self.keys.len();
        for i in 0..self.keys.len() {
            if key > self.keys[i] {
                new_key_loc = i + 1;
                break;
            }
        }
        self.keys.insert(new_key_loc, key);

        self.pointers.remove(new_key_loc);
        self.pointers.insert(new_key_loc, right_pointer);
        self.pointers.insert(new_key_loc, left_pointer);

        Ok(())
    }

    /// This function is used when the branch is full and we need to split the contents into two new branches
    /// **WARNING** If this function fails the branch should be considered poisoned and not used.
    pub fn add_and_split(
        &mut self,
        left_pointer: PageOffset,
        key: SqlTuple,
        right_pointer: PageOffset,
    ) -> Result<(SqlTuple, BTreeBranch), BTreeBranchError> {
        let key_size = key.encoded_size();

        //Unchecked add
        let mut new_key_loc = self.keys.len();
        for i in 0..self.keys.len() {
            if key > self.keys[i] {
                new_key_loc = i + 1;
                break;
            }
        }
        self.keys.insert(new_key_loc, key);

        self.pointers.remove(new_key_loc);
        self.pointers.insert(new_key_loc, right_pointer);
        self.pointers.insert(new_key_loc, left_pointer);

        //Now we split
        let (middle, right_keys, right_pointers) =
            split_branch(&mut self.keys, &mut self.pointers)?;

        let new_right = BTreeBranch {
            parent_node: self.parent_node,
            keys: right_keys,
            pointers: right_pointers,
        };

        if self.encoded_size() > PAGE_SIZE.into() || new_right.encoded_size() > PAGE_SIZE.into() {
            return Err(BTreeBranchError::KeyTooLarge(key_size));
        }

        Ok((middle, new_right))
    }

    pub fn can_fit(&self, new_key: &SqlTuple) -> bool {
        let current_size = 1 + //Type
        (PageOffset::encoded_size()) + //Parent Pointer
        expected_encoded_size(self.keys.len() + 1) + //Length assuming inserted
        self.keys.iter().fold(0, |acc, tup| acc +
            NullMask::encoded_size(tup) +
            tup.encoded_size()) + //Keys
         NullMask::encoded_size(new_key) +  //New key null mask
        new_key.encoded_size() + //New Key
        ItemIdData::encoded_size() * (self.keys.len() + 1); //Pointers to nodes

        current_size <= PAGE_SIZE as usize
    }

    /// Finds the first PageOffset that satisfys the range
    pub fn search<'a, R>(&'a self, range: R) -> Result<&'a PageOffset, BTreeBranchError>
    where
        R: RangeBounds<SqlTuple>,
    {
        if self.keys.is_empty() {
            return Err(BTreeBranchError::MissingKeys());
        }

        Ok(index_search_start(&self.keys, &self.pointers, range)?)
    }
}

impl SelfEncodedSize for BTreeBranch {
    fn encoded_size(&self) -> usize {
        let mut new_size = 1 + (PageOffset::encoded_size()); //Type plus pointer

        new_size += expected_encoded_size(self.keys.len());
        for tup in self.keys.iter() {
            new_size += NullMask::encoded_size(tup);
            new_size += tup.encoded_size();
        }

        new_size += self.pointers.len() * PageOffset::encoded_size();

        new_size
    }
}

impl Serializable for BTreeBranch {
    fn serialize(&self, buffer: &mut impl BufMut) {
        buffer.put_u8(NodeType::Branch as u8);

        BTreeNode::write_node(buffer, Some(self.parent_node));

        encode_size(buffer, self.keys.len());

        for key in self.keys.iter() {
            BTreeNode::write_sql_tuple(buffer, key);
        }

        self.pointers.iter().for_each(|p| p.serialize(buffer));
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
    IndexSearchError(#[from] IndexSearchError),
    #[error(transparent)]
    ItemIdDataError(#[from] ItemIdDataError),
    #[error("Key too large size: {0}")]
    KeyTooLarge(usize),
    #[error("No keys to search")]
    MissingKeys(),
    #[error("Missing Data for Node Type need {0}, have {1}")]
    MissingNodeTypeData(usize, usize),
    #[error("Missing Data for Pointer need {0}, have {1}")]
    MissingPointerData(usize, usize),
    #[error(transparent)]
    NullMaskError(#[from] NullMaskError),
    #[error(transparent)]
    SizeError(#[from] SizeError),
    #[error(transparent)]
    SplitBranchError(#[from] SplitBranchError),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error("Unable to split")]
    UnableToSplit(),
    #[error("Unable to find split point")]
    UnableToFindSplit(),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        constants::Nullable,
        engine::objects::{
            types::{BaseSqlTypes, BaseSqlTypesMapper, SqlTypeDefinition},
            Attribute, Index,
        },
    };
    use bytes::BytesMut;
    use uuid::Uuid;

    fn get_index() -> Index {
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
            parent_node: PageOffset(1),
            keys,
            pointers,
        };

        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        test.serialize(&mut buffer);
        let test_parse = BTreeNode::parse(&mut buffer, &get_index())?;

        match test_parse {
            BTreeNode::Branch(b) => assert_eq!(test, b),
            _ => panic!("Not a branch"),
        }

        Ok(())
    }
}
