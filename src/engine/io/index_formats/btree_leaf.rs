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
            row_formats::{ItemPointer, NullMask, NullMaskError},
            ConstEncodedSize, EncodedSize, SelfEncodedSize, SizeError,
        },
        objects::{types::BaseSqlTypesError, SqlTuple},
    },
};
use bytes::{BufMut, BytesMut};
use std::{collections::BTreeMap, num::TryFromIntError, ops::RangeBounds};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct BTreeLeaf {
    pub parent_node: Option<PageOffset>,
    pub left_node: Option<PageOffset>,
    pub right_node: Option<PageOffset>,
    pub nodes: BTreeMap<SqlTuple, Vec<ItemPointer>>,
}

impl BTreeLeaf {
    pub fn new() -> BTreeLeaf {
        BTreeLeaf {
            parent_node: None,
            left_node: None,
            right_node: None,
            nodes: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, key: SqlTuple, item_ptr: ItemPointer) -> Result<(), BTreeLeafError> {
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

    /// This function is used when the leaf is full and we need to split the contents into a new leaf, to the right.
    /// **WARNING** If this function fails the leaf should be considered poisoned and not used.
    pub fn add_and_split(
        &mut self,
        current_node: PageOffset,
        new_node: PageOffset,
        key: SqlTuple,
        item_ptr: ItemPointer,
    ) -> Result<(SqlTuple, BTreeLeaf), BTreeLeafError> {
        let key_size = key.encoded_size();

        //Unchecked add
        match self.nodes.get_mut(&key) {
            Some(iids) => iids.push(item_ptr),
            None => {
                self.nodes.insert(key, vec![item_ptr]);
            }
        }

        //Now we split
        let middle_entry = self
            .nodes
            .iter()
            .nth(self.nodes.len() / 2)
            .ok_or_else(BTreeLeafError::UnableToSplit)?
            .0
            .clone();

        let new_right_nodes = self.nodes.split_off(&middle_entry);
        let new_right = BTreeLeaf {
            parent_node: self.parent_node,
            left_node: Some(current_node),
            right_node: self.right_node,
            nodes: new_right_nodes,
        };

        self.right_node = Some(new_node);

        if self.encoded_size() > PAGE_SIZE.into() || new_right.encoded_size() > PAGE_SIZE.into() {
            return Err(BTreeLeafError::KeyTooLarge(key_size));
        }

        let new_split_point = self
            .nodes
            .iter()
            .rev()
            .next()
            .ok_or_else(BTreeLeafError::UnableToFindSplit)?;

        Ok((new_split_point.0.clone(), new_right))
    }

    pub fn can_fit(&self, new_key: &SqlTuple) -> bool {
        let new_key_present = self.nodes.contains_key(new_key);

        let mut new_size = 1 + (PageOffset::encoded_size() * 3); //Type plus pointers

        //The bucket length may change size
        if new_key_present {
            new_size += expected_encoded_size(self.nodes.len());
        } else {
            new_size += expected_encoded_size(self.nodes.len() + 1);

            new_size += NullMask::encoded_size(new_key);
            new_size += new_key.encoded_size();
            new_size += expected_encoded_size(1); //New Item Id
            new_size += ItemIdData::encoded_size()
        }

        for (tup, iids) in self.nodes.iter() {
            new_size += NullMask::encoded_size(tup);
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

    pub fn search<R>(&self, range: R) -> Vec<ItemPointer>
    where
        R: RangeBounds<SqlTuple>,
    {
        self.nodes
            .range(range)
            .flat_map(|(k, v)| v.clone())
            .collect()
    }

    pub fn serialize(&self, buffer: &mut BytesMut) -> Result<(), BTreeLeafError> {
        buffer.put_u8(NodeType::Leaf as u8);

        BTreeNode::write_node(buffer, self.parent_node)?;
        BTreeNode::write_node(buffer, self.left_node)?;
        BTreeNode::write_node(buffer, self.right_node)?;

        encode_size(buffer, self.nodes.len());

        for (key, iids) in self.nodes.iter() {
            BTreeNode::write_sql_tuple(buffer, key);

            encode_size(buffer, iids.len());
            for iid in iids {
                iid.serialize(buffer);
            }
        }

        //Zero pad to page size
        if buffer.len() < PAGE_SIZE as usize {
            let free_space = vec![0; PAGE_SIZE as usize - buffer.len()];
            buffer.extend_from_slice(&free_space);
        }

        Ok(())
    }
}

impl SelfEncodedSize for BTreeLeaf {
    fn encoded_size(&self) -> usize {
        let mut new_size = 1 + (PageOffset::encoded_size() * 3); //Type plus pointers

        new_size += expected_encoded_size(self.nodes.len());

        for (tup, iids) in self.nodes.iter() {
            new_size += NullMask::encoded_size(tup);
            new_size += tup.encoded_size();

            new_size += expected_encoded_size(iids.len());
            new_size += ItemIdData::encoded_size() * iids.len();
        }

        new_size
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
        engine::{
            io::page_formats::UInt12,
            objects::{
                types::{BaseSqlTypes, BaseSqlTypesMapper, SqlTypeDefinition},
                Attribute, Index,
            },
        },
    };
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
    fn test_btree_leaf_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = BTreeLeaf {
            parent_node: None,
            left_node: Some(PageOffset(1)),
            right_node: Some(PageOffset(2)),
            nodes: BTreeMap::new(),
        };
        let first_key = SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test".to_string()))]);
        let first_val = ItemPointer::new(PageOffset(1), UInt12::new(2)?);
        test.add(first_key, first_val)?;
        test.add(
            SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test2".to_string()))]),
            ItemPointer::new(PageOffset(3), UInt12::new(4)?),
        )?;

        let found = test.search(
            SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test".to_string()))])
                ..=SqlTuple(vec![None, Some(BaseSqlTypes::Text("Test".to_string()))]),
        );

        assert_eq!(
            found,
            vec![ItemPointer::new(PageOffset(1), UInt12::new(2)?)]
        );

        let mut test_serial = BytesMut::with_capacity(PAGE_SIZE as usize);
        test.serialize(&mut test_serial)?;
        let test_parse = match BTreeNode::parse(&mut test_serial, &get_index())? {
            BTreeNode::Leaf(l) => l,
            _ => {
                panic!("That's not a leaf!");
            }
        };

        assert_eq!(test, test_parse);

        Ok(())
    }
}
