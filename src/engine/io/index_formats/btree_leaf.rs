use super::{
    btree_node::{BTreeNodeError, NodeType},
    BTreeNode,
};
use crate::{
    constants::PAGE_SIZE,
    engine::{
        io::{
            encode_size, expected_encoded_size,
            format_traits::Serializable,
            page_formats::PageOffset,
            row_formats::{ItemPointer, NullMask, NullMaskError},
            ConstEncodedSize, EncodedSize, SelfEncodedSize, SizeError,
        },
        objects::{types::BaseSqlTypesError, SqlTuple},
    },
};
use bytes::BufMut;
use std::{collections::BTreeMap, num::TryFromIntError, ops::RangeBounds};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct BTreeLeaf {
    pub parent_node: PageOffset,
    pub left_node: Option<PageOffset>,
    pub right_node: Option<PageOffset>,
    pub nodes: BTreeMap<SqlTuple, Vec<ItemPointer>>,
}

impl BTreeLeaf {
    pub fn new(parent_node: PageOffset) -> BTreeLeaf {
        BTreeLeaf {
            parent_node,
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
            new_size += ItemPointer::encoded_size()
        }

        for (tup, ips) in self.nodes.iter() {
            new_size += NullMask::encoded_size(tup);
            new_size += tup.encoded_size();

            if new_key_present && tup == new_key {
                new_size += expected_encoded_size(ips.len() + 1);
                new_size += ItemPointer::encoded_size() * (ips.len() + 1);
            } else {
                new_size += expected_encoded_size(ips.len());
                new_size += ItemPointer::encoded_size() * ips.len();
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
}

impl SelfEncodedSize for BTreeLeaf {
    fn encoded_size(&self) -> usize {
        let mut new_size = 1 + (PageOffset::encoded_size() * 3); //Type plus pointers

        new_size += expected_encoded_size(self.nodes.len());

        for (tup, ips) in self.nodes.iter() {
            new_size += NullMask::encoded_size(tup);
            new_size += tup.encoded_size();

            new_size += expected_encoded_size(ips.len());
            new_size += ItemPointer::encoded_size() * ips.len();
        }

        new_size
    }
}

impl Serializable for BTreeLeaf {
    fn serialize(&self, buffer: &mut impl BufMut) {
        buffer.put_u8(NodeType::Leaf as u8);

        self.parent_node.serialize(buffer);
        BTreeNode::write_node(buffer, self.left_node);
        BTreeNode::write_node(buffer, self.right_node);

        encode_size(buffer, self.nodes.len());

        for (key, ips) in self.nodes.iter() {
            BTreeNode::write_sql_tuple(buffer, key);

            encode_size(buffer, ips.len());
            for ip in ips {
                ip.serialize(buffer);
            }
        }
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

    //Super unsafe function to get test data, just don't count too high
    fn get_key(index: usize) -> (SqlTuple, ItemPointer) {
        (
            SqlTuple(vec![Some(BaseSqlTypes::Integer(index as u32))]),
            ItemPointer::new(PageOffset(index), UInt12::new(index as u16).unwrap()),
        )
    }

    #[test]
    fn sizes_match() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = BTreeLeaf {
            parent_node: PageOffset(1),
            left_node: Some(PageOffset(2)),
            right_node: Some(PageOffset(3)),
            nodes: BTreeMap::new(),
        };
        test.add(
            SqlTuple(vec![
                Some(BaseSqlTypes::Text("test".to_string())),
                Some(BaseSqlTypes::Integer(0)),
            ]),
            ItemPointer::new(PageOffset(1), UInt12::new(2)?),
        )?;
        let calc_len = test.encoded_size();

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);

        assert_eq!(calc_len, buffer.freeze().len());
        Ok(())
    }

    #[test]
    fn test_btree_leaf_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = BTreeLeaf {
            parent_node: PageOffset(1),
            left_node: Some(PageOffset(2)),
            right_node: Some(PageOffset(3)),
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
        test.serialize(&mut test_serial);
        let test_parse = match BTreeNode::parse(&mut test_serial, &get_index())? {
            BTreeNode::Leaf(l) => l,
            _ => {
                panic!("That's not a leaf!");
            }
        };

        assert_eq!(test, test_parse);

        Ok(())
    }

    #[test]
    fn test_btree_leaf_split() -> Result<(), Box<dyn std::error::Error>> {
        let mut leaf = BTreeLeaf::new(PageOffset(1));

        let mut i = 0;
        loop {
            let (key, ptr) = get_key(i);
            if leaf.can_fit(&key) {
                leaf.add(key, ptr)?;
            } else {
                break;
            }
            i += 1;
        }

        //Now let's split
        let leaf_size = leaf.nodes.len();
        let (key, ptr) = get_key(i);
        let (split_key, split_right) =
            leaf.add_and_split(PageOffset(2), PageOffset(3), key, ptr)?;

        assert_eq!(leaf_size + 1, leaf.nodes.len() + split_right.nodes.len());
        assert!(leaf_size > leaf.nodes.len());
        assert!(leaf_size > split_right.nodes.len());

        for n in leaf.nodes {
            assert!(n.0 <= split_key);
        }
        for n in split_right.nodes {
            assert!(n.0 > split_key);
        }

        assert_eq!(leaf.right_node, Some(PageOffset(3)));
        assert_eq!(split_right.left_node, Some(PageOffset(2)));

        Ok(())
    }
}
