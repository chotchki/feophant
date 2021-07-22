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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem::size_of;
use std::{convert::TryFrom, num::TryFromIntError};
use thiserror::Error;

use crate::{
    constants::BuiltinSqlTypes,
    engine::{
        io::{page_formats::ItemIdData, row_formats::NullMask},
        objects::{Index, SqlTuple},
    },
};

#[derive(Clone, Debug)]
pub enum BTreeNode {
    Branch(BTreeBranch),
    Leaf(BTreeLeaf),
}

#[derive(Clone, Debug)]
pub struct BTreeBranch {
    pub parent_node: Option<BTreePage>,
    pub left_node: Option<BTreePage>,
    pub right_node: Option<BTreePage>,
    pub nodes: Vec<(SqlTuple, BTreePage)>,
}

#[derive(Clone, Debug)]
pub struct BTreeLeaf {
    pub parent_node: Option<BTreePage>,
    pub left_node: Option<BTreePage>,
    pub right_node: Option<BTreePage>,
    pub nodes: Vec<(SqlTuple, ItemIdData)>,
}

#[derive(Clone, Copy, Debug)]
pub enum NodeType {
    Node,
    Leaf,
}

#[derive(Clone, Copy, Debug)]
pub struct BTreePage(pub usize);

impl BTreeBranch {
    pub fn serialize(&self) -> Result<Bytes, BTreeError> {
        let mut buffer = BytesMut::new();
        buffer.put_u8(1);

        match self.parent_node {
            Some(pn) => {
                let pn_u64 = u64::try_from(pn.0)?;
                buffer.put_uint_le(pn_u64, size_of::<usize>())
            }
            None => buffer.put_uint_le(0, size_of::<usize>()),
        }
        match self.left_node {
            Some(ln) => {
                let ln_u64 = u64::try_from(ln.0)?;
                buffer.put_uint_le(ln_u64, size_of::<usize>())
            }
            None => buffer.put_uint_le(0, size_of::<usize>()),
        }
        match self.right_node {
            Some(rn) => {
                let rn_u64 = u64::try_from(rn.0)?;
                buffer.put_uint_le(rn_u64, size_of::<usize>())
            }
            None => buffer.put_uint_le(0, size_of::<usize>()),
        }

        BTreeBranch::write_count(&mut buffer, self.nodes.len());

        for (key, pointer) in self.nodes.iter() {
            let nulls = NullMask::serialize(&key);
            buffer.put(nulls);

            for data in key.0.iter() {
                if data.is_none() {
                    continue;
                }

                let data_bytes = data.as_ref().unwrap().serialize();
                buffer.extend_from_slice(&data_bytes);
            }

            let pointer_u64 = u64::try_from(pointer.0)?;
            buffer.put_uint_le(pointer_u64, size_of::<usize>());
        }

        Ok(buffer.freeze())
    }

    fn write_count(buffer: &mut BytesMut, in_count: usize) {
        let mut count = in_count;
        while count > 0 {
            let last_count = count as u8;
            let mut digit: u8 = last_count & 0x7f;
            count >>= 7;
            if count > 0 {
                digit |= 0x80;
            }
            buffer.put_u8(digit);
        }
    }

    pub fn parse(index_def: &Index, buffer: &mut impl Buf) -> Result<BTreeNode, BTreeError> {
        if buffer.remaining() < size_of::<u8>() {
            return Err(BTreeError::MissingNodeTypeData(
                size_of::<u8>(),
                buffer.remaining(),
            ));
        }
        let node_type = buffer.get_u8();

        if buffer.remaining() < size_of::<usize>() {
            return Err(BTreeError::MissingParentData(
                size_of::<usize>(),
                buffer.remaining(),
            ));
        }
        let parent = buffer.get_uint_le(size_of::<usize>());
        let mut parent_node = None;
        if parent != 0 {
            parent_node = Some(BTreePage(usize::try_from(parent)?));
        }

        if buffer.remaining() < size_of::<usize>() {
            return Err(BTreeError::MissingLeftData(
                size_of::<usize>(),
                buffer.remaining(),
            ));
        }
        let left = buffer.get_uint_le(size_of::<usize>());
        let mut left_node = None;
        if left != 0 {
            left_node = Some(BTreePage(usize::try_from(left)?));
        }

        if buffer.remaining() < size_of::<usize>() {
            return Err(BTreeError::MissingRightData(
                size_of::<usize>(),
                buffer.remaining(),
            ));
        }
        let right = buffer.get_uint_le(size_of::<usize>());
        let mut right_node = None;
        if right != 0 {
            right_node = Some(BTreePage(usize::try_from(right)?));
        }

        return Err(BTreeError::Unknown());
        /* if node_type == 0 {
            return Ok(BTreeNode::Leaf(BTreeLeaf{
                parent_node,
                left_node,
                right_node,
                nodes: Vec<(SqlTuple, ItemIdData)>,
            }));
        } else {
            let bucket_count = BTreeBranch::parse_count(buffer)?;
            let buckets = Vec::with_capacity(bucket_count);

            for b in 0..bucket_count {
                let nulls = NullMask::parse(buffer, index_def.columns.len())?;
                let bucket =
                for c in index_def.columns {

                }
            }


            return Ok(BTreeNode::Branch(BTreeBranch{
                parent_node,
                left_node,
                right_node,
                nodes: Vec<(SqlTuple, BTreePage)>,
            }));
        }*/
    }

    fn parse_count(buffer: &mut impl Buf) -> Result<usize, BTreeError> {
        let mut length: usize = 0;
        let mut high_bit = 1;
        let mut loop_count = 0;
        while high_bit == 1 {
            if !buffer.has_remaining() {
                return Err(BTreeError::BufferTooShort());
            }

            let b = buffer.get_u8();
            high_bit = b >> 7;

            let mut low_bits: usize = (b & 0x7f).into();
            low_bits <<= 7 * loop_count;
            loop_count += 1;

            length += low_bits;
        }

        Ok(length)
    }
}

#[derive(Debug, Error)]
pub enum BTreeError {
    #[error("Buffer too short to parse")]
    BufferTooShort(),
    #[error("Missing Data for Left need {0}, have {1}")]
    MissingLeftData(usize, usize),
    #[error("Missing Data for Node Type need {0}, have {1}")]
    MissingNodeTypeData(usize, usize),
    #[error("Missing Data for Parent need {0}, have {1}")]
    MissingParentData(usize, usize),
    #[error("Missing Data for Right need {0}, have {1}")]
    MissingRightData(usize, usize),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error("Not implemented")]
    Unknown(),
}
