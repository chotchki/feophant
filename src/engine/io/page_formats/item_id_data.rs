//! Pointer type to indicate where an item is inside a page
//! See here for doc: https://www.postgresql.org/docs/current/storage-page-layout.html
use crate::engine::io::{
    format_traits::{Parseable, Serializable},
    ConstEncodedSize,
};

use super::{UInt12, UInt12Error};
use bytes::{Buf, BufMut};
use std::ops::Range;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ItemIdData {
    offset: UInt12,
    pub length: UInt12,
}

impl ItemIdData {
    pub fn new(offset: UInt12, length: UInt12) -> ItemIdData {
        ItemIdData { offset, length }
    }

    pub fn get_range(&self) -> Range<usize> {
        let offset_usize = self.offset.to_u16() as usize;
        let length_usize = self.length.to_u16() as usize;
        offset_usize..(offset_usize + length_usize)
    }
}

impl ConstEncodedSize for ItemIdData {
    fn encoded_size() -> usize {
        3
    }
}

impl Parseable<ItemIdDataError> for ItemIdData {
    type Output = Self;
    fn parse(buffer: &mut impl Buf) -> Result<Self, ItemIdDataError> {
        let items = UInt12::parse_packed(buffer, 2)?;
        Ok(ItemIdData::new(items[0], items[1]))
    }
}

impl Serializable for ItemIdData {
    fn serialize(&self, buffer: &mut impl BufMut) {
        UInt12::serialize_packed(buffer, &[self.offset, self.length]);
    }
}

#[derive(Debug, Error)]
pub enum ItemIdDataError {
    #[error("Not enough data has {0} bytes")]
    InsufficentData(usize),
    #[error(transparent)]
    UInt12Error(#[from] UInt12Error),
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::constants::PAGE_SIZE;

    use super::*;

    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let iid = ItemIdData::new(UInt12::new(1)?, UInt12::new(2)?);

        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        iid.serialize(&mut buffer);

        let mut buffer = buffer.freeze();
        let result = ItemIdData::parse(&mut buffer)?;

        assert_eq!(iid, result);

        Ok(())
    }
}
