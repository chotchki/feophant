//! Pointer type to indicate where an item is inside a page
//! See here for doc: https://www.postgresql.org/docs/current/storage-page-layout.html
use super::{UInt12, UInt12Error};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem::size_of;
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

    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(size_of::<ItemIdData>());
        buf.put_u16_le(self.offset.to_u16());
        buf.put_u16_le(self.length.to_u16());
        buf.freeze()
    }

    pub fn parse(buffer: &mut impl Buf) -> Result<Self, ItemIdDataError> {
        if buffer.remaining() < size_of::<UInt12>() * 2 {
            return Err(ItemIdDataError::InsufficentData(buffer.remaining()));
        }
        let offset = UInt12::new(buffer.get_u16_le())?;
        let length = UInt12::new(buffer.get_u16_le())?;
        Ok(ItemIdData { offset, length })
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
    use super::*;

    #[test]
    fn test_roundtrip() {
        let test = ItemIdData::new(UInt12::new(1).unwrap(), UInt12::new(2).unwrap());
        let mut test_serial = test.serialize();
        let test_rt = ItemIdData::parse(&mut test_serial).unwrap();

        let test_new = ItemIdData::new(UInt12::new(1).unwrap(), UInt12::new(2).unwrap());
        assert_eq!(test_rt, test_new);
    }
}
