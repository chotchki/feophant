//! Pointer type to indicate where an item is inside a page
//! See here for doc: https://www.postgresql.org/docs/current/storage-page-layout.html
use super::UInt12;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem::size_of;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct ItemIdData {
    offset: UInt12,
    length: UInt12,
}

impl ItemIdData {
    pub fn new(offset: UInt12, length: UInt12) -> ItemIdData {
        ItemIdData { offset, length }
    }

    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(size_of::<ItemIdData>());
        buf.put_u16_le(self.offset.to_u16());
        buf.put_u16_le(self.length.to_u16());
        buf.freeze()
    }

    fn parse(mut input: Bytes) -> Result<Self, ItemIdDataError> {
        if input.len() < 4 {
            return Err(ItemIdDataError::InsufficentData(input.len()));
        }
        let offset = UInt12::new(input.get_u16_le()).ok_or_else(ItemIdDataError::OffsetTooLarge)?;
        let length = UInt12::new(input.get_u16_le()).ok_or_else(ItemIdDataError::LengthTooLarge)?;
        Ok(ItemIdData { offset, length })
    }
}

#[derive(Debug, Error)]
pub enum ItemIdDataError {
    #[error("Not enough data has {0} bytes")]
    InsufficentData(usize),
    #[error("Offset is too large")]
    OffsetTooLarge(),
    #[error("Length is too large")]
    LengthTooLarge(),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let test = ItemIdData::new(UInt12::new(1).unwrap(), UInt12::new(2).unwrap());
        let test_serial = test.serialize();
        let test_rt = ItemIdData::parse(test_serial).unwrap();

        let test_new = ItemIdData::new(UInt12::new(1).unwrap(), UInt12::new(2).unwrap());
        assert_eq!(test_rt, test_new);
    }
}
