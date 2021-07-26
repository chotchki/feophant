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

    pub const fn serialize_size() -> usize {
        3
    }

    pub fn get_range(&self) -> Range<usize> {
        let offset_usize = self.offset.to_u16() as usize;
        let length_usize = self.length.to_u16() as usize;
        offset_usize..(offset_usize + length_usize)
    }

    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::serialize_size());

        //The goal is to pack 2x 6bit numbers into 3 bytes
        let offset_u16 = self.offset.to_u16();
        let length_u16 = self.length.to_u16();

        let offset_u16_f = (offset_u16 & 0x00FF) as u8;
        let offset_u16_s = ((offset_u16 & 0xFF00) >> 4) as u8;

        let length_u16_f = ((length_u16 & 0xFF00) >> 8) as u8;
        let length_u16_s = (length_u16 & 0x00FF) as u8;

        let combined = offset_u16_s | length_u16_f;

        buf.put_u8(offset_u16_f);
        buf.put_u8(combined);
        buf.put_u8(length_u16_s);
        buf.freeze()
    }

    pub fn parse(buffer: &mut impl Buf) -> Result<Self, ItemIdDataError> {
        if buffer.remaining() < Self::serialize_size() {
            return Err(ItemIdDataError::InsufficentData(buffer.remaining()));
        }

        let offset_u16_f = buffer.get_u8() as u16;
        let combined = buffer.get_u8() as u16;
        let length_u16_s = buffer.get_u8() as u16;

        let combined_l = (combined & 0x00F0) << 4;
        let combined_r = (combined & 0x000F) << 8;

        let offset_m = offset_u16_f | combined_l;
        let length_m = length_u16_s | combined_r;

        let offset = UInt12::new(offset_m)?;
        let length = UInt12::new(length_m)?;
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
    fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        //Test numbers were picked to give a distingishable binary pattern for troubleshooting
        let test = ItemIdData::new(UInt12::new(2730)?, UInt12::new(1365)?);
        let mut test_serial = test.serialize();
        assert_eq!(test_serial.len(), 3);
        let test_rt = ItemIdData::parse(&mut test_serial)?;

        assert_eq!(test_rt, test);

        Ok(())
    }
}
