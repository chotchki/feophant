//! See https://www.postgresql.org/docs/current/storage-page-layout.html for reference documentation
//! I'm only implementing enough for my needs until proven otherwise
use super::{ItemIdData, UInt12, UInt12Error};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::convert::TryFrom;
use std::mem::size_of;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct PageHeader {
    pd_lower: UInt12,
    pd_upper: UInt12,
}

impl PageHeader {
    pub fn new() -> PageHeader {
        PageHeader {
            pd_lower: UInt12::new((size_of::<PageHeader>()) as u16).unwrap(),
            pd_upper: UInt12::max(),
        }
    }

    pub fn get_item_count(&self) -> usize {
        let lower: usize = self.pd_lower.to_u16().into();
        (lower - size_of::<PageHeader>()) / size_of::<ItemIdData>()
    }

    pub fn get_free_space(&self) -> usize {
        //Handle no free space
        if self.pd_upper < self.pd_lower {
            return 0;
        }
        (self.pd_upper - self.pd_lower).to_u16() as usize + 1
    }

    pub fn can_fit(&self, row_size: usize) -> bool {
        let needed = row_size + size_of::<ItemIdData>();
        let have = self.get_free_space();
        have >= needed
    }

    pub fn add_item(&mut self, row_size: usize) -> Result<ItemIdData, PageHeaderError> {
        if !self.can_fit(row_size) {
            return Err(PageHeaderError::InsufficentFreeSpace());
        }

        let row_u12 = UInt12::try_from(row_size)?;

        self.pd_lower += UInt12::try_from(size_of::<ItemIdData>())?;
        self.pd_upper -= row_u12;

        //Need to increment the offset by 1 since the pointer is now pointing a free space
        let item_offset = self.pd_upper + UInt12::new(1).unwrap();

        Ok(ItemIdData::new(item_offset, row_u12))
    }

    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(size_of::<PageHeader>());
        buf.put_u16_le(self.pd_lower.to_u16());
        buf.put_u16_le(self.pd_upper.to_u16());
        buf.freeze()
    }

    pub fn parse(buffer: &mut impl Buf) -> Result<Self, PageHeaderError> {
        if buffer.remaining() < size_of::<PageHeader>() {
            return Err(PageHeaderError::InsufficentData(buffer.remaining()));
        }
        let pd_lower =
            UInt12::new(buffer.get_u16_le()).ok_or_else(PageHeaderError::LowerOffsetTooLarge)?;
        let pd_upper =
            UInt12::new(buffer.get_u16_le()).ok_or_else(PageHeaderError::UpperOffsetTooLarge)?;
        Ok(PageHeader { pd_lower, pd_upper })
    }
}

#[derive(Debug, Error)]
pub enum PageHeaderError {
    #[error("Not enough space to add")]
    InsufficentFreeSpace(),
    #[error("Value for u12 too large")]
    TooLarge(#[from] UInt12Error),
    #[error("Not enough data has {0} bytes")]
    InsufficentData(usize),
    #[error("Lower offset is too large")]
    LowerOffsetTooLarge(),
    #[error("Upper offset is too large")]
    UpperOffsetTooLarge(),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let test = PageHeader::new();
        let mut test_serial = test.serialize();
        let test_rt = PageHeader::parse(&mut test_serial).unwrap();

        let test_new = PageHeader::new();
        assert_eq!(test_rt, test_new);
    }

    #[test]
    fn test_initial_freespace() {
        let test = PageHeader::new();

        let default_free_space: usize =
            (UInt12::max().to_u16() as usize) + 1 - size_of::<PageHeader>();
        let found_free_space = test.get_free_space();
        assert_eq!(found_free_space, default_free_space);
    }

    #[test]
    fn test_item_count() {
        let mut test = PageHeader::new();

        test.add_item(5).unwrap();
        test.add_item(5).unwrap();

        assert_eq!(test.get_item_count(), 2);

        let remain_free = (UInt12::max().to_u16() as usize) + 1 //Initial
            - size_of::<PageHeader>() //Header
            - (size_of::<ItemIdData>() * 2) //Two items
            - 10; //Their data
        assert_eq!(test.get_free_space(), remain_free)
    }

    #[test]
    fn test_too_big() {
        let mut test = PageHeader::new();

        let needed = (UInt12::max().to_u16() as usize) + 1
            - size_of::<PageHeader>()
            - size_of::<ItemIdData>();
        test.add_item(needed).unwrap(); //Should be maxed out

        assert_eq!(test.get_item_count(), 1); //Should have an item
        assert_eq!(test.get_free_space(), 0); //Should be full
        assert!(!test.can_fit(1)); //Should not be able to store a tiny item
        assert!(test.add_item(0).is_err()); //Adding more should fail
    }
}
