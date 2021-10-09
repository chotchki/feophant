//! See https://www.postgresql.org/docs/current/storage-page-layout.html for reference documentation
//! I'm only implementing enough for my needs until proven otherwise
use crate::engine::io::{format_traits::Parseable, ConstEncodedSize};

use super::{ItemIdData, UInt12, UInt12Error};
use bytes::{Buf, BufMut};
use std::convert::TryFrom;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct PageHeader {
    pd_lower: UInt12,
    pd_upper: UInt12,
}

impl PageHeader {
    pub fn new() -> PageHeader {
        PageHeader {
            pd_lower: UInt12::new((PageHeader::encoded_size()) as u16).unwrap(),
            pd_upper: UInt12::max(),
        }
    }

    pub fn get_item_count(&self) -> usize {
        let lower: usize = self.pd_lower.to_u16().into();
        (lower - PageHeader::encoded_size()) / ItemIdData::encoded_size()
    }

    pub fn get_free_space(&self) -> usize {
        //Handle no free space
        if self.pd_upper < self.pd_lower {
            return 0;
        }
        (self.pd_upper - self.pd_lower).to_u16() as usize + 1
    }

    pub fn can_fit(&self, row_size: usize) -> bool {
        let needed = row_size + ItemIdData::encoded_size();
        let have = self.get_free_space();
        have >= needed
    }

    pub fn add_item(&mut self, row_size: usize) -> Result<ItemIdData, PageHeaderError> {
        if !self.can_fit(row_size) {
            return Err(PageHeaderError::InsufficentFreeSpace());
        }

        let row_u12 = UInt12::try_from(row_size)?;

        self.pd_lower += UInt12::try_from(ItemIdData::encoded_size())?;
        self.pd_upper -= row_u12;

        //Need to increment the offset by 1 since the pointer is now pointing a free space
        let item_offset = self.pd_upper + UInt12::new(1).unwrap();

        Ok(ItemIdData::new(item_offset, row_u12))
    }

    pub fn serialize(&self, buffer: &mut impl BufMut) {
        UInt12::serialize_packed(buffer, &[self.pd_lower, self.pd_upper]);
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstEncodedSize for PageHeader {
    fn encoded_size() -> usize {
        3
    }
}

impl Parseable<PageHeaderError> for PageHeader {
    type Output = Self;
    fn parse(buffer: &mut impl Buf) -> Result<Self, PageHeaderError> {
        let items = UInt12::parse_packed(buffer, 2)?;
        Ok(PageHeader {
            pd_lower: items[0],
            pd_upper: items[1],
        })
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
    use bytes::BytesMut;

    use crate::constants::PAGE_SIZE;

    use super::*;

    #[test]
    fn sizes_match() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = PageHeader::new();
        let calc_len = PageHeader::encoded_size();

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);

        assert_eq!(calc_len, buffer.freeze().len());
        Ok(())
    }

    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let test = PageHeader::new();
        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);
        let test_rt = PageHeader::parse(&mut buffer)?;

        let test_new = PageHeader::new();
        assert_eq!(test_rt, test_new);

        Ok(())
    }

    #[test]
    fn test_initial_freespace() {
        let test = PageHeader::new();

        let default_free_space: usize = PAGE_SIZE as usize - PageHeader::encoded_size();
        let found_free_space = test.get_free_space();
        assert_eq!(found_free_space, default_free_space);
    }

    #[test]
    fn test_item_count() {
        let mut test = PageHeader::new();

        test.add_item(5).unwrap();
        test.add_item(5).unwrap();

        assert_eq!(test.get_item_count(), 2);

        let remain_free = PAGE_SIZE as usize //Initial
            - PageHeader::encoded_size() //Header
            - (ItemIdData::encoded_size() * 2) //Two items
            - 10; //Their data
        assert_eq!(test.get_free_space(), remain_free)
    }

    #[test]
    fn test_too_big() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = PageHeader::new();

        let needed = PAGE_SIZE as usize - PageHeader::encoded_size() - ItemIdData::encoded_size();
        test.add_item(needed)?; //Should be maxed out

        assert_eq!(test.get_item_count(), 1); //Should have an item
        assert_eq!(test.get_free_space(), 0); //Should be full
        assert!(!test.can_fit(1)); //Should not be able to store a tiny item
        assert!(test.add_item(0).is_err()); //Adding more should fail

        Ok(())
    }
}
