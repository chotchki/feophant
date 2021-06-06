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
            pd_lower: UInt12::new((size_of::<PageHeader>() - 1) as u16).unwrap(),
            pd_upper: UInt12::max(),
        }
    }

    pub fn get_free_space(&self) -> usize {
        (self.pd_upper - self.pd_lower).to_u16() as usize
    }

    pub fn can_store(&self, row_size: usize) -> bool {
        self.get_free_space() > (row_size + size_of::<Self>())
    }

    pub fn add_item(&mut self, row_size: usize) -> Result<ItemIdData, PageHeaderError> {
        if !self.can_store(row_size) {
            return Err(PageHeaderError::InsufficentFreeSpace());
        }

        let row_u12 = UInt12::try_from(row_size).map_err(PageHeaderError::TooLarge)?;

        self.pd_lower +=
            UInt12::try_from(size_of::<ItemIdData>()).map_err(PageHeaderError::TooLarge)?;
        self.pd_upper -= row_u12;

        Ok(ItemIdData::new(self.pd_upper, row_u12))
    }

    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(size_of::<PageHeader>());
        buf.put_u16_le(self.pd_lower.to_u16());
        buf.put_u16_le(self.pd_upper.to_u16());
        buf.freeze()
    }

    pub fn parse(mut input: Bytes) -> Result<Self, PageHeaderError> {
        if input.len() < 4 {
            return Err(PageHeaderError::InsufficentData(input.len()));
        }
        let pd_lower =
            UInt12::new(input.get_u16_le()).ok_or_else(PageHeaderError::LowerOffsetTooLarge)?;
        let pd_upper =
            UInt12::new(input.get_u16_le()).ok_or_else(PageHeaderError::UpperOffsetTooLarge)?;
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
        let test_serial = test.serialize();
        let test_rt = PageHeader::parse(test_serial).unwrap();

        let test_new = PageHeader::new();
        assert_eq!(test_rt, test_new);
    }
}
