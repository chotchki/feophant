//! See https://www.postgresql.org/docs/current/storage-page-layout.html for reference documentation
//! I'm only implementing enough for my needs until proven otherwise
use super::PageOffset;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem::size_of;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct PageHeader {
    pd_lower: PageOffset,
    pd_upper: PageOffset,
}

impl PageHeader {
    pub fn new() -> PageHeader {
        PageHeader {
            pd_lower: PageOffset::new((size_of::<PageHeader>() - 1) as u16).unwrap(),
            pd_upper: PageOffset::max(),
        }
    }

    fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(size_of::<PageHeader>());
        buf.put_u16_le(self.pd_lower.to_u16());
        buf.put_u16_le(self.pd_upper.to_u16());
        buf.freeze()
    }

    fn parse(mut input: Bytes) -> Result<Self, PageHeaderError> {
        if input.len() < 4 {
            return Err(PageHeaderError::InsufficentData(input.len()));
        }
        let pd_lower =
            PageOffset::new(input.get_u16_le()).ok_or_else(PageHeaderError::LowerOffsetTooLarge)?;
        let pd_upper =
            PageOffset::new(input.get_u16_le()).ok_or_else(PageHeaderError::UpperOffsetTooLarge)?;
        Ok(PageHeader { pd_lower, pd_upper })
    }
}

#[derive(Debug, Error)]
pub enum PageHeaderError {
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
