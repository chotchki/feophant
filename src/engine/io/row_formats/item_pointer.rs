//! Item Pointers tell a row where the latest version of itself might be stored.
//! Details here: https://www.postgresql.org/docs/current/storage-page-layout.html look at t_ctid
//!
//! We will be treating this a little different since our size will be based on usize

use crate::engine::io::format_traits::{Parseable, Serializable};
use crate::engine::io::page_formats::{PageOffset, PageOffsetError};
use crate::engine::io::ConstEncodedSize;

use super::super::page_formats::{UInt12, UInt12Error};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::convert::TryFrom;
use std::fmt;
use std::mem::size_of;
use std::num::TryFromIntError;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ItemPointer {
    pub page: PageOffset,
    pub count: UInt12,
}

impl ItemPointer {
    pub fn new(page: PageOffset, count: UInt12) -> ItemPointer {
        ItemPointer { page, count }
    }
}

impl ConstEncodedSize for ItemPointer {
    fn encoded_size() -> usize {
        size_of::<usize>() + UInt12::encoded_size()
    }
}

impl fmt::Display for ItemPointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\tItemPointer")?;
        writeln!(f, "\tPage: {}", self.page)?;
        writeln!(f, "\tCount: {}", self.count)
    }
}

impl Parseable<ItemPointerError> for ItemPointer {
    type Output = Self;
    fn parse(buffer: &mut impl Buf) -> Result<Self, ItemPointerError> {
        let po = PageOffset::parse(buffer)?;
        let items = UInt12::parse_packed(buffer, 1)?;
        Ok(ItemPointer::new(po, items[0]))
    }
}

impl Serializable for ItemPointer {
    fn serialize(&self, buffer: &mut impl BufMut) {
        self.page.serialize(buffer);
        UInt12::serialize_packed(buffer, &[self.count]);
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ItemPointerError {
    #[error(transparent)]
    PageOffsetError(#[from] PageOffsetError),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error(transparent)]
    UInt12Error(#[from] UInt12Error),
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    #[test]
    fn sizes_match() -> Result<(), Box<dyn std::error::Error>> {
        let test = ItemPointer::new(PageOffset(1), UInt12::new(2)?);
        let calc_len = ItemPointer::encoded_size();

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);

        assert_eq!(calc_len, buffer.freeze().len());
        Ok(())
    }

    #[test]
    fn test_item_pointer_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let test = ItemPointer::new(PageOffset(1), UInt12::new(1).unwrap());

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);
        let test_reparse = ItemPointer::parse(&mut buffer.freeze())?;

        assert_eq!(test, test_reparse);
        Ok(())
    }

    #[test]
    fn test_item_pointer_error_conditions() -> Result<(), Box<dyn std::error::Error>> {
        let parse = ItemPointer::parse(&mut Bytes::new());

        assert_eq!(
            Err(ItemPointerError::PageOffsetError(
                PageOffsetError::BufferTooShort(size_of::<usize>(), 0)
            )),
            parse
        );

        let parse = ItemPointer::parse(&mut Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 0, 1]));

        assert_eq!(
            Err(ItemPointerError::UInt12Error(UInt12Error::InsufficentData(
                0
            ))),
            parse
        );
        Ok(())
    }

    #[test]
    fn test_encoded_size() {
        match size_of::<usize>() {
            4 => assert_eq!(6, ItemPointer::encoded_size()),
            8 => assert_eq!(10, ItemPointer::encoded_size()),
            _ => panic!("You're on your own on this arch."),
        }
    }
}
