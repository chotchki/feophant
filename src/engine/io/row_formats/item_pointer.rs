//! Item Pointers tell a row where the latest version of itself might be stored.
//! Details here: https://www.postgresql.org/docs/current/storage-page-layout.html look at t_ctid
//!
//! We will be treating this a little different since our size will be based on usize

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
    pub page: usize,
    pub count: UInt12,
}

impl ItemPointer {
    pub fn new(page: usize, count: UInt12) -> ItemPointer {
        ItemPointer { page, count }
    }

    pub fn serialize(&self) -> Bytes {
        let mut buffer = BytesMut::with_capacity(size_of::<ItemPointer>());

        buffer.put_slice(&self.page.to_le_bytes());
        UInt12::serialize_packed(&mut buffer, &vec![self.count]);

        buffer.freeze()
    }

    pub fn parse(buffer: &mut impl Buf) -> Result<Self, ItemPointerError> {
        if buffer.remaining() < size_of::<usize>() {
            return Err(ItemPointerError::BufferTooShort(
                size_of::<usize>(),
                buffer.remaining(),
            ));
        }

        let value = buffer.get_uint_le(size_of::<usize>());
        let page = usize::try_from(value)?;

        let items = UInt12::parse_packed(buffer, 1)?;
        Ok(ItemPointer::new(page, items[0]))
    }
}

impl fmt::Display for ItemPointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\tItemPointer\n")?;
        write!(f, "\tPage: {}\n", self.page)?;
        write!(f, "\tCount: {}\n", self.count)
    }
}

impl ConstEncodedSize for ItemPointer {
    fn encoded_size() -> usize {
        size_of::<usize>() + UInt12::encoded_size()
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ItemPointerError {
    #[error("Not enough space to parse usize need {0} got {1}")]
    BufferTooShort(usize, usize),
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
    fn test_item_pointer_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let test = ItemPointer::new(1, UInt12::new(1).unwrap());

        let mut test_serial = test.clone().serialize();
        let test_reparse = ItemPointer::parse(&mut test_serial)?;

        //Smoke test display
        println!("{}", test_reparse);

        assert_eq!(test, test_reparse);
        Ok(())
    }

    #[test]
    fn test_item_pointer_error_conditions() -> Result<(), Box<dyn std::error::Error>> {
        let parse = ItemPointer::parse(&mut Bytes::new());

        assert_eq!(
            Err(ItemPointerError::BufferTooShort(size_of::<usize>(), 0)),
            parse
        );

        let parse = ItemPointer::parse(&mut Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 0, 1]));

        assert_eq!(
            Err(ItemPointerError::UInt12Error(UInt12Error::InsufficentData(
                1
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
