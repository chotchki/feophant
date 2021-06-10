//! Item Pointers tell a row where the latest version of itself might be stored.
//! Details here: https://www.postgresql.org/docs/current/storage-page-layout.html look at t_ctid
//!
//! We will be treating this a little different since our size will be based on usize
use super::super::page_formats::{UInt12, UInt12Error};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem;
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
        let mut buffer = BytesMut::with_capacity(mem::size_of::<ItemPointer>());

        buffer.put_slice(&self.page.to_le_bytes());
        buffer.put(self.count.serialize());

        buffer.freeze()
    }

    pub fn parse(buffer: &mut impl Buf) -> Result<Self, ItemPointerError> {
        if buffer.remaining() < mem::size_of::<usize>() {
            return Err(ItemPointerError::BufferTooShort(
                mem::size_of::<usize>(),
                buffer.remaining(),
            ));
        }

        let mut raw_page = [0; mem::size_of::<usize>()];
        buffer.copy_to_slice(&mut raw_page);
        let page = usize::from_le_bytes(raw_page);

        let count = UInt12::parse(buffer)?;
        Ok(ItemPointer::new(page, count))
    }
}

#[derive(Debug, Error)]
pub enum ItemPointerError {
    #[error("Not enough space to parse usize need {0} got {1}")]
    BufferTooShort(usize, usize),
    #[error("U12ParseError")]
    U12ParseError(#[from] UInt12Error),
}
