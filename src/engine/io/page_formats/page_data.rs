use super::super::super::objects::Table;
use super::super::row_formats::RowData;
use super::{ItemIdData, PageHeader, PageHeaderError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem::size_of;
use thiserror::Error;

pub struct PageData {
    page_header: PageHeader,
    rows: Vec<(ItemIdData, Bytes)>,
}

impl PageData {
    pub fn new() -> PageData {
        PageData {
            page_header: PageHeader::new(),
            rows: vec![],
        }
    }

    //fast check if there is still space in this page
    pub fn can_store(&self, row_data_size: usize) -> bool {
        self.page_header.can_store(row_data_size)
    }

    pub fn store(&mut self, row_data: Bytes) -> Result<(), PageDataError> {
        let item_data = self
            .page_header
            .add_item(row_data.len())
            .map_err(PageDataError::InsufficentFreeSpace)?;

        self.rows.push((item_data, row_data));
        Ok(())
    }

    pub fn serialize(&self) -> Bytes {
        let mut buffer = BytesMut::new();
        buffer.put(self.page_header.serialize());

        buffer.freeze()
    }

    pub fn parse(table: &Table, mut page_buffer: impl Buf) -> Result<PageData, PageDataError> {
        Err(PageDataError::Unknown())
    }

    //Todo implement updates, just unsure if it should be here
}

#[derive(Debug, Error)]
pub enum PageDataError {
    #[error("Not Enough Free Space")]
    InsufficentFreeSpace(#[from] PageHeaderError),

    #[error("Not enough max data need {0} got {1}")]
    MissingMaxData(usize, usize),
    #[error("Not enough infomask data need {0} got {1}")]
    MissingInfoMaskData(usize, usize),
    #[error("Not enough null mask data need {0} got {1}")]
    MissingNullMaskData(usize, usize),
    #[error("Not Implemented")]
    Unknown(),
}

#[cfg(test)]
mod tests {
    use super::*;
}
