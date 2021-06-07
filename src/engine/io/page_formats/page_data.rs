use super::super::super::objects::Table;
use super::super::row_formats::RowData;
use super::{ItemIdData, PageHeader, PageHeaderError, UInt12};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem::size_of;
use std::sync::Arc;
use thiserror::Error;

pub struct PageData {
    table: Arc<Table>,
    page_header: PageHeader,
    rows: Vec<(ItemIdData, RowData)>,
}

impl PageData {
    pub fn new(table: Arc<Table>) -> PageData {
        PageData {
            table,
            page_header: PageHeader::new(),
            rows: vec![],
        }
    }

    //fast check if there is still space in this page
    pub fn can_store(&self, row_data_size: usize) -> bool {
        self.page_header.can_store(row_data_size)
    }

    pub fn store(&mut self, row_data: RowData) -> Result<(), PageDataError> {
        let row_data_len = row_data.serialize().len();

        let item_data = self
            .page_header
            .add_item(row_data_len)
            .map_err(PageDataError::PageHeaderParseError)?;

        self.rows.push((item_data, row_data));
        Ok(())
    }

    pub fn serialize(&self, table: &Table) -> Bytes {
        let mut buffer = BytesMut::with_capacity((UInt12::max().to_u16() + 1).into());
        buffer.put(self.page_header.serialize());

        //Now write items data in order
        for (item, _) in self.rows.iter() {
            buffer.put(item.serialize())
        }

        //Fill the free space
        let free_space = vec![0; self.page_header.get_free_space()];
        buffer.extend_from_slice(&free_space);

        //Write items in reverse order
        for (_, value) in self.rows.iter().rev() {
            buffer.put(value.serialize());
        }

        buffer.freeze()
    }

    pub fn parse(table: &Table, mut page_buffer: impl Buf) -> Result<PageData, PageDataError> {
        let page_header =
            PageHeader::parse(page_buffer).map_err(PageDataError::PageHeaderParseError)?;

        //let item_count = page_header.

        Err(PageDataError::Unknown())
    }

    //Todo implement updates, just unsure if it should be here
}

#[derive(Debug, Error)]
pub enum PageDataError {
    #[error("Page Header Parse Error")]
    PageHeaderParseError(#[from] PageHeaderError),

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
    use super::super::super::super::super::constants::{
        BuiltinSqlTypes, DeserializeTypes, SqlTypeError,
    };
    use super::super::super::super::objects::{Attribute, Table, TransactionId};
    use super::*;

    #[test]
    fn test_page_data_roundtrip() {
        let table = Arc::new(Table::new(
            "test_table".to_string(),
            vec![
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header".to_string(),
                    DeserializeTypes::Text,
                ),
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "id".to_string(),
                    DeserializeTypes::Uuid,
                ),
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header3".to_string(),
                    DeserializeTypes::Text,
                ),
            ],
        ));

        let test = RowData::new(table.clone(),
            TransactionId::new(1),
            None,
            vec![
                Some(BuiltinSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BuiltinSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ],
        );

        let mut pg = PageData::new(table);
        assert!(pg.store(test.clone()).is_ok());
    }
}
