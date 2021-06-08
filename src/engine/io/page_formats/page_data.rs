use super::super::super::objects::Table;
use super::super::row_formats::{RowData, RowDataError};
use super::{ItemIdData, ItemIdDataError, PageHeader, PageHeaderError, UInt12};
use bytes::{BufMut, Bytes, BytesMut};
use std::mem;
use std::slice::Iter;
use std::sync::Arc;
use thiserror::Error;

pub struct PageData {
    table: Arc<Table>,
    page_header: PageHeader,
    item_ids: Vec<ItemIdData>,
    rows: Vec<RowData>,
}

impl PageData {
    pub fn new(table: Arc<Table>) -> PageData {
        PageData {
            table,
            page_header: PageHeader::new(),
            item_ids: vec![],
            rows: vec![],
        }
    }

    //fast check if there is still space in this page
    pub fn can_fit(&self, row_data_size: usize) -> bool {
        self.page_header.can_fit(row_data_size)
    }

    pub fn store(&mut self, row_data: RowData) -> Result<(), PageDataError> {
        let row_data_len = row_data.serialize().len();

        let item_data = self
            .page_header
            .add_item(row_data_len)
            .map_err(PageDataError::PageHeaderParseError)?;
        self.item_ids.push(item_data);
        self.rows.push(row_data);
        Ok(())
    }

    pub fn row_iter(&self) -> Iter<'_, RowData> {
        self.rows.iter()
    }

    pub fn serialize(&self) -> Bytes {
        let mut buffer = BytesMut::with_capacity((UInt12::max().to_u16() + 1).into());
        let mut cur_offset = 0;

        println!(
            "header {0} {1}",
            cur_offset,
            self.page_header.serialize().len()
        );
        buffer.put(self.page_header.serialize());
        cur_offset += self.page_header.serialize().len();

        //Now write items data in order
        for item in self.item_ids.iter() {
            println!("item {0} {1}", cur_offset, item.serialize().len());
            buffer.put(item.serialize());
            cur_offset += item.serialize().len();
        }

        //Fill the free space
        let free_space = vec![0; self.page_header.get_free_space()];
        println!("free space {0} {1}", cur_offset, free_space.len());
        buffer.extend_from_slice(&free_space);
        cur_offset += free_space.len();

        //Write items in reverse order
        for value in self.rows.iter().rev() {
            println!("row {0} {1}", cur_offset, value.serialize().len());
            buffer.put(value.serialize());
            cur_offset += value.serialize().len();
        }

        buffer.freeze()
    }

    pub fn parse(table: Arc<Table>, mut buffer: Bytes) -> Result<PageData, PageDataError> {
        //Note since we need random access, everything MUST work off slices otherwise counts will be off

        let mut page_header_slice = buffer.slice(0..mem::size_of::<PageHeader>());
        let page_header = PageHeader::parse(&mut page_header_slice)
            .map_err(PageDataError::PageHeaderParseError)?;

        let mut item_ids: Vec<ItemIdData> = Vec::with_capacity(page_header.get_item_count());
        let mut rows: Vec<RowData> = Vec::with_capacity(page_header.get_item_count());
        for i in 0..page_header.get_item_count() {
            let iid_lower_offset =
                mem::size_of::<PageHeader>() + (mem::size_of::<ItemIdData>() * i);
            let iid_upper_offset =
                mem::size_of::<PageHeader>() + (mem::size_of::<ItemIdData>() * (i + 1));
            let mut iid_slice = buffer.slice(iid_lower_offset..iid_upper_offset);
            let iid =
                ItemIdData::parse(&mut iid_slice).map_err(PageDataError::ItemIdDataParseError)?;

            let row_slice = buffer.slice(iid.get_range());
            let row = RowData::parse(table.clone(), row_slice)?;
            item_ids.push(iid);
            rows.push(row);
        }

        Ok(PageData {
            table,
            page_header,
            item_ids,
            rows,
        })
    }

    //Todo implement updates, just unsure if it should be here
}

#[derive(Debug, Error)]
pub enum PageDataError {
    #[error("Page Header Parse Error")]
    PageHeaderParseError(#[from] PageHeaderError),
    #[error("Item Id Data Parse Error")]
    ItemIdDataParseError(#[from] ItemIdDataError),
    #[error("Row Data Parse Error")]
    RowDataParseError(#[from] RowDataError),
}

#[cfg(test)]
mod tests {
    use super::super::super::super::super::constants::{BuiltinSqlTypes, DeserializeTypes};
    use super::super::super::super::objects::{Attribute, Table, TransactionId};
    use super::*;

    fn get_table() -> Arc<Table> {
        Arc::new(Table::new(
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
        ))
    }

    #[test]
    fn test_page_data_roundtrip() {
        let table = get_table();

        let rows = vec!(RowData::new(table.clone(),
            TransactionId::new(0xDEADBEEF),
            None,
            vec![
                Some(BuiltinSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BuiltinSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ],
        ).unwrap());

        let mut pg = PageData::new(table.clone());
        for r in rows.clone() {
            assert!(pg.store(r.clone()).is_ok());
        }
        let serial = pg.serialize();
        let pg_parsed = PageData::parse(table.clone(), serial).unwrap();

        //Get the data in the same format for comparison
        let test_rows: Vec<&RowData> = rows.iter().collect();
        let result_rows: Vec<&RowData> = pg_parsed.row_iter().collect();
        assert_eq!(test_rows, result_rows);
    }

    #[test]
    fn test_page_data_roundtrip_two_rows() {
        let table = get_table();

        let rows = vec!(RowData::new(table.clone(),
            TransactionId::new(0xDEADBEEF),
            None,
            vec![
                Some(BuiltinSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BuiltinSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ],
        ).unwrap(), RowData::new(table.clone(),
        TransactionId::new(0xDEADBEEF),
        None,
        vec![
            Some(BuiltinSqlTypes::Text("this also a test".to_string())),
            None,
            Some(BuiltinSqlTypes::Text("it would help if I didn't mix and match types".to_string())),
        ],
        ).unwrap());

        let mut pg = PageData::new(table.clone());
        for r in rows.clone() {
            assert!(pg.store(r.clone()).is_ok());
        }
        let serial = pg.serialize();
        let pg_parsed = PageData::parse(table.clone(), serial).unwrap();

        //Get the data in the same format for comparison
        let test_rows: Vec<&RowData> = rows.iter().collect();
        let result_rows: Vec<&RowData> = pg_parsed.row_iter().collect();
        assert_eq!(test_rows, result_rows);
    }
}
