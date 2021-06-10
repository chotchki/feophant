use super::super::super::objects::Table;
use super::super::row_formats::{ItemPointer, RowData, RowDataError};
use super::{ItemIdData, ItemIdDataError, PageHeader, PageHeaderError, UInt12, UInt12Error};
use async_stream::stream;
use bytes::{BufMut, Bytes, BytesMut};
use futures::pin_mut;
use futures::stream::Stream;
use futures::stream::StreamExt;
use std::convert::TryFrom;
use std::mem;
use std::slice::Iter;
use std::sync::Arc;
use thiserror::Error;

pub struct PageData {
    table: Arc<Table>,
    page: usize,
    page_header: PageHeader,
    item_ids: Vec<ItemIdData>,
    //TODO debating if I should defer parsing until later
    rows: Vec<RowData>,
}

impl PageData {
    pub fn new(table: Arc<Table>, page: usize) -> PageData {
        PageData {
            table,
            page,
            page_header: PageHeader::new(),
            item_ids: vec![],
            rows: vec![],
        }
    }

    //fast check if there is still space in this page
    pub fn can_fit(&self, row_data_size: usize) -> bool {
        self.page_header.can_fit(row_data_size)
    }

    //TODO debating if this should be row_data or bytes
    pub fn insert(&mut self, mut row_data: RowData) -> Result<(), PageDataError> {
        //Insert rewrites the row's location, update will not
        row_data.item_pointer = Some(ItemPointer::new(
            self.page,
            UInt12::try_from(self.rows.len())?,
        ));

        let row_data_len = row_data.serialize().len();

        let item_data = self.page_header.add_item(row_data_len)?;
        self.item_ids.push(item_data);
        self.rows.push(row_data);
        Ok(())
    }

    pub fn get_stream(&self) -> impl Stream<Item = RowData> {
        let rows_clone = self.rows.clone();
        stream! {
            for row in rows_clone.iter() {
                yield row.clone();
            }
        }
    }

    pub fn serialize(&self) -> Bytes {
        let mut buffer = BytesMut::with_capacity((UInt12::max().to_u16() + 1).into());

        buffer.put(self.page_header.serialize());

        //Now write items data in order
        for item in self.item_ids.iter() {
            buffer.put(item.serialize());
        }

        //Fill the free space
        let free_space = vec![0; self.page_header.get_free_space()];
        buffer.extend_from_slice(&free_space);

        //Write items in reverse order
        for value in self.rows.iter().rev() {
            buffer.put(value.serialize());
        }

        buffer.freeze()
    }

    pub fn parse(
        table: Arc<Table>,
        page: usize,
        mut buffer: Bytes,
    ) -> Result<PageData, PageDataError> {
        //Note since we need random access, everything MUST work off slices otherwise counts will be off

        let mut page_header_slice = buffer.slice(0..mem::size_of::<PageHeader>());
        let page_header = PageHeader::parse(&mut page_header_slice)?;

        let mut item_ids: Vec<ItemIdData> = Vec::with_capacity(page_header.get_item_count());
        let mut rows: Vec<RowData> = Vec::with_capacity(page_header.get_item_count());
        for i in 0..page_header.get_item_count() {
            let iid_lower_offset =
                mem::size_of::<PageHeader>() + (mem::size_of::<ItemIdData>() * i);
            let iid_upper_offset =
                mem::size_of::<PageHeader>() + (mem::size_of::<ItemIdData>() * (i + 1));
            let mut iid_slice = buffer.slice(iid_lower_offset..iid_upper_offset);
            let iid = ItemIdData::parse(&mut iid_slice)?;

            let row_slice = buffer.slice(iid.get_range());
            let row = RowData::parse(table.clone(), row_slice)?;
            item_ids.push(iid);
            rows.push(row);
        }

        Ok(PageData {
            table,
            page,
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
    #[error("UInt12 Conversion Error")]
    UInt12Error(#[from] UInt12Error),
}

#[cfg(test)]
mod tests {
    use super::super::super::super::super::constants::{BuiltinSqlTypes, DeserializeTypes};
    use super::super::super::super::objects::{Attribute, Table};
    use super::super::super::super::transactions::TransactionId;
    use super::*;

    //Async testing help can be found here: https://blog.x5ff.xyz/blog/async-tests-tokio-rust/
    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

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
            None,
            vec![
                Some(BuiltinSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BuiltinSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ],
        ).unwrap());

        let mut pg = PageData::new(table.clone(), 0);
        for r in rows.clone() {
            assert!(pg.insert(r.clone()).is_ok());
        }
        let serial = pg.serialize();
        let pg_parsed = PageData::parse(table.clone(), 0, serial).unwrap();

        pin_mut!(pg_parsed);
        let result_rows: Vec<RowData> = aw!(pg_parsed.get_stream().collect());
        assert_eq!(rows, result_rows);
    }

    #[test]
    fn test_page_data_roundtrip_two_rows() {
        let table = get_table();

        let rows = vec!(RowData::new(table.clone(),
            TransactionId::new(0xDEADBEEF),
            None,
            None,
            vec![
                Some(BuiltinSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BuiltinSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ],
        ).unwrap(), RowData::new(table.clone(),
        TransactionId::new(0xDEADBEEF),
        None,
        None,
        vec![
            Some(BuiltinSqlTypes::Text("this also a test".to_string())),
            None,
            Some(BuiltinSqlTypes::Text("it would help if I didn't mix and match types".to_string())),
        ],
        ).unwrap());

        let mut pg = PageData::new(table.clone(), 0);
        for r in rows.clone() {
            assert!(pg.insert(r.clone()).is_ok());
        }
        let serial = pg.serialize();
        let pg_parsed = PageData::parse(table.clone(), 0, serial).unwrap();

        pin_mut!(pg_parsed);
        let result_rows: Vec<RowData> = aw!(pg_parsed.get_stream().collect());
        assert_eq!(rows, result_rows);
    }
}
