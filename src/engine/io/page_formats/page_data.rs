use crate::engine::io::format_traits::{Parseable, Serializable};
use crate::engine::io::{ConstEncodedSize, EncodedSize};
use crate::engine::objects::SqlTuple;
use crate::engine::transactions::TransactionId;

use super::super::super::objects::Table;
use super::super::row_formats::{ItemPointer, RowData, RowDataError};
use super::{
    ItemIdData, ItemIdDataError, PageHeader, PageHeaderError, PageOffset, UInt12, UInt12Error,
};
use async_stream::stream;
use bytes::{BufMut, Bytes};
use futures::stream::Stream;
use std::convert::TryFrom;
use std::sync::Arc;
use thiserror::Error;

pub struct PageData {
    page: PageOffset,
    page_header: PageHeader,
    item_ids: Vec<ItemIdData>,
    //TODO debating if I should defer parsing until later
    rows: Vec<RowData>,
}

impl PageData {
    pub fn new(page: PageOffset) -> PageData {
        PageData {
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

    pub fn insert(
        &mut self,
        current_tran_id: TransactionId,
        table: &Arc<Table>,
        user_data: SqlTuple,
    ) -> Result<ItemPointer, PageDataError> {
        let item_pointer = ItemPointer::new(self.page, UInt12::try_from(self.rows.len())?);
        let row_data_len = RowData::encoded_size(&user_data);
        let row_data = RowData::new(
            table.sql_type.clone(),
            current_tran_id,
            None,
            item_pointer,
            user_data,
        );

        let item_data = self.page_header.add_item(row_data_len)?;
        self.item_ids.push(item_data);
        self.rows.push(row_data);
        Ok(item_pointer)
    }

    pub fn update(&mut self, row_data: RowData, row_count: UInt12) -> Result<(), PageDataError> {
        let row_data_len = RowData::encoded_size(&row_data.user_data);
        let row_count = row_count.to_usize();
        if row_count > self.item_ids.len() - 1 || row_count > self.rows.len() - 1 {
            return Err(PageDataError::IndexOutofBounds(
                row_count,
                self.item_ids.len(),
                self.rows.len(),
            ));
        }

        let iid = &self.item_ids[row_count];
        if iid.length.to_usize() != row_data_len {
            return Err(PageDataError::UpdateChangedLength(
                iid.length.to_usize(),
                row_data_len,
            ));
        }

        self.rows[row_count] = row_data;
        Ok(())
    }

    pub fn get_row(&self, count: UInt12) -> Option<&RowData> {
        self.rows.get(count.to_usize())
    }

    pub fn get_stream(&self) -> impl Stream<Item = RowData> {
        let rows_clone = self.rows.clone();
        stream! {
            for row in rows_clone.iter() {
                yield row.clone();
            }
        }
    }

    pub fn parse(
        table: &Arc<Table>,
        page: PageOffset,
        buffer: &Bytes,
    ) -> Result<PageData, PageDataError> {
        //Note since we need random access, everything MUST work off slices otherwise counts will be off

        let mut page_header_slice = &buffer[0..PageHeader::encoded_size()];
        let page_header = PageHeader::parse(&mut page_header_slice)?;

        let mut item_ids: Vec<ItemIdData> = Vec::with_capacity(page_header.get_item_count());
        let mut rows: Vec<RowData> = Vec::with_capacity(page_header.get_item_count());
        for i in 0..page_header.get_item_count() {
            let iid_lower_offset = PageHeader::encoded_size() + (ItemIdData::encoded_size() * i);
            let iid_upper_offset =
                PageHeader::encoded_size() + (ItemIdData::encoded_size() * (i + 1));
            let mut iid_slice = &buffer[iid_lower_offset..iid_upper_offset];
            let iid = ItemIdData::parse(&mut iid_slice)?;

            let mut row_slice = &buffer[iid.get_range()];
            let row = RowData::parse(table.clone(), &mut row_slice)?;
            item_ids.push(iid);
            rows.push(row);
        }

        Ok(PageData {
            page,
            page_header,
            item_ids,
            rows,
        })
    }
}

impl Serializable for PageData {
    fn serialize(&self, buffer: &mut impl BufMut) {
        self.page_header.serialize(buffer);

        //Now write items data in order
        self.item_ids.iter().for_each(|f| f.serialize(buffer));

        //Fill the free space
        let free_space = vec![0; self.page_header.get_free_space()];
        buffer.put_slice(&free_space);

        //Write items in reverse order
        self.rows.iter().rev().for_each(|r| r.serialize(buffer));
    }
}

#[derive(Debug, Error)]
pub enum PageDataError {
    #[error(transparent)]
    PageHeaderParseError(#[from] PageHeaderError),
    #[error(transparent)]
    ItemIdDataParseError(#[from] ItemIdDataError),
    #[error(transparent)]
    RowDataParseError(#[from] RowDataError),
    #[error(transparent)]
    UInt12Error(#[from] UInt12Error),
    #[error("Row {0} does not exist to update we have {1}:{2} rows")]
    IndexOutofBounds(usize, usize, usize),
    #[error("Updates cannot change row length! Old: {0} New: {1}")]
    UpdateChangedLength(usize, usize),
}

#[cfg(test)]
mod tests {
    use crate::constants::PAGE_SIZE;
    use crate::engine::get_table;
    use crate::engine::objects::SqlTuple;

    use super::super::super::super::objects::types::BaseSqlTypes;
    use super::super::super::super::transactions::TransactionId;
    use super::*;
    use bytes::BytesMut;
    use futures::pin_mut;
    use tokio_stream::StreamExt;

    fn get_item_pointer(row_num: usize) -> ItemPointer {
        ItemPointer::new(PageOffset(0), UInt12::new(row_num as u16).unwrap())
    }

    #[tokio::test]
    async fn test_page_data_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let table = get_table();

        let rows = vec!(RowData::new(table.sql_type.clone(),
            TransactionId::new(0xDEADBEEF),
            None,
            get_item_pointer(0),
            SqlTuple(vec![
                Some(BaseSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BaseSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ]),
        ));

        let mut pd = PageData::new(PageOffset(0));
        for r in rows.clone() {
            assert!(pd.insert(r.min, &table, r.user_data).is_ok());
        }
        let mut serial = BytesMut::with_capacity(PAGE_SIZE as usize);
        pd.serialize(&mut serial);

        assert_eq!(PAGE_SIZE as usize, serial.len());
        let pg_parsed = PageData::parse(&table, PageOffset(0), &serial.freeze()).unwrap();

        pin_mut!(pg_parsed);
        let result_rows: Vec<RowData> = pg_parsed.get_stream().collect().await;
        assert_eq!(rows, result_rows);

        Ok(())
    }

    #[tokio::test]
    async fn test_page_data_roundtrip_two_rows() {
        let table = get_table();

        let rows = vec!(RowData::new(table.sql_type.clone(),
            TransactionId::new(0xDEADBEEF),
            None,
            get_item_pointer(0),
            SqlTuple(vec![
                Some(BaseSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BaseSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ]),
        ), RowData::new(table.sql_type.clone(),
        TransactionId::new(0xDEADBEEF),
        None,
        get_item_pointer(1),
        SqlTuple(vec![
            Some(BaseSqlTypes::Text("this also a test".to_string())),
            None,
            Some(BaseSqlTypes::Text("it would help if I didn't mix and match types".to_string())),
        ]),
        ));

        let mut pd = PageData::new(PageOffset(0));
        for r in rows.clone() {
            assert!(pd.insert(r.min, &table, r.user_data).is_ok());
        }
        let mut serial = BytesMut::with_capacity(PAGE_SIZE as usize);
        pd.serialize(&mut serial);
        let pg_parsed = PageData::parse(&table, PageOffset(0), &serial.freeze()).unwrap();

        pin_mut!(pg_parsed);
        let result_rows: Vec<RowData> = pg_parsed.get_stream().collect().await;
        assert_eq!(rows, result_rows);
    }

    #[tokio::test]
    async fn test_page_data_update() {
        let table = get_table();

        let mut row = RowData::new(table.sql_type.clone(),
            TransactionId::new(0xDEADBEEF),
            None,
            get_item_pointer(0),
            SqlTuple(vec![
                Some(BaseSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BaseSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ]),
        );

        let mut pd = PageData::new(PageOffset(0));
        let rip = pd.insert(row.min, &table, row.user_data.clone());
        assert!(rip.is_ok());

        let ip = rip.unwrap();

        row.item_pointer = get_item_pointer(1);

        assert!(pd.update(row.clone(), ip.count).is_ok());

        pin_mut!(pd);
        let result_rows: Vec<RowData> = pd.get_stream().collect().await;
        assert_eq!(row, result_rows[0]);
    }
}
