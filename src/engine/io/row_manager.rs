use super::super::objects::Table;
use super::super::transactions::TransactionId;
use super::page_formats::{PageData, PageDataError, UInt12};
use super::row_formats::{ItemPointer, RowData, RowDataError};
use super::{EncodedSize, IOManager, IOManagerError};
use crate::engine::objects::SqlTuple;
use async_stream::try_stream;
use futures::stream::Stream;
use std::sync::Arc;
use thiserror::Error;

/// The row manager is a mapper between rows and pages on disk.
///
/// It operates at the lowest level, no visibility checks are done.
#[derive(Clone, Debug)]
pub struct RowManager {
    io_manager: IOManager,
}

impl RowManager {
    pub fn new(io_manager: IOManager) -> RowManager {
        RowManager { io_manager }
    }

    pub async fn insert_row(
        self,
        current_tran_id: TransactionId,
        table: Arc<Table>,
        user_data: SqlTuple,
    ) -> Result<ItemPointer, RowManagerError> {
        RowManager::insert_row_internal(self.io_manager.clone(), current_tran_id, table, user_data)
            .await
    }

    //Note this is a logical delete
    //TODO debating if this should respect the visibility map, probably yes just trying to limit the pain
    pub async fn delete_row(
        self,
        current_tran_id: TransactionId,
        table: Arc<Table>,
        row_pointer: ItemPointer,
    ) -> Result<(), RowManagerError> {
        let (mut page, mut row) = self.get(table.clone(), row_pointer).await?;

        if row.max.is_some() {
            return Err(RowManagerError::AlreadyDeleted(
                row_pointer.count,
                row.max.unwrap(),
            ));
        }

        row.max = Some(current_tran_id);

        page.update(row, row_pointer.count)?;

        self.io_manager
            .update_page(&table.id, page.serialize(), row_pointer.page)
            .await?;
        Ok(())
    }

    //Note this is an insert new row, delete old row operation
    pub async fn update_row(
        &mut self,
        current_tran_id: TransactionId,
        table: Arc<Table>,
        row_pointer: ItemPointer,
        new_user_data: SqlTuple,
    ) -> Result<ItemPointer, RowManagerError> {
        //First get the current row so we have it for the update/delete
        let (mut old_page, mut old_row) = self.get(table.clone(), row_pointer).await?;

        if old_row.max.is_some() {
            return Err(RowManagerError::AlreadyDeleted(
                row_pointer.count,
                old_row.max.unwrap(),
            ));
        }

        let new_row_len = RowData::encoded_size(&new_user_data);

        //Prefer using the old page if possible
        let new_row_pointer;
        if old_page.can_fit(new_row_len) {
            new_row_pointer = old_page.insert(current_tran_id, &table, new_user_data)?;
        } else {
            new_row_pointer = RowManager::insert_row_internal(
                self.io_manager.clone(),
                current_tran_id,
                table.clone(),
                new_user_data,
            )
            .await?;
        }

        old_row.max = Some(current_tran_id);
        old_row.item_pointer = new_row_pointer;
        old_page.update(old_row, row_pointer.count)?;

        self.io_manager
            .update_page(&table.id, old_page.serialize(), row_pointer.page)
            .await?;

        return Ok(new_row_pointer);
    }

    pub async fn get(
        &self,
        table: Arc<Table>,
        row_pointer: ItemPointer,
    ) -> Result<(PageData, RowData), RowManagerError> {
        let page_bytes = self
            .io_manager
            .get_page(&table.id, row_pointer.page)
            .await
            .ok_or_else(|| RowManagerError::NonExistentPage(row_pointer.page))?;
        let page = PageData::parse(table, row_pointer.page, page_bytes)?;

        let row = page
            .get_row(row_pointer.count)
            .ok_or_else(|| RowManagerError::NonExistentRow(row_pointer.count, row_pointer.page))?
            .clone();

        Ok((page, row))
    }

    // Provides an unfiltered view of the underlying table
    pub fn get_stream(
        self,
        table: Arc<Table>,
    ) -> impl Stream<Item = Result<RowData, RowManagerError>> {
        try_stream! {
            let mut page_num = 0;
            for await page_bytes in self.io_manager.get_stream(table.id) {
                let page = PageData::parse(table.clone(), page_num, page_bytes)?;
                for await row in page.get_stream() {
                    yield row;
                }
                page_num += 1;
            }
        }
    }

    async fn insert_row_internal(
        io_manager: IOManager,
        current_tran_id: TransactionId,
        table: Arc<Table>,
        user_data: SqlTuple,
    ) -> Result<ItemPointer, RowManagerError> {
        let mut page_num = 0;
        loop {
            let page_bytes = io_manager.get_page(&table.id, page_num).await;
            match page_bytes {
                Some(p) => {
                    let mut page = PageData::parse(table.clone(), page_num, p)?;
                    if page.can_fit(RowData::encoded_size(&user_data)) {
                        let new_row_pointer = page.insert(current_tran_id, &table, user_data)?;
                        let new_page_bytes = page.serialize();
                        io_manager
                            .update_page(&table.id, new_page_bytes, page_num)
                            .await?;
                        return Ok(new_row_pointer);
                    } else {
                        page_num += 1;
                        continue;
                    }
                }
                None => {
                    let mut new_page = PageData::new(page_num);
                    let new_row_pointer = new_page.insert(current_tran_id, &table, user_data)?; //TODO Will NOT handle overly large rows
                    io_manager.add_page(&table.id, new_page.serialize()).await?;
                    return Ok(new_row_pointer);
                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum RowManagerError {
    #[error(transparent)]
    PageDataError(#[from] PageDataError),
    #[error(transparent)]
    IOManagerError(#[from] IOManagerError),
    #[error(transparent)]
    RowDataError(#[from] RowDataError),
    #[error("Page {0} does not exist")]
    NonExistentPage(usize),
    #[error("Row {0} in Page {1} does not exist")]
    NonExistentRow(UInt12, usize),
    #[error("Row {0} already deleted in {1}")]
    AlreadyDeleted(UInt12, TransactionId),
    #[error("Row {0} is not visible")]
    NotVisibleRow(RowData),
}

#[cfg(test)]
mod tests {
    use super::super::super::objects::Attribute;
    use super::super::super::objects::Table;
    use super::*;
    use crate::constants::Nullable;
    use crate::engine::objects::types::BaseSqlTypes;
    use crate::engine::objects::types::BaseSqlTypesMapper;
    use futures::pin_mut;
    use futures::stream::StreamExt;

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    fn get_table() -> Arc<Table> {
        Arc::new(Table::new(
            uuid::Uuid::new_v4(),
            "test_table".to_string(),
            vec![
                Attribute::new(
                    "header".to_string(),
                    BaseSqlTypesMapper::Text,
                    Nullable::NotNull,
                    None,
                ),
                Attribute::new(
                    "id".to_string(),
                    BaseSqlTypesMapper::Uuid,
                    Nullable::Null,
                    None,
                ),
                Attribute::new(
                    "header3".to_string(),
                    BaseSqlTypesMapper::Text,
                    Nullable::NotNull,
                    None,
                ),
            ],
        ))
    }

    fn get_row(input: String) -> SqlTuple {
        SqlTuple(vec![
                Some(BaseSqlTypes::Text(input)),
                None,
                Some(BaseSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ])
    }

    #[test]
    fn test_row_manager_mass_insert() -> Result<(), Box<dyn std::error::Error>> {
        let table = get_table();
        let pm = IOManager::new();
        let rm = RowManager::new(pm);

        let tran_id = TransactionId::new(1);

        for _ in 0..500 {
            assert!(aw!(rm.clone().insert_row(
                tran_id,
                table.clone(),
                get_row("test".to_string())
            ))
            .is_ok());
        }

        //Now let's make sure they're really in the table
        pin_mut!(rm);
        let result_rows: Vec<RowData> = aw!(rm
            .clone()
            .get_stream(table.clone())
            .map(Result::unwrap)
            .collect());

        let sample_row = get_row("test".to_string());
        for row in result_rows {
            assert_eq!(row.user_data, sample_row);
        }

        Ok(())
    }

    #[test]
    fn test_row_manager_crud() -> Result<(), Box<dyn std::error::Error>> {
        let table = get_table();
        let pm = IOManager::new();
        let rm = RowManager::new(pm);

        let tran_id = TransactionId::new(1);

        let insert_pointer =
            aw!(rm
                .clone()
                .insert_row(tran_id, table.clone(), get_row("test".to_string())))?;

        let tran_id_2 = TransactionId::new(3);

        let update_pointer = aw!(rm.clone().update_row(
            tran_id_2,
            table.clone(),
            insert_pointer,
            get_row("test2".to_string())
        ))?;

        //Now let's make sure the update took
        pin_mut!(rm);
        let result_rows: Vec<RowData> = aw!(rm
            .clone()
            .get_stream(table.clone())
            .map(Result::unwrap)
            .collect());
        assert_eq!(result_rows.len(), 2);

        let tran_id_3 = TransactionId::new(3);

        aw!(rm
            .clone()
            .delete_row(tran_id_3, table.clone(), update_pointer))?;

        Ok(())
    }
}
