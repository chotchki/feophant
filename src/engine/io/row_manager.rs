use super::super::objects::Table;
use super::super::transactions::TransactionId;
use super::block_layer::file_manager2::{FileManager2, FileManager2Error};
use super::block_layer::free_space_manager::{FreeSpaceManager, FreeSpaceManagerError, FreeStat};
use super::format_traits::Serializable;
use super::page_formats::{PageData, PageDataError, PageId, PageOffset, PageType, UInt12};
use super::row_formats::{ItemPointer, RowData, RowDataError};
use super::EncodedSize;
use crate::engine::objects::SqlTuple;
use async_stream::try_stream;
use futures::stream::Stream;
use std::sync::Arc;
use thiserror::Error;

/// The row manager is a mapper between rows and pages on disk.
///
/// It operates at the lowest level, no visibility checks are done.
#[derive(Clone)]
pub struct RowManager {
    file_manager: Arc<FileManager2>,
    free_space_manager: FreeSpaceManager,
}

impl RowManager {
    pub fn new(
        file_manager: Arc<FileManager2>,
        free_space_manager: FreeSpaceManager,
    ) -> RowManager {
        RowManager {
            file_manager,
            free_space_manager,
        }
    }

    pub async fn insert_row(
        &self,
        current_tran_id: TransactionId,
        table: &Arc<Table>,
        user_data: SqlTuple,
    ) -> Result<ItemPointer, RowManagerError> {
        self.insert_row_internal(current_tran_id, table, user_data)
            .await
    }

    //Note this is a logical delete
    //TODO debating if this should respect the visibility map, probably yes just trying to limit the pain
    pub async fn delete_row(
        &self,
        current_tran_id: TransactionId,
        table: &Arc<Table>,
        row_pointer: ItemPointer,
    ) -> Result<(), RowManagerError> {
        let page_id = PageId {
            resource_key: table.id,
            page_type: PageType::Data,
        };

        let (page, page_guard) = self
            .file_manager
            .get_page_for_update(&page_id, &row_pointer.page)
            .await?;

        let mut page = PageData::parse(table, row_pointer.page, &page)?;
        let mut row = page
            .get_row(row_pointer.count)
            .ok_or(RowManagerError::NonExistentRow(
                row_pointer.count,
                row_pointer.page,
            ))?
            .clone();

        if row.max.is_some() {
            return Err(RowManagerError::AlreadyDeleted(
                row_pointer.count,
                row.max.unwrap(),
            ));
        }

        row.max = Some(current_tran_id);

        page.update(row, row_pointer.count)?;
        let new_page = page.serialize_and_pad();

        self.file_manager.update_page(page_guard, new_page).await?;

        Ok(())
    }

    //Note this is an insert new row, delete old row operation
    pub async fn update_row(
        &mut self,
        current_tran_id: TransactionId,
        table: &Arc<Table>,
        row_pointer: ItemPointer,
        new_user_data: SqlTuple,
    ) -> Result<ItemPointer, RowManagerError> {
        //First get the current row so we have it for the update
        let page_id = PageId {
            resource_key: table.id,
            page_type: PageType::Data,
        };

        let (old_page_buffer, old_guard) = self
            .file_manager
            .get_page_for_update(&page_id, &row_pointer.page)
            .await?;

        let mut old_page = PageData::parse(table, row_pointer.page, &old_page_buffer)?;

        let mut old_row = old_page
            .get_row(row_pointer.count)
            .ok_or(RowManagerError::NonExistentRow(
                row_pointer.count,
                row_pointer.page,
            ))?
            .clone();

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
            new_row_pointer = old_page.insert(current_tran_id, table, new_user_data)?;
        } else {
            self.free_space_manager
                .mark_page(page_id, row_pointer.page, FreeStat::Full)
                .await?;
            new_row_pointer = self
                .insert_row_internal(current_tran_id, table, new_user_data)
                .await?;
        }

        old_row.max = Some(current_tran_id);
        old_row.item_pointer = new_row_pointer;
        old_page.update(old_row, row_pointer.count)?;
        let old_page_buffer = old_page.serialize_and_pad();

        self.file_manager
            .update_page(old_guard, old_page_buffer)
            .await?;

        Ok(new_row_pointer)
    }

    pub async fn get(
        &self,
        table: &Arc<Table>,
        row_pointer: ItemPointer,
    ) -> Result<RowData, RowManagerError> {
        let page_id = PageId {
            resource_key: table.id,
            page_type: PageType::Data,
        };

        let (page_buffer, _page_guard) = self
            .file_manager
            .get_page(&page_id, &row_pointer.page)
            .await?;

        let page = PageData::parse(table, row_pointer.page, &page_buffer)?;

        let row = page
            .get_row(row_pointer.count)
            .ok_or(RowManagerError::NonExistentRow(
                row_pointer.count,
                row_pointer.page,
            ))?
            .clone();

        Ok(row)
    }

    // Provides an unfiltered view of the underlying table
    pub fn get_stream(
        &self,
        table: &Arc<Table>,
    ) -> impl Stream<Item = Result<RowData, RowManagerError>> {
        let page_id = PageId {
            resource_key: table.id,
            page_type: PageType::Data,
        };

        let file_manager = self.file_manager.clone();
        let table = table.clone();

        try_stream! {
            let mut page_num = PageOffset(0);

            loop {
                match file_manager.get_page(&page_id, &page_num).await {
                    Ok((buffer, _guard)) => {
                        let page = PageData::parse(&table, page_num, &buffer)?;
                        for await row in page.get_stream() {
                            yield row;
                        }
                    },
                    Err(_) => {
                        return ();
                    }
                }

                page_num += PageOffset(1);
            }
        }
    }

    // TODO implement free space maps so I don't have to scan every page
    async fn insert_row_internal(
        &self,
        current_tran_id: TransactionId,
        table: &Arc<Table>,
        user_data: SqlTuple,
    ) -> Result<ItemPointer, RowManagerError> {
        let page_id = PageId {
            resource_key: table.id,
            page_type: PageType::Data,
        };
        let user_data_size = RowData::encoded_size(&user_data);

        loop {
            let next_free_page = self.free_space_manager.get_next_free_page(page_id).await?;
            match self
                .file_manager
                .get_page_for_update(&page_id, &next_free_page)
                .await
            {
                Ok((buffer, page_guard)) => {
                    let mut page = PageData::parse(table, next_free_page, &buffer)?;
                    if page.can_fit(user_data_size) {
                        let new_row_pointer = page.insert(current_tran_id, table, user_data)?;
                        let buffer = page.serialize_and_pad();
                        self.file_manager.update_page(page_guard, buffer).await?;
                        return Ok(new_row_pointer);
                    } else {
                        self.free_space_manager
                            .mark_page(page_id, next_free_page, FreeStat::Full)
                            .await?;
                        continue;
                    }
                }
                Err(_) => {
                    //We got here because we asked for an offset that didn't exist yet.
                    let (new_page_offset, new_page_guard) =
                        self.file_manager.get_next_offset(&page_id).await?;

                    let mut new_page = PageData::new(new_page_offset);
                    let new_row_pointer = new_page.insert(current_tran_id, table, user_data)?; //TODO Will NOT handle overly large rows

                    let new_page_buffer = new_page.serialize_and_pad();

                    self.file_manager
                        .add_page(new_page_guard, new_page_buffer)
                        .await?;
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
    FileManager2Error(#[from] FileManager2Error),
    #[error(transparent)]
    FreeSpaceManagerError(#[from] FreeSpaceManagerError),
    #[error(transparent)]
    RowDataError(#[from] RowDataError),
    #[error("Page {0} does not exist")]
    NonExistentPage(PageOffset),
    #[error("Row {0} in Page {1} does not exist")]
    NonExistentRow(UInt12, PageOffset),
    #[error("Row {0} already deleted in {1}")]
    AlreadyDeleted(UInt12, TransactionId),
    #[error("Row {0} is not visible")]
    NotVisibleRow(RowData),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::get_row;
    use crate::engine::get_table;
    use futures::pin_mut;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_row_manager_mass_insert() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let table = get_table();
        let fm = Arc::new(FileManager2::new(tmp_dir.clone())?);
        let fsm = FreeSpaceManager::new(fm.clone());
        let rm = RowManager::new(fm, fsm);

        let tran_id = TransactionId::new(1);

        for i in 0..50 {
            rm.clone()
                .insert_row(tran_id, &table, get_row(i.to_string()))
                .await?;
        }

        drop(rm);

        //Now let's make sure they're really in the table, persisting across restarts
        let fm = Arc::new(FileManager2::new(tmp_dir.clone())?);
        let fsm = FreeSpaceManager::new(fm.clone());
        let rm = RowManager::new(fm, fsm);

        pin_mut!(rm);
        let result_rows: Vec<RowData> = rm
            .clone()
            .get_stream(&table)
            .map(Result::unwrap)
            .collect()
            .await;

        assert_eq!(result_rows.len(), 50);
        result_rows
            .iter()
            .enumerate()
            .take(50)
            .map(|(i, row)| {
                let sample_row = get_row(i.to_string());
                assert_eq!(row.user_data, sample_row);
            })
            .for_each(drop);

        Ok(())
    }

    #[tokio::test]
    async fn test_row_manager_crud() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let table = get_table();
        let fm = Arc::new(FileManager2::new(tmp_dir.clone())?);
        let fsm = FreeSpaceManager::new(fm.clone());
        let mut rm = RowManager::new(fm, fsm);

        let tran_id = TransactionId::new(1);

        let insert_pointer = rm
            .insert_row(tran_id, &table, get_row("test".to_string()))
            .await?;

        let tran_id_2 = TransactionId::new(3);

        let update_pointer = rm
            .update_row(
                tran_id_2,
                &table,
                insert_pointer,
                get_row("test2".to_string()),
            )
            .await?;

        //Now let's make sure the update took
        pin_mut!(rm);
        let result_rows: Vec<RowData> = rm.get_stream(&table).map(Result::unwrap).collect().await;
        assert_eq!(result_rows.len(), 2);

        let tran_id_3 = TransactionId::new(3);

        rm.delete_row(tran_id_3, &table, update_pointer).await?;

        Ok(())
    }
}
