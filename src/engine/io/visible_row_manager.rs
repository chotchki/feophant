//! This sits above the row manager and ensures that all commands follow the visibility rules
//! See here for basic discussion: http://www.interdb.jp/pg/pgsql05.html#_5.6.
//!
//! If you need to bypass this, go down a layer
use super::super::super::constants::BuiltinSqlTypes;
use super::super::objects::Table;
use super::super::transactions::{
    TransactionId, TransactionManager, TransactionManagerError, TransactionStatus,
};
use super::{
    page_formats::PageData,
    row_formats::{ItemPointer, RowData},
    RowManager, RowManagerError,
};
use async_stream::try_stream;
use futures::stream::Stream;
use log::debug;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct VisibleRowManager {
    row_manager: RowManager,
    tran_manager: TransactionManager,
}

impl VisibleRowManager {
    pub fn new(row_manager: RowManager, tran_manager: TransactionManager) -> VisibleRowManager {
        VisibleRowManager {
            row_manager,
            tran_manager,
        }
    }

    pub async fn insert_row(
        self,
        current_tran_id: TransactionId,
        table: Arc<Table>,
        user_data: Vec<Option<BuiltinSqlTypes>>,
    ) -> Result<ItemPointer, VisibleRowManagerError> {
        self.row_manager
            .insert_row(current_tran_id, table, user_data)
            .await
            .map_err(VisibleRowManagerError::RowManagerError)
    }

    pub async fn get(
        &self,
        tran_id: TransactionId,
        table: Arc<Table>,
        row_pointer: ItemPointer,
    ) -> Result<(PageData, RowData), VisibleRowManagerError> {
        let (page, row) = self.row_manager.get(table, row_pointer).await?;

        if VisibleRowManager::is_visible(self.tran_manager.clone(), tran_id, &row).await? {
            Ok((page, row))
        } else {
            Err(VisibleRowManagerError::NotVisibleRow(row))
        }
    }

    // Provides a filtered view that respects transaction visability
    pub fn get_stream(
        self,
        tran_id: TransactionId,
        table: Arc<Table>,
    ) -> impl Stream<Item = Result<RowData, VisibleRowManagerError>> {
        try_stream! {
            let tm = self.tran_manager;

            for await row in self.row_manager.get_stream(table) {
                let unwrap_row = row?;
                if VisibleRowManager::is_visible(tm.clone(), tran_id, &unwrap_row).await? {
                    debug!("Found visible row {:?}", unwrap_row);
                    yield unwrap_row;
                } else {
                    debug!("Found not visible row {:?}", unwrap_row);
                }
            }
        }
    }

    //TODO I want to find a way to NOT depend on tm
    async fn is_visible(
        mut tm: TransactionManager,
        tran_id: TransactionId,
        row_data: &RowData,
    ) -> Result<bool, VisibleRowManagerError> {
        if row_data.min == tran_id {
            match row_data.max {
                Some(m) => {
                    if m == tran_id {
                        return Ok(false);
                    } else {
                        //In the future for us since min cannot be greater than max
                        return Ok(true);
                    }
                }
                None => return Ok(true),
            }
        }

        //TODO check hint bits
        if row_data.min > tran_id {
            return Ok(false);
        }

        if tm.get_status(row_data.min).await? != TransactionStatus::Commited {
            return Ok(false);
        }

        match row_data.max {
            Some(m) => {
                if m > tran_id {
                    return Ok(true);
                }
                if tm.get_status(m).await? != TransactionStatus::Commited {
                    return Ok(true);
                } else {
                    return Ok(false);
                }
            }
            None => {
                return Ok(true);
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum VisibleRowManagerError {
    #[error("Row {0} is not visible")]
    NotVisibleRow(RowData),
    #[error("Test")]
    Test(),
    #[error(transparent)]
    RowManagerError(#[from] RowManagerError),
    #[error(transparent)]
    TransactionManagerError(#[from] TransactionManagerError),
}
