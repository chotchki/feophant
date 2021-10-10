//! This sits above the row manager and ensures that all commands follow the visibility rules
//! See here for basic discussion: http://www.interdb.jp/pg/pgsql05.html#_5.6.
//!
//! If you need to bypass this, go down a layer
use crate::engine::objects::SqlTuple;

use super::super::objects::Table;
use super::super::transactions::{
    TransactionId, TransactionManager, TransactionManagerError, TransactionStatus,
};
use super::{
    row_formats::{ItemPointer, RowData},
    RowManager, RowManagerError,
};
use async_stream::try_stream;
use futures::stream::Stream;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone)]
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
        &self,
        current_tran_id: TransactionId,
        table: &Arc<Table>,
        user_data: SqlTuple,
    ) -> Result<ItemPointer, VisibleRowManagerError> {
        self.row_manager
            .insert_row(current_tran_id, table, user_data)
            .await
            .map_err(VisibleRowManagerError::RowManagerError)
    }

    pub async fn get(
        &mut self,
        tran_id: TransactionId,
        table: &Arc<Table>,
        row_pointer: ItemPointer,
    ) -> Result<RowData, VisibleRowManagerError> {
        let row = self.row_manager.get(table, row_pointer).await?;

        if VisibleRowManager::is_visible(&mut self.tran_manager, tran_id, &row).await? {
            Ok(row)
        } else {
            Err(VisibleRowManagerError::NotVisibleRow(row))
        }
    }

    // Provides a filtered view that respects transaction visability
    pub fn get_stream(
        &self,
        tran_id: TransactionId,
        table: &Arc<Table>,
    ) -> impl Stream<Item = Result<RowData, VisibleRowManagerError>> {
        let rm = self.row_manager.clone();
        let mut tm = self.tran_manager.clone();
        let table = table.clone();

        try_stream! {
            for await row in rm.get_stream(&table) {
                let unwrap_row = row?;
                if VisibleRowManager::is_visible(&mut tm, tran_id, &unwrap_row).await? {
                    yield unwrap_row;
                }
            }
        }
    }

    pub async fn any_visible(
        &mut self,
        table: &Arc<Table>,
        tran_id: TransactionId,
        ptrs: &Vec<ItemPointer>,
    ) -> Result<bool, VisibleRowManagerError> {
        for p in ptrs {
            match self.get(tran_id, table, *p).await {
                Ok(o) => return Ok(true),
                Err(VisibleRowManagerError::NotVisibleRow(_)) => continue,
                Err(e) => {
                    return Err(e);
                }
            }
        }
        return Ok(false);
    }

    //TODO I want to find a way to NOT depend on tm
    async fn is_visible(
        tm: &mut TransactionManager,
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
                if m > tran_id || tm.get_status(m).await? != TransactionStatus::Commited {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            None => Ok(true),
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
