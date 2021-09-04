use async_stream::try_stream;
use futures::Stream;
use std::sync::Arc;
use thiserror::Error;

use crate::{
    constants::Nullable,
    engine::{
        objects::{
            types::{BaseSqlTypes, BaseSqlTypesMapper},
            SqlTuple, Table,
        },
        transactions::TransactionId,
    },
};

use super::{
    row_formats::{ItemPointer, RowData},
    VisibleRowManager, VisibleRowManagerError,
};

/// The goal of the constraint manager is to ensure all constrainst are satisfied
/// before we hand it off deeper into the stack. For now its taking on the null checks
/// of RowData
#[derive(Clone, Debug)]
pub struct ConstraintManager {
    vis_row_man: VisibleRowManager,
}

impl ConstraintManager {
    pub fn new(vis_row_man: VisibleRowManager) -> ConstraintManager {
        ConstraintManager { vis_row_man }
    }

    pub async fn insert_row(
        self,
        current_tran_id: TransactionId,
        table: Arc<Table>,
        user_data: SqlTuple,
    ) -> Result<ItemPointer, ConstraintManagerError> {
        if table.attributes.len() != user_data.0.len() {
            return Err(ConstraintManagerError::TableRowSizeMismatch(
                table.attributes.len(),
                user_data.0.len(),
            ));
        }
        for (data, column) in user_data.0.iter().zip(table.attributes.clone()) {
            match data {
                Some(d) => {
                    if !d.type_matches(&column.sql_type) {
                        return Err(ConstraintManagerError::TableRowTypeMismatch(
                            d.clone(),
                            column.sql_type,
                        ));
                    }
                }
                None => {
                    if column.nullable != Nullable::Null {
                        return Err(ConstraintManagerError::UnexpectedNull(column.name));
                    }
                }
            }
        }

        Ok(self
            .vis_row_man
            .insert_row(current_tran_id, table, user_data)
            .await?)
    }

    /// Gets a specific tuple from below, at the moment just a passthrough
    pub async fn get(
        &self,
        tran_id: TransactionId,
        table: Arc<Table>,
        row_pointer: ItemPointer,
    ) -> Result<RowData, ConstraintManagerError> {
        Ok(self.vis_row_man.get(tran_id, table, row_pointer).await?)
    }

    /// Provides a filtered view that respects transaction visibility
    /// At the moment this is practically just a passthrough
    pub fn get_stream(
        self,
        tran_id: TransactionId,
        table: Arc<Table>,
    ) -> impl Stream<Item = Result<RowData, ConstraintManagerError>> {
        try_stream! {
            for await row in self.vis_row_man.get_stream(tran_id, table) {
                let unwrap_row = row?;
                yield unwrap_row;
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum ConstraintManagerError {
    #[error("Table definition length {0} does not match columns passed {1}")]
    TableRowSizeMismatch(usize, usize),
    #[error("Table definition type {0} does not match column passed {1}")]
    TableRowTypeMismatch(BaseSqlTypes, BaseSqlTypesMapper),
    #[error(transparent)]
    VisibleRowManagerError(#[from] VisibleRowManagerError),
    #[error("Column null when ask not to be {0}")]
    UnexpectedNull(String),
}
