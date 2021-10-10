use async_stream::try_stream;
use futures::Stream;
use std::sync::Arc;
use thiserror::Error;

use crate::{
    constants::Nullable,
    engine::{
        objects::{
            types::{BaseSqlTypes, BaseSqlTypesMapper},
            SqlTuple, SqlTupleError, Table,
        },
        transactions::TransactionId,
    },
};

use super::{
    index_manager::IndexManagerError,
    row_formats::{ItemPointer, RowData},
    IndexManager, VisibleRowManager, VisibleRowManagerError,
};

/// The goal of the constraint manager is to ensure all constraints are satisfied
/// before we hand it off deeper into the stack. For now its taking on the null checks
/// of RowData
#[derive(Clone)]
pub struct ConstraintManager {
    index_manager: IndexManager,
    vis_row_man: VisibleRowManager,
}

impl ConstraintManager {
    pub fn new(index_manager: IndexManager, vis_row_man: VisibleRowManager) -> ConstraintManager {
        ConstraintManager {
            index_manager,
            vis_row_man,
        }
    }

    pub async fn insert_row(
        &mut self,
        current_tran_id: TransactionId,
        table: &Arc<Table>,
        user_data: SqlTuple,
    ) -> Result<ItemPointer, ConstraintManagerError> {
        //column count check
        if table.attributes.len() != user_data.0.len() {
            return Err(ConstraintManagerError::TableRowSizeMismatch(
                table.attributes.len(),
                user_data.0.len(),
            ));
        }

        //null checks
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

        //constraint check
        for c in &table.constraints {
            match c {
                crate::engine::objects::Constraint::PrimaryKey(p) => {
                    debug!("searching for {:?}", user_data);
                    match self
                        .index_manager
                        .search_for_key(
                            &p.index,
                            &user_data
                                .clone()
                                .filter_map(&table.sql_type, &p.index.columns)?,
                        )
                        .await?
                    {
                        Some(rows) => {
                            //We need to check if each of these rows are alive
                            if self
                                .vis_row_man
                                .any_visible(table, current_tran_id, &rows)
                                .await?
                            {
                                return Err(ConstraintManagerError::PrimaryKeyViolation());
                            }
                        }
                        None => {
                            continue;
                        }
                    }
                }
            }
        }

        //Insert the row
        let row_item_ptr = self
            .vis_row_man
            .insert_row(current_tran_id, table, user_data.clone())
            .await?;

        //Update the indexes
        //TODO figure out if that makes sense in this layer
        for i in &table.indexes {
            let tuple_for_index = match user_data.clone().filter_map(&table.sql_type, &i.columns) {
                Ok(u) => u,
                Err(_) => {
                    continue;
                }
            };

            self.index_manager
                .add(i, tuple_for_index, row_item_ptr)
                .await?;
        }

        Ok(row_item_ptr)
    }

    /// Gets a specific tuple from below, at the moment just a passthrough
    pub async fn get(
        &mut self,
        tran_id: TransactionId,
        table: &Arc<Table>,
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
            for await row in self.vis_row_man.get_stream(tran_id, &table) {
                let unwrap_row = row?;
                yield unwrap_row;
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum ConstraintManagerError {
    #[error(transparent)]
    IndexManagerError(#[from] IndexManagerError),
    #[error("Primary Key violation")]
    PrimaryKeyViolation(),
    #[error(transparent)]
    SqlTupleError(#[from] SqlTupleError),
    #[error("Table definition length {0} does not match columns passed {1}")]
    TableRowSizeMismatch(usize, usize),
    #[error("Table definition type {0} does not match column passed {1}")]
    TableRowTypeMismatch(BaseSqlTypes, BaseSqlTypesMapper),
    #[error(transparent)]
    VisibleRowManagerError(#[from] VisibleRowManagerError),
    #[error("Column null when ask not to be {0}")]
    UnexpectedNull(String),
}
