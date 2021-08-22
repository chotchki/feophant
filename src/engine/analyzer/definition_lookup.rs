//! This command will look up ONLY hardcoded table definitions first,
//! should be able to fallback to reading new ones off disk

use super::super::super::constants::TableDefinitions;
use super::super::io::row_formats::{RowData, RowDataError};
use super::super::io::{VisibleRowManager, VisibleRowManagerError};
use super::super::objects::{
    types::{BaseSqlTypes, BaseSqlTypesMapper},
    Attribute, Table, TableError,
};
use super::super::transactions::TransactionId;
use crate::constants::Nullable;
use crate::engine::objects::types::BaseSqlTypesError;
use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio::pin;
use tokio_stream::StreamExt;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct DefinitionLookup {
    vis_row_man: VisibleRowManager,
}

impl DefinitionLookup {
    pub fn new(vis_row_man: VisibleRowManager) -> DefinitionLookup {
        DefinitionLookup { vis_row_man }
    }

    pub async fn get_definition(
        &self,
        tran_id: TransactionId,
        name: String,
    ) -> Result<Arc<Table>, DefinitionLookupError> {
        //System Tables always load
        let system_tables = TableDefinitions::VALUES;
        for i in &system_tables {
            if i.value().name == name {
                return Ok(i.value());
            }
        }

        //TODO not happy with how many strings there are
        let tbl_row = self.get_table_row(tran_id, name).await?;
        let table_id = match tbl_row.get_column_not_null("id")? {
            BaseSqlTypes::Uuid(u) => u,
            _ => return Err(DefinitionLookupError::ColumnWrongType()),
        };
        let table_name = match tbl_row.get_column_not_null("name")? {
            BaseSqlTypes::Text(t) => t,
            _ => return Err(DefinitionLookupError::ColumnWrongType()),
        };

        let tbl_columns = self.get_table_columns(tran_id, table_id).await?;
        let mut tbl_attrs = vec![];
        for c in tbl_columns {
            let c_name = match c.get_column_not_null("attname")? {
                BaseSqlTypes::Text(t) => t,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };
            let c_type = match c.get_column_not_null("atttypid")? {
                BaseSqlTypes::Text(t) => t,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };

            let c_null = match c.get_column_not_null("attnotnull")? {
                BaseSqlTypes::Bool(b) => Nullable::from(b),
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };

            tbl_attrs.push(Attribute::new(
                c_name,
                BaseSqlTypesMapper::from_str(&c_type)?,
                c_null,
                None, //Todo encode the column length
            ));
        }

        Ok(Arc::new(Table::new(table_id, table_name, tbl_attrs)))
    }

    async fn get_table_row(
        &self,
        tran_id: TransactionId,
        name: String,
    ) -> Result<RowData, DefinitionLookupError> {
        //Now we have to search
        let pg_class = TableDefinitions::PgClass.value();
        let row_stream = self.vis_row_man.clone().get_stream(tran_id, pg_class);
        pin!(row_stream);
        while let Some(row_res) = row_stream.next().await {
            let row = row_res?;
            if row.get_column_not_null("name")? == BaseSqlTypes::Text(name.clone()) {
                return Ok(row);
            }
        }

        Err(DefinitionLookupError::TableDoesNotExist(name))
    }

    async fn get_table_columns(
        &self,
        tran_id: TransactionId,
        attrelid: Uuid,
    ) -> Result<Vec<RowData>, DefinitionLookupError> {
        let mut columns = vec![];
        let pg_attr = TableDefinitions::PgAttribute.value();
        let row_stream = self
            .vis_row_man
            .clone()
            .get_stream(tran_id, pg_attr.clone());
        pin!(row_stream);
        while let Some(row_res) = row_stream.next().await {
            let row = row_res?;
            if row.get_column_not_null("attrelid")? == BaseSqlTypes::Uuid(attrelid) {
                columns.push(row);
            }
        }

        if columns.is_empty() {
            return Err(DefinitionLookupError::NoColumnsFound());
        }

        //Figure out what column we're dealing with
        let col_offset = pg_attr.get_column_index("attnum".to_string())?;

        //Extract column number into tuples so we can sort
        let mut column_tuples = vec![];
        for c in &columns {
            let wrapped_value = c
                .user_data
                .0
                .get(col_offset)
                .ok_or(DefinitionLookupError::WrongColumnIndex(col_offset))?;
            let not_null_value = wrapped_value
                .as_ref()
                .ok_or(DefinitionLookupError::ColumnNull(col_offset))?;
            match not_null_value {
                BaseSqlTypes::Integer(i) => column_tuples.push((i, c.clone())),
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            }
        }

        //Now the columns are good but we need to check for gaps
        column_tuples.sort_by(|a, b| a.0.cmp(b.0));
        for (i, tup) in column_tuples.iter().enumerate() {
            let i_u32 = u32::try_from(i)?;
            if tup.0 != &i_u32 {
                return Err(DefinitionLookupError::ColumnGap(i));
            }
        }

        //Re-extract the columns
        let columns = column_tuples.into_iter().map(|c| c.1).collect();

        Ok(columns)
    }
}

#[derive(Debug, Error)]
pub enum DefinitionLookupError {
    #[error("{0} is not a valid table")]
    TableDoesNotExist(String),
    #[error("No columns found")]
    NoColumnsFound(),
    #[error("Column index does not exist {0}")]
    WrongColumnIndex(usize),
    #[error(transparent)]
    BaseSqlTypesError(#[from] BaseSqlTypesError),
    #[error("Column empty on index {0}")]
    ColumnNull(usize),
    #[error("Column wrong type")]
    ColumnWrongType(),
    #[error("Gap in columns found at {0}")]
    ColumnGap(usize),
    #[error(transparent)]
    RowDataError(#[from] RowDataError),
    #[error(transparent)]
    VisibleRowManagerError(#[from] VisibleRowManagerError),
    #[error(transparent)]
    TableError(#[from] TableError),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::engine::io::{FileManager, LockCacheManager};

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::super::super::io::RowManager;
    use super::super::super::transactions::TransactionManager;
    use super::super::super::Engine;
    use super::*;

    #[tokio::test]
    async fn test_find_pg_class() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let tm = TransactionManager::new();
        let rm = RowManager::new(LockCacheManager::new(fm));
        let vm = VisibleRowManager::new(rm, tm);
        let dl = DefinitionLookup::new(vm);

        let tran_id = TransactionId::new(1);

        let pg_class_def = dl.get_definition(tran_id, "pg_class".to_string()).await?;
        assert_eq!(pg_class_def.name, "pg_class".to_string());

        Ok(())
    }

    #[tokio::test]
    async fn test_no_such_class() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let tm = TransactionManager::new();
        let rm = RowManager::new(LockCacheManager::new(fm));
        let vm = VisibleRowManager::new(rm, tm);
        let dl = DefinitionLookup::new(vm);

        let tran_id = TransactionId::new(1);

        let pg_class_def = dl
            .get_definition(tran_id, "something_random".to_string())
            .await;
        match pg_class_def {
            Ok(_) => assert!(false),
            Err(DefinitionLookupError::TableDoesNotExist(_)) => assert!(true),
            _ => assert!(false),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_def_lookup() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let mut tm = TransactionManager::new();
        let rm = RowManager::new(LockCacheManager::new(fm.clone()));
        let vm = VisibleRowManager::new(rm.clone(), tm.clone());
        let dl = DefinitionLookup::new(vm);
        let mut engine = Engine::new(fm, tm.clone());

        let tran = tm.start_trans().await?;
        engine
            .process_query(tran, "create table foo (bar text)".to_string())
            .await?;
        tm.commit_trans(tran).await?;

        let tran = tm.start_trans().await?;
        dl.get_definition(tran, "foo".to_string()).await?;
        tm.commit_trans(tran).await?;

        assert!(true);
        Ok(())
    }
}
