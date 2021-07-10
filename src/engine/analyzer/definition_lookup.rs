//! This command will look up ONLY hardcoded table definitions first,
//! should be able to fallback to reading new ones off disk

use crate::constants::Nullable;

use super::super::super::constants::{
    BuiltinSqlTypes, DeserializeTypes, SqlTypeError, TableDefinitions,
};
use super::super::io::row_formats::{RowData, RowDataError};
use super::super::io::{VisibleRowManager, VisibleRowManagerError};
use super::super::objects::{Attribute, Table, TableError};
use super::super::transactions::TransactionId;
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

        let tbl_row = self.get_table_row(tran_id, name).await?;
        let table_id = match tbl_row.get_column_not_null("id".to_string())? {
            BuiltinSqlTypes::Uuid(u) => u,
            _ => return Err(DefinitionLookupError::ColumnWrongType()),
        };
        let table_name = match tbl_row.get_column_not_null("name".to_string())? {
            BuiltinSqlTypes::Text(t) => t,
            _ => return Err(DefinitionLookupError::ColumnWrongType()),
        };

        let tbl_columns = self.get_table_columns(tran_id, table_id).await?;
        let mut tbl_attrs = vec![];
        for c in tbl_columns {
            let c_name = match c.get_column_not_null("attname".to_string())? {
                BuiltinSqlTypes::Text(t) => t,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };
            let c_type = match c.get_column_not_null("atttypid".to_string())? {
                BuiltinSqlTypes::Text(t) => t,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };

            let c_null = match c.get_column_not_null("attnotnull".to_string())? {
                BuiltinSqlTypes::Bool(b) => Nullable::from(b),
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };

            tbl_attrs.push(Attribute::new(
                //TODO: Oops didn't store the column's id
                table_id,
                c_name,
                DeserializeTypes::from_str(&c_type)?,
                c_null,
            ));
        }

        Ok(Arc::new(Table::new_existing(
            table_id, table_name, tbl_attrs,
        )))
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
            if row.get_column_not_null("name".to_string())? == BuiltinSqlTypes::Text(name.clone()) {
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
            if row.get_column_not_null("attrelid".to_string())? == BuiltinSqlTypes::Uuid(attrelid) {
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
                .ok_or_else(|| DefinitionLookupError::WrongColumnIndex(col_offset))?;
            let not_null_value = wrapped_value
                .as_ref()
                .ok_or_else(|| DefinitionLookupError::ColumnNull(col_offset))?;
            match not_null_value {
                BuiltinSqlTypes::Integer(i) => column_tuples.push((i, c.clone())),
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            }
        }

        //Now the columns are good but we need to check for gaps
        column_tuples.sort_by(|a, b| a.0.cmp(&b.0));
        for i in 0..column_tuples.len() {
            let i_u32 = u32::try_from(i)?;
            if column_tuples[i].0 != &i_u32 {
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
    SqlTypeError(#[from] SqlTypeError),
    #[error(transparent)]
    TableError(#[from] TableError),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::super::super::io::{IOManager, RowManager};
    use super::super::super::transactions::TransactionManager;
    use super::super::super::Engine;
    use super::*;
    use tokio::sync::RwLock;

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    fn test_find_pg_class() {
        let pm = Arc::new(RwLock::new(IOManager::new()));
        let tm = TransactionManager::new();
        let rm = RowManager::new(pm);
        let vm = VisibleRowManager::new(rm, tm);
        let dl = DefinitionLookup::new(vm);

        let tran_id = TransactionId::new(1);

        let pg_class_def = aw!(dl.get_definition(tran_id, "pg_class".to_string())).unwrap();
        assert_eq!(pg_class_def.name, "pg_class".to_string());
    }

    #[test]
    fn test_no_such_class() {
        let pm = Arc::new(RwLock::new(IOManager::new()));
        let tm = TransactionManager::new();
        let rm = RowManager::new(pm);
        let vm = VisibleRowManager::new(rm, tm);
        let dl = DefinitionLookup::new(vm);

        let tran_id = TransactionId::new(1);

        let pg_class_def = aw!(dl.get_definition(tran_id, "something_random".to_string()));
        match pg_class_def {
            Ok(_) => assert!(false),
            Err(DefinitionLookupError::TableDoesNotExist(_)) => assert!(true),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_def_lookup() -> Result<(), Box<dyn std::error::Error>> {
        let pm = Arc::new(RwLock::new(IOManager::new()));
        let mut tm = TransactionManager::new();
        let rm = RowManager::new(pm.clone());
        let vm = VisibleRowManager::new(rm.clone(), tm.clone());
        let dl = DefinitionLookup::new(vm);
        let mut engine = Engine::new(pm, tm.clone());

        let tran = aw!(tm.start_trans())?;
        aw!(engine.process_query(tran, "create table foo (bar text)".to_string()))?;
        aw!(tm.commit_trans(tran))?;

        let tran = aw!(tm.start_trans())?;
        aw!(dl.get_definition(tran, "foo".to_string()))?;
        aw!(tm.commit_trans(tran))?;

        assert!(true);
        Ok(())
    }
}
