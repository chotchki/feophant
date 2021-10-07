//! This command will look up ONLY hardcoded table definitions first,
//! should be able to fallback to reading new ones off disk
//TODO we should use the existing sql query functionality instead of this handcoded stuff
use super::super::io::row_formats::{RowData, RowDataError};
use super::super::io::{VisibleRowManager, VisibleRowManagerError};
use super::super::objects::{
    types::{BaseSqlTypes, BaseSqlTypesMapper},
    Attribute, Table, TableError,
};
use super::super::transactions::TransactionId;
use crate::constants::system_tables::{pg_attribute, pg_class, pg_constraint, pg_index};
use crate::constants::{Nullable, SystemTables};
use crate::engine::objects::types::{BaseSqlTypesError, SqlTypeDefinition};
use crate::engine::objects::{Constraint, Index, PrimaryKeyConstraint};
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
        let system_tables = SystemTables::VALUES;
        for i in &system_tables {
            if i.value().name == name {
                return Ok(i.value());
            }
        }

        let pg_class_entry = self.get_table_row(tran_id, name.clone()).await?;
        let table_id = match pg_class_entry.get_column_not_null(pg_class::COLUMN_ID)? {
            BaseSqlTypes::Uuid(u) => u,
            _ => return Err(DefinitionLookupError::ColumnWrongType()),
        };

        let tbl_columns = self.get_table_columns(tran_id, table_id).await?;
        let mut tbl_attrs = vec![];
        for c in tbl_columns {
            let c_name = match c.get_column_not_null(pg_attribute::COLUMN_NAME)? {
                BaseSqlTypes::Text(t) => t,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };
            let c_type = match c.get_column_not_null(pg_attribute::COLUMN_SQL_TYPE)? {
                BaseSqlTypes::Text(t) => t,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };

            let c_null = match c.get_column_not_null(pg_attribute::COLUMN_NULLABLE)? {
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

        let indexes = self
            .get_table_indexes(tran_id, table_id, &tbl_attrs)
            .await?;
        let constraints = self
            .get_table_constraints(tran_id, table_id, &indexes)
            .await?;

        Ok(Arc::new(Table::new(
            table_id,
            name,
            tbl_attrs,
            constraints,
            indexes,
        )))
    }

    async fn get_table_row(
        &self,
        tran_id: TransactionId,
        name: String,
    ) -> Result<RowData, DefinitionLookupError> {
        let row_stream = self
            .vis_row_man
            .clone()
            .get_stream(tran_id, SystemTables::PgClass.value());
        pin!(row_stream);
        while let Some(row_res) = row_stream.next().await {
            let row = row_res?;
            if row.get_column_not_null(pg_class::COLUMN_NAME)? == BaseSqlTypes::Text(name.clone()) {
                return Ok(row);
            }
        }

        Err(DefinitionLookupError::TableDoesNotExist(name))
    }

    async fn get_table_columns(
        &self,
        tran_id: TransactionId,
        class_id: Uuid,
    ) -> Result<Vec<RowData>, DefinitionLookupError> {
        let mut columns = vec![];
        let pg_attr = SystemTables::PgAttribute.value();
        let row_stream = self
            .vis_row_man
            .clone()
            .get_stream(tran_id, pg_attr.clone());
        pin!(row_stream);
        while let Some(row_res) = row_stream.next().await {
            let row = row_res?;
            if row.get_column_not_null(pg_attribute::COLUMN_CLASS_ID)?
                == BaseSqlTypes::Uuid(class_id)
            {
                columns.push(row);
            }
        }

        if columns.is_empty() {
            return Err(DefinitionLookupError::NoColumnsFound());
        }

        //Figure out what column we're dealing with
        let col_offset = pg_attr.get_column_index(pg_attribute::COLUMN_COLUMN_NUM)?;

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

    async fn get_table_indexes(
        &self,
        tran_id: TransactionId,
        class_id: Uuid,
        attributes: &[Attribute],
    ) -> Result<Vec<Arc<Index>>, DefinitionLookupError> {
        let mut rows = vec![];
        let row_stream = self
            .vis_row_man
            .clone()
            .get_stream(tran_id, SystemTables::PgIndex.value());
        pin!(row_stream);
        while let Some(row_res) = row_stream.next().await {
            let row = row_res?;
            if row.get_column_not_null(pg_index::COLUMN_CLASS_ID)? == BaseSqlTypes::Uuid(class_id) {
                rows.push(row);
            }
        }

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let mut indexes = vec![];
        for r in rows.iter() {
            let id = match r.get_column_not_null(pg_index::COLUMN_ID)? {
                BaseSqlTypes::Uuid(u) => u,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };
            let name = match r.get_column_not_null(pg_index::COLUMN_NAME)? {
                BaseSqlTypes::Text(t) => t,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };
            let columns = match r.get_column_not_null(pg_index::COLUMN_ATTRIBUTES)? {
                BaseSqlTypes::Array(a) => {
                    let mut cols = vec![];
                    for col in a {
                        match col {
                            BaseSqlTypes::Integer(i) => {
                                let i_usize = usize::try_from(i)?;
                                cols.push(
                                    attributes
                                        .get(i_usize)
                                        .ok_or(DefinitionLookupError::WrongColumnIndex(i_usize))?
                                        .clone(),
                                );
                            }
                            _ => {
                                return Err(DefinitionLookupError::ColumnWrongType());
                            }
                        }
                    }
                    cols
                }
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };
            let unique = match r.get_column_not_null(pg_index::COLUMN_UNIQUE)? {
                BaseSqlTypes::Bool(b) => b,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };

            indexes.push(Arc::new(Index {
                id,
                name,
                columns: Arc::new(SqlTypeDefinition::new(&columns)),
                unique,
            }));
        }

        Ok(indexes)
    }

    async fn get_table_constraints(
        &self,
        tran_id: TransactionId,
        class_id: Uuid,
        indexes: &[Arc<Index>],
    ) -> Result<Vec<Constraint>, DefinitionLookupError> {
        let mut rows = vec![];
        let row_stream = self
            .vis_row_man
            .clone()
            .get_stream(tran_id, SystemTables::PgConstraint.value());
        pin!(row_stream);
        while let Some(row_res) = row_stream.next().await {
            let row = row_res?;
            if row.get_column_not_null(pg_constraint::COLUMN_CLASS_ID)?
                == BaseSqlTypes::Uuid(class_id)
            {
                rows.push(row);
            }
        }

        let mut constraints = vec![];
        'outer: for r in rows.iter() {
            let name = match r.get_column_not_null(pg_constraint::COLUMN_NAME)? {
                BaseSqlTypes::Text(t) => t,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };
            let index_id = match r.get_column_not_null(pg_constraint::COLUMN_INDEX_ID)? {
                BaseSqlTypes::Uuid(u) => u,
                _ => return Err(DefinitionLookupError::ColumnWrongType()),
            };

            for i in indexes {
                if i.id == index_id {
                    constraints.push(Constraint::PrimaryKey(PrimaryKeyConstraint {
                        name,
                        index: i.clone(),
                    }));
                    continue 'outer;
                }
            }
            return Err(DefinitionLookupError::IndexDoesNotExist(index_id));
        }
        Ok(constraints)
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
    #[error("Index {0} does not exist")]
    IndexDoesNotExist(Uuid),
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

    use crate::engine::io::block_layer::file_manager::FileManager;
    use crate::engine::io::block_layer::lock_cache_manager::LockCacheManager;

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
            Err(DefinitionLookupError::TableDoesNotExist(_)) => {}
            _ => panic!("Should not have gotten here!"),
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

        Ok(())
    }
}
