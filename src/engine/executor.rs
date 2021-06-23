use super::super::constants::{BuiltinSqlTypes, TableDefinitions};
use super::io::{RowManager, RowManagerError};
use super::objects::{ParseTree, PlannedStatement};
use super::transactions::TransactionId;
use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

mod definition_lookup;
pub use definition_lookup::DefinitionLookup;

mod full_table_scan;
pub use full_table_scan::FullTableScan;

#[derive(Clone, Debug)]
pub struct Executor {
    row_manager: RowManager,
}

impl Executor {
    pub fn new(row_manager: RowManager) -> Executor {
        Executor { row_manager }
    }

    //Return type is unknown at the moment
    pub fn execute(plan_tree: PlannedStatement) -> Result<(), ExecutorError> {
        Err(ExecutorError::Unknown())
    }

    //Bypass planning since there isn't anything optimize
    pub async fn execute_utility(
        &self,
        tran_id: TransactionId,
        parse_tree: Arc<ParseTree>,
    ) -> Result<(), ExecutorError> {
        let rm = self.row_manager.clone();

        let create_table = match parse_tree.deref() {
            ParseTree::CreateTable(t) => t,
            _ => return Err(ExecutorError::NotUtility()),
        };

        let table_id = Uuid::new_v4();
        let pg_class = TableDefinitions::PgClass.value();
        let table_row = vec![
            Some(BuiltinSqlTypes::Uuid(table_id)),
            Some(BuiltinSqlTypes::Text(create_table.table_name.clone())),
        ];

        rm.insert_row(tran_id, pg_class, table_row).await?;

        let pg_attribute = TableDefinitions::PgAttribute.value();
        for i in 0..create_table.provided_columns.len() {
            let rm = self.row_manager.clone();
            let i_u32 = u32::try_from(i).map_err(ExecutorError::ConversionError)?;
            let table_row = vec![
                Some(BuiltinSqlTypes::Uuid(table_id)),
                Some(BuiltinSqlTypes::Text(
                    create_table.provided_columns[i].0.clone(),
                )),
                Some(BuiltinSqlTypes::Text(
                    //TODO we did not validate that it is a real type
                    create_table.provided_columns[i].1.clone(),
                )),
                Some(BuiltinSqlTypes::Integer(i_u32)),
            ];
            rm.clone()
                .insert_row(tran_id, pg_attribute.clone(), table_row)
                .await?;
        }

        //Making a table right now requires
        //  the insertion of a row in pg_class
        //      Need the definition of pg_class
        //  the insertion of a row per column in pg_attributes
        //      Need the definition of pg_attribute

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Not a utility statement")]
    NotUtility(),
    #[error(transparent)]
    RowManagerError(#[from] RowManagerError),
    #[error("Unable to convert usize to u32")]
    ConversionError(#[from] TryFromIntError),
    #[error("Unknown")]
    Unknown(),
}
