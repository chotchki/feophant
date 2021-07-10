use crate::engine::objects::SqlTuple;

use super::super::constants::{BuiltinSqlTypes, TableDefinitions};
use super::io::{VisibleRowManager, VisibleRowManagerError};
use super::objects::{ParseTree, Plan, PlannedStatement, Table};
use super::transactions::TransactionId;
use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

mod full_table_scan;
pub use full_table_scan::FullTableScan;

#[derive(Clone, Debug)]
pub struct Executor {
    vis_row_man: VisibleRowManager,
}

impl Executor {
    pub fn new(vis_row_man: VisibleRowManager) -> Executor {
        Executor { vis_row_man }
    }

    //Return type is unknown at the moment
    pub async fn execute(
        &self,
        tran_id: TransactionId,
        plan_tree: PlannedStatement,
    ) -> Result<(), ExecutorError> {
        match plan_tree.plan {
            Plan::ModifyTable(mt) => self.modify_table(tran_id, mt.table, mt.source).await,
            _ => Err(ExecutorError::Unknown()),
        }
    }

    async fn modify_table(
        &self,
        tran_id: TransactionId,
        table: Arc<Table>,
        source: Arc<Plan>,
    ) -> Result<(), ExecutorError> {
        let rm = self.vis_row_man.clone();

        let values = match source.as_ref() {
            Plan::StaticData(sd) => sd.clone(),
            _ => return Err(ExecutorError::RecursionNotAllowed()),
        };

        rm.insert_row(tran_id, table, values).await?;

        Ok(())
    }

    //Bypass planning since there isn't anything optimize
    pub async fn execute_utility(
        &self,
        tran_id: TransactionId,
        parse_tree: ParseTree,
    ) -> Result<(), ExecutorError> {
        let rm = self.vis_row_man.clone();

        let create_table = match parse_tree {
            ParseTree::CreateTable(t) => t,
            _ => return Err(ExecutorError::NotUtility()),
        };

        let table_id = Uuid::new_v4();
        let pg_class = TableDefinitions::PgClass.value();
        let table_row = Arc::new(SqlTuple(vec![
            Some(BuiltinSqlTypes::Uuid(table_id)),
            Some(BuiltinSqlTypes::Text(create_table.table_name.clone())),
        ]));

        rm.insert_row(tran_id, pg_class, table_row).await?;

        let pg_attribute = TableDefinitions::PgAttribute.value();
        for i in 0..create_table.provided_columns.len() {
            let rm = self.vis_row_man.clone();
            let i_u32 = u32::try_from(i).map_err(ExecutorError::ConversionError)?;
            let table_row = Arc::new(SqlTuple(vec![
                Some(BuiltinSqlTypes::Uuid(table_id)),
                Some(BuiltinSqlTypes::Text(
                    create_table.provided_columns[i].name.clone(),
                )),
                Some(BuiltinSqlTypes::Text(
                    //TODO we did not validate that it is a real type
                    create_table.provided_columns[i].sql_type.clone(),
                )),
                Some(BuiltinSqlTypes::Integer(i_u32)),
                Some(BuiltinSqlTypes::Bool(create_table.provided_columns[i].null)),
            ]));
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
    VisibleRowManagerError(#[from] VisibleRowManagerError),
    #[error("Unable to convert usize to u32")]
    ConversionError(#[from] TryFromIntError),
    #[error("Recursive Plans Not Allowed")]
    RecursionNotAllowed(),
    #[error("Unknown")]
    Unknown(),
}
