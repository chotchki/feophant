use crate::engine::objects::SqlTuple;

use super::super::constants::{BuiltinSqlTypes, TableDefinitions};
use super::io::{VisibleRowManager, VisibleRowManagerError};
use super::objects::{Attribute, ParseTree, Plan, PlannedStatement, SqlTupleError, Table};
use super::transactions::TransactionId;
use async_stream::try_stream;
use futures::stream::Stream;
use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

mod full_table_scan;
pub use full_table_scan::FullTableScan;

//TODO way too many clones / Arc flipping. Unsure if I could make use of references better

#[derive(Clone, Debug)]
pub struct Executor {
    vis_row_man: VisibleRowManager,
}

impl Executor {
    pub fn new(vis_row_man: VisibleRowManager) -> Executor {
        Executor { vis_row_man }
    }

    pub fn execute(
        self,
        tran_id: TransactionId,
        plan_tree: PlannedStatement,
    ) -> Pin<Box<dyn Stream<Item = Result<SqlTuple, ExecutorError>> + Send>> {
        self.execute_plans(tran_id, plan_tree.plan)
    }

    fn execute_plans(
        self,
        tran_id: TransactionId,
        plan: Arc<Plan>,
    ) -> Pin<Box<dyn Stream<Item = Result<SqlTuple, ExecutorError>> + Send>> {
        match plan.as_ref() {
            Plan::CartesianJoin(cp) => {
                self.cartesian_join(tran_id, cp.left.clone(), cp.right.clone())
            }
            Plan::FullTableScan(fts) => {
                self.full_table_scan(tran_id, fts.table.clone(), fts.columns.clone())
            }
            Plan::ModifyTable(mt) => {
                self.modify_table(tran_id, mt.table.clone(), mt.source.clone())
            }
            Plan::StaticData(sd) => self.static_data(sd.clone()),
        }
    }

    fn cartesian_join(
        self,
        tran_id: TransactionId,
        left: Arc<Plan>,
        right: Arc<Plan>,
    ) -> Pin<Box<impl Stream<Item = Result<SqlTuple, ExecutorError>>>> {
        let s = try_stream! {
            for await left_data in self.clone().execute_plans(tran_id, left) {
                let left_data = left_data?;

                for await right_data in self.clone().execute_plans(tran_id, right.clone()) {
                    let right_data = right_data?;

                    yield SqlTuple::merge(&left_data, &right_data);
                }
            }
        };
        Box::pin(s)
    }

    fn full_table_scan(
        self,
        tran_id: TransactionId,
        table: Arc<Table>,
        columns: Vec<Attribute>,
    ) -> Pin<Box<impl Stream<Item = Result<SqlTuple, ExecutorError>>>> {
        let s = try_stream! {
            let vis = self.vis_row_man.clone();

            for await row in vis.get_stream(tran_id, table.clone()) {
                let data = row?.user_data.clone();

                //Need to rewrite to the column / order needed
                let requested_row = data.filter_map(&table, &columns)?;

                yield requested_row;
            }
        };
        Box::pin(s)
    }

    fn modify_table(
        self,
        tran_id: TransactionId,
        table: Arc<Table>,
        source: Arc<Plan>,
    ) -> Pin<Box<impl Stream<Item = Result<SqlTuple, ExecutorError>>>> {
        let vis = self.vis_row_man.clone();

        let s = try_stream! {
            for await val in self.execute_plans(tran_id, source) {
                let unwrapped_val = val?;
                vis.clone()
                    .insert_row(tran_id, table.clone(), Arc::new(unwrapped_val.clone()))
                    .await?;
                yield unwrapped_val;
            }
        };
        Box::pin(s)
    }

    fn static_data(
        self,
        rows: Arc<Vec<SqlTuple>>,
    ) -> Pin<Box<impl Stream<Item = Result<SqlTuple, ExecutorError>>>> {
        let s = try_stream! {
            for row in rows.as_ref().into_iter() {
                yield row.clone();
            }
        };
        Box::pin(s)
    }

    //Bypass planning since there isn't anything optimize
    pub async fn execute_utility(
        &self,
        tran_id: TransactionId,
        parse_tree: ParseTree,
    ) -> Result<Vec<SqlTuple>, ExecutorError> {
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
        Ok(vec![])
    }
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Not a utility statement")]
    NotUtility(),
    #[error(transparent)]
    SqlTupleError(#[from] SqlTupleError),
    #[error(transparent)]
    VisibleRowManagerError(#[from] VisibleRowManagerError),
    #[error("Unable to convert usize to u32")]
    ConversionError(#[from] TryFromIntError),
    #[error("Recursive Plans Not Allowed")]
    RecursionNotAllowed(),
    #[error("Unknown")]
    Unknown(),
}
