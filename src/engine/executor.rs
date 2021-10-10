use crate::constants::SystemTables;
use crate::engine::objects::types::BaseSqlTypes;
use crate::engine::objects::{ConstraintMapper, SqlTuple};

use super::io::{ConstraintManager, ConstraintManagerError};
use super::objects::types::SqlTypeDefinition;
use super::objects::{ParseTree, Plan, PlannedStatement, SqlTupleError, Table};
use super::transactions::TransactionId;
use async_stream::try_stream;
use futures::stream::Stream;
use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

//TODO way too many clones / Arc flipping. Unsure if I could make use of references better

#[derive(Clone)]
pub struct Executor {
    cons_man: ConstraintManager,
}

impl Executor {
    pub fn new(cons_man: ConstraintManager) -> Executor {
        Executor { cons_man }
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
                self.full_table_scan(tran_id, fts.src_table.clone(), fts.target_type.clone())
            }
            Plan::ModifyTable(mt) => self.modify_table(tran_id, &mt.table, mt.source.clone()),
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
        src_table: Arc<Table>,
        target_type: Arc<SqlTypeDefinition>,
    ) -> Pin<Box<impl Stream<Item = Result<SqlTuple, ExecutorError>>>> {
        let s = try_stream! {
            let vis = self.cons_man;

            for await row in vis.get_stream(tran_id, src_table.clone()) {
                let data = row?.user_data.clone();

                //Need to rewrite to the column / order needed
                let requested_row = data.filter_map(&src_table.sql_type, &target_type)?;

                yield requested_row;
            }
        };
        Box::pin(s)
    }

    fn modify_table(
        self,
        tran_id: TransactionId,
        table: &Arc<Table>,
        source: Arc<Plan>,
    ) -> Pin<Box<impl Stream<Item = Result<SqlTuple, ExecutorError>>>> {
        let vis = self.clone().cons_man;
        let table = table.clone();

        let s = try_stream! {
            for await val in self.clone().execute_plans(tran_id, source) {
                let unwrapped_val = val?;
                vis.clone()
                    .insert_row(tran_id, &table, unwrapped_val.clone())
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
            for row in rows.as_ref().iter() {
                yield row.clone();
            }
        };
        Box::pin(s)
    }

    //Bypass planning since there isn't anything optimize
    pub async fn execute_utility(
        &mut self,
        tran_id: TransactionId,
        parse_tree: ParseTree,
    ) -> Result<Vec<SqlTuple>, ExecutorError> {
        let mut cm = self.cons_man.clone();

        let create_table = match parse_tree {
            ParseTree::CreateTable(t) => t,
            _ => return Err(ExecutorError::NotUtility()),
        };

        let table_id = Uuid::new_v4();
        let pg_class = SystemTables::PgClass.value();
        let table_row = SqlTuple(vec![
            Some(BaseSqlTypes::Uuid(table_id)),
            Some(BaseSqlTypes::Text(create_table.table_name.clone())),
        ]);

        cm.insert_row(tran_id, &pg_class, table_row).await?;

        let mut primary_key_cols = vec![];

        let pg_attribute = SystemTables::PgAttribute.value();
        for i in 0..create_table.provided_columns.len() {
            let cm = self.cons_man.clone();
            let i_u32 = u32::try_from(i).map_err(ExecutorError::ConversionError)?;
            let table_row = SqlTuple(vec![
                Some(BaseSqlTypes::Uuid(table_id)),
                Some(BaseSqlTypes::Text(
                    create_table.provided_columns[i].name.clone(),
                )),
                Some(BaseSqlTypes::Text(
                    //TODO we did not validate that it is a real type
                    create_table.provided_columns[i].sql_type.clone(),
                )),
                Some(BaseSqlTypes::Integer(i_u32)),
                Some(BaseSqlTypes::Bool(create_table.provided_columns[i].null)),
            ]);
            cm.clone()
                .insert_row(tran_id, &pg_attribute, table_row)
                .await?;

            if create_table.provided_columns[i].primary_key {
                primary_key_cols.push(BaseSqlTypes::Integer(i_u32));
            }
        }

        if !primary_key_cols.is_empty() {
            //We assume the the order that columns with primary key were defined are the order desired
            let pk_id = Uuid::new_v4();
            let primary_key_index = SqlTuple(vec![
                Some(BaseSqlTypes::Uuid(pk_id)),
                Some(BaseSqlTypes::Uuid(table_id)),
                Some(BaseSqlTypes::Text(format!(
                    "{}_primary_key_index",
                    create_table.table_name
                ))),
                Some(BaseSqlTypes::Array(primary_key_cols)),
                Some(BaseSqlTypes::Bool(true)),
            ]);
            let pg_index = SystemTables::PgIndex.value();
            self.cons_man
                .clone()
                .insert_row(tran_id, &pg_index, primary_key_index)
                .await?;

            //Now we can insert the constraint
            let primary_key_constraint = SqlTuple(vec![
                Some(BaseSqlTypes::Uuid(Uuid::new_v4())),
                Some(BaseSqlTypes::Uuid(table_id)),
                Some(BaseSqlTypes::Uuid(pk_id)),
                Some(BaseSqlTypes::Text(format!(
                    "{}_primary_key",
                    create_table.table_name
                ))),
                Some(BaseSqlTypes::Text(ConstraintMapper::PrimaryKey.to_string())),
            ]);
            let pg_constraint = SystemTables::PgConstraint.value();
            self.cons_man
                .clone()
                .insert_row(tran_id, &pg_constraint, primary_key_constraint)
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
    ConstraintManagerError(#[from] ConstraintManagerError),
    #[error("Unable to convert usize to u32")]
    ConversionError(#[from] TryFromIntError),
    #[error("Recursive Plans Not Allowed")]
    RecursionNotAllowed(),
    #[error("Unknown")]
    Unknown(),
}
