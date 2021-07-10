//! The planner takes a parsed query and makes it into a set of commands that can be sequentially executed.
use super::objects::{
    CommandType, JoinType, ModifyTablePlan, Plan, PlannedCommon, PlannedStatement, QueryTree,
    RangeRelation,
};
use std::sync::Arc;
use thiserror::Error;

pub struct Planner {}

impl Planner {
    pub fn plan(query_tree: QueryTree) -> Result<PlannedStatement, PlannerError> {
        match query_tree.command_type {
            CommandType::Insert => {
                return Planner::planInsert(query_tree);
            }
            _ => {
                return Err(PlannerError::NotImplemented());
            }
        }
    }

    fn planInsert(query_tree: QueryTree) -> Result<PlannedStatement, PlannerError> {
        //So we know we want to insert, now the question is into what.
        //I'm going to start with a simple insert and let it evolve.

        //So we should be able to find a join for our table target
        let join = query_tree
            .joins
            .first()
            .ok_or_else(|| PlannerError::TooManyJoins(query_tree.joins.len()))?;

        match (join.0, join.1.as_ref(), join.2.as_ref()) {
            (JoinType::Inner, RangeRelation::Table(t), RangeRelation::AnonymousTable(at)) => {
                return Ok(PlannedStatement {
                    common: PlannedCommon {},
                    plan: Plan::ModifyTable(ModifyTablePlan {
                        table: t.table.clone(),
                        source: Arc::new(Plan::StaticData(at.clone())),
                    }),
                });
            }
            (_, _, _) => return Err(PlannerError::NotImplemented()),
        }
    }
}

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("Too Many Joins {0}")]
    TooManyJoins(usize),
    #[error("Not Implemented")]
    NotImplemented(),
}
