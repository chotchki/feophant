//! The planner takes a parsed query and makes it into a set of commands that can be sequentially executed.
use super::objects::{CommandType, ModifyTablePlan, PlannedCommon, PlannedStatement, QueryTree};
use thiserror::Error;

pub struct Planner {}

impl Planner {
    pub fn plan(query_tree: QueryTree) -> Result<PlannedStatement, PlannerError> {
        match query_tree.command_type {
            CommandType::Insert => {
                // return PlannedStatement{
                //     common: PlannedCommon{},
                //     plan: Plan::ModifyTable(
                //         ModifyTablePlan{
                //             table: query_tree.range_tables[0],
                //             source: query_tree.
                //         }
                //     )
                // }
                return Err(PlannerError::Unknown());
            }
            _ => {
                return Err(PlannerError::Unknown());
            }
        }
        Err(PlannerError::Unknown())
    }
}

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("Unknown")]
    Unknown(),
}
