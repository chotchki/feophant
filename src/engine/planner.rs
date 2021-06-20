//! The planner takes a parsed query and makes it into a set of commands that can be sequentially executed.
use super::objects::{PlannedStatement, QueryTree};
use thiserror::Error;

pub struct Planner {}

impl Planner {
    pub fn plan(query_tree: QueryTree) -> Result<PlannedStatement, PlannerError> {
        Err(PlannerError::Unknown())
    }
}

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("Unknown")]
    Unknown(),
}
