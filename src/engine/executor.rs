use super::objects::PlannedStatement;
use thiserror::Error;

mod definition_lookup;
pub use definition_lookup::DefinitionLookup;

mod full_table_scan;
pub use full_table_scan::FullTableScan;

pub struct Executor {}

impl Executor {
    //Return type is unknown at the moment
    pub fn execute(plan_tree: PlannedStatement) {}
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Unknown")]
    Unknown(),
}
