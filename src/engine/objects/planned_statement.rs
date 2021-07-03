use crate::constants::BuiltinSqlTypes;
use std::sync::Arc;

use super::Table;

pub struct PlannedStatement {
    pub common: PlannedCommon,
    pub plan: Plan,
}

pub struct PlannedCommon {}

pub enum Plan {
    ModifyTable(ModifyTablePlan),
    StaticData(Vec<Option<BuiltinSqlTypes>>),
}

pub struct ModifyTablePlan {
    pub table: Arc<Table>,
    pub source: Arc<Plan>,
}
