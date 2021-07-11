use std::sync::Arc;

use super::{SqlTuple, Table};

pub struct PlannedStatement {
    pub common: PlannedCommon,
    pub plan: Plan,
}

pub struct PlannedCommon {}

pub enum Plan {
    ModifyTable(ModifyTablePlan),
    StaticData(Arc<SqlTuple>),
}

pub struct ModifyTablePlan {
    pub table: Arc<Table>,
    pub source: Arc<Plan>,
}
