use std::sync::Arc;

use super::{types::SqlTypeDefinition, Attribute, SqlTuple, Table};

pub struct PlannedStatement {
    pub common: PlannedCommon,
    pub plan: Arc<Plan>,
}

pub struct PlannedCommon {}

pub enum Plan {
    CartesianJoin(CartesianJoin),
    FullTableScan(FullTableScan),
    ModifyTable(ModifyTablePlan),
    StaticData(Arc<Vec<SqlTuple>>),
}

pub struct CartesianJoin {
    ///Output columns from this plan
    //pub columns: Vec<Attribute>,
    ///Columns defining the output of the left plan
    //pub left_cols: Vec<Attribute>,
    ///The left plan; Left is just arbitrary
    pub left: Arc<Plan>,
    ///Columns defining the output of the right plan
    //pub right_cols: Vec<Attribute>,
    ///The right plan; Right is just arbitrary
    pub right: Arc<Plan>,
}

pub struct FullTableScan {
    pub src_table: Arc<Table>,
    pub target_type: Arc<SqlTypeDefinition>,
}

pub struct ModifyTablePlan {
    pub table: Arc<Table>,
    pub source: Arc<Plan>,
}
