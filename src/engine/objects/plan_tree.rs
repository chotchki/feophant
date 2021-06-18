pub struct PlannedStatement {
    pub common: PlannedCommon,
    pub plan: Plan,
}

pub struct PlannedCommon {}

enum Plan {
    ModifyTable(),
}
