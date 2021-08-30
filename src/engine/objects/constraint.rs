use super::Index;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum Constraint {
    PrimaryKey(PrimaryKeyConstraint),
}

#[derive(Clone, Debug, PartialEq)]
pub struct PrimaryKeyConstraint {
    pub name: String,
    pub index: Arc<Index>,
}
