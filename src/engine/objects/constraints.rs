use super::Index;
use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

mod parse_constraint;

#[derive(Clone, Debug, PartialEq)]
pub enum Constraint {
    PrimaryKey(PrimaryKeyConstraint),
}

/// ConstraintMapper exists to map to SqlType without imposing the cost of an empty version
///
/// This will exist until this RFC is brought back: https://github.com/rust-lang/rfcs/pull/2593
#[derive(Clone, Debug, PartialEq)]
pub enum ConstraintMapper {
    PrimaryKey,
}

impl Display for ConstraintMapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PrimaryKey => {
                write!(f, "PrimaryKey")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PrimaryKeyConstraint {
    pub name: String,
    pub index: Arc<Index>,
}
