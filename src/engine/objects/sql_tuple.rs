//!Wrapper type for a row in the database unattached to a table

use crate::constants::BuiltinSqlTypes;

#[derive(Clone, Debug, PartialEq)]
pub struct SqlTuple(pub Vec<Option<BuiltinSqlTypes>>);
