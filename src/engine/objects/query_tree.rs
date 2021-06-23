//! Is the result of the parse tree post validation
//! See here: https://www.postgresql.org/docs/current/querytree.html
use super::super::super::constants::BuiltinSqlTypes;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct QueryTree {
    //the command type
    pub command_type: CommandType,
    //the range tables - Code smell don't like the use of options
    pub range_tables: Vec<Arc<RangeRelation>>,
    //the result relation - may not be needed
    //the target list
    pub targets: Vec<TargetEntry>,
    //the qualification - Don't really understand this yet
    pub qualification: Vec<WhereEntry>,
    //the join tree
    pub joins: Vec<(JoinType, Arc<RangeRelation>, Arc<RangeRelation>)>,
    //the others
    pub sorts: Vec<(SortType, TargetEntry)>,
}

#[derive(Clone, Copy, Debug)]
pub enum CommandType {
    Select,
    Insert,
    Update,
    Delete,
    Utility,
}

#[derive(Clone, Debug)]
pub enum RangeRelation {
    Table(RangeRelationTable),
    View(RangeRelationTable),
    SubQuery(Option<QueryTree>),
}

#[derive(Clone, Debug)]
pub struct RangeRelationTable {
    name: String,
    id: Uuid,
    alias: Option<String>,
}

//Every entry in the target list contains an expression that can be a
//constant value, a variable pointing to a column of one of the
//relations in the range table, a parameter, or an expression tree
//made of function calls, constants, variables, operators, etc.
#[derive(Clone, Debug)]
pub enum TargetEntry {
    Parameter(BuiltinSqlTypes),
}

#[derive(Clone, Debug)]
pub enum WhereEntry {}

#[derive(Clone, Copy, Debug)]
pub enum JoinType {
    Inner,
    OuterLeft,
    OuterRight,
    OuterFull,
}

#[derive(Clone, Copy, Debug)]
pub enum SortType {
    Ascending,
    Descending,
}
