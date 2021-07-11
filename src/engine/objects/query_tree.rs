//! Is the result of the parse tree post validation
//! See here: https://www.postgresql.org/docs/current/querytree.html
use super::Attribute;
use super::SqlTuple;
use super::Table;
use std::sync::Arc;

//Note the comments below are based on my current understanding of how Postgres works,
//I'm sure these comments will age poorly
//
//Its important to note that postgres heavily uses references in these structs I'm not sure
//if that makes sense in Rust. My focus is understanding the data relationships.
#[derive(Clone, Debug)]
pub struct QueryTree {
    //the command type
    pub command_type: CommandType,

    //the target list of columns to be affected
    pub targets: Vec<TargetEntry>,

    //These are tables being used as inputs for the query.
    //They could be a table, view, static data, or even a sub query.
    //How to represent some of this is TBD
    pub range_tables: Vec<RangeRelation>,

    //the qualification - Don't really understand this yet
    //pub qualification: Vec<WhereEntry>,

    //the join tree is to relate entries in the range tables to each other
    pub joins: Vec<(JoinType, RangeRelation, RangeRelation)>,
    //the others
    //pub sorts: Vec<(SortType, TargetEntry)>,
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
    //View(RangeRelationTable),
    //SubQuery(Option<QueryTree>),
    AnonymousTable(Arc<SqlTuple>), //Used for inserts
}

#[derive(Clone, Debug)]
pub struct RangeRelationTable {
    pub table: Arc<Table>,
    pub alias: Option<String>,
}

//Every entry in the target list contains an expression that can be a
//constant value, a variable pointing to a column of one of the
//relations in the range table, a parameter, or an expression tree
//made of function calls, constants, variables, operators, etc.
#[derive(Clone, Debug)]
pub enum TargetEntry {
    Parameter(Attribute),
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
