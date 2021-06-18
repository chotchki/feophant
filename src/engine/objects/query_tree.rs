//! Is the result of the parse tree post validation
//! See here: https://www.postgresql.org/docs/current/querytree.html
use super::super::super::constants::BuiltinSqlTypes;
use std::rc::Rc;
use uuid::Uuid;

//This may end up being a hybrid struct/enum thing
pub struct QueryTree {
    //the command type
    command_type: CommandType,
    //the range tables
    range_tables: Vec<Rc<RangeRelation>>,
    //the result relation - may not be needed
    //the target list
    targets: Vec<TargetEntry>,
    //the qualification - Don't really understand this yet
    qualification: Vec<WhereEntry>,
    //the join tree
    joins: Vec<(JoinType, Rc<RangeRelation>, Rc<RangeRelation>)>,
    //the others
    sorts: Vec<(SortType, TargetEntry)>,
}

enum CommandType {
    Select,
    Insert,
    Update,
    Delete,
    Utility,
}

enum RangeRelation {
    Table(RangeRelationTable),
    View(RangeRelationTable),
    SubQuery(Option<QueryTree>),
}

struct RangeRelationTable {
    name: String,
    id: Uuid,
    alias: Option<String>,
}

//Every entry in the target list contains an expression that can be a
//constant value, a variable pointing to a column of one of the
//relations in the range table, a parameter, or an expression tree
//made of function calls, constants, variables, operators, etc.
enum TargetEntry {
    Relation(RangeRelation),
    Parameter(BuiltinSqlTypes),
}

enum WhereEntry {}

enum JoinType {
    Inner,
    OuterLeft,
    OuterRight,
    OuterFull,
}

enum SortType {
    Ascending,
    Descending,
}
