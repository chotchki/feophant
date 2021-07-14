use super::ParseExpression;

#[derive(Clone, Debug)]
pub enum ParseTree {
    CreateTable(RawCreateTableCommand),
    Insert(RawInsertCommand),
    Select(RawSelectCommand),
}

#[derive(Clone, Debug)]
pub struct RawCreateTableCommand {
    pub table_name: String,
    pub provided_columns: Vec<RawColumn>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RawColumn {
    pub name: String,
    pub sql_type: String,
    pub null: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RawInsertCommand {
    pub table_name: String,
    pub provided_columns: Option<Vec<String>>,
    pub provided_values: Vec<ParseExpression>,
}

//TODO This is VERY bare bones, will be radically changed once more is implemented
#[derive(Clone, Debug, PartialEq)]
pub struct RawSelectCommand {
    pub columns: Vec<String>,
    pub table: String,
}
