#[derive(Clone, Debug)]
pub enum ParseTree {
    CreateTable(RawCreateTableCommand),
    Insert(RawInsertCommand),
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
    pub provided_values: Vec<String>,
}
