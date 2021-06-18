pub enum ParseTree {
    CreateTable(RawCreateTableCommand),
    Insert(RawInsertCommand),
}

#[derive(Clone)]
pub struct RawCreateTableCommand {
    pub table_name: String,
    pub provided_columns: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RawInsertCommand {
    pub table_name: String,
    pub provided_columns: Option<Vec<String>>,
    pub provided_values: Vec<String>,
}
