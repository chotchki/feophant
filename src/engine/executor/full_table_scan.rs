use super::super::io::IOManager;
use super::super::objects::{Attribute, Table};

pub struct FullTableScan {
    pm: IOManager,
}

impl FullTableScan {
    //Just implementing string equality for comparison
    pub fn matching(table_def: Table, attribute: Attribute, value: String) {}
}
