use super::super::super::constants::TableDefinitions;
use super::super::io::PageManager;
use super::super::objects::{Attribute, Table};

pub struct FullTableScan {
    pm: PageManager,
}

impl FullTableScan {
    //Just implementing string equality for comparison
    pub fn matching(table_def: Table, attribute: Attribute, value: String) {}
}
