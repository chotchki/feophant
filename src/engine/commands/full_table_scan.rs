use super::super::objects::{Attribute,Table};
use super::super::io::PageManager;
use super::super::super::constants::TableDefinitions;

pub struct FullTableScan {
    pm: PageManager
}

impl FullTableScan {
    //Just implementing string equality for comparison
    pub fn matching(table_def: Table, attribute: Attribute, value: String){

    }
}