use super::super::objects::{PgAttribute,PgTable};
use super::super::io::PageManager;
use super::super::super::constants::TableDefinitions;

pub struct FullTableScan {
    pm: PageManager
}

impl FullTableScan {
    //Just implementing string equality for comparison
    pub fn matching(table_def: PgTable, attribute: PgAttribute, value: String){

    }
}