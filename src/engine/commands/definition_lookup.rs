//! This command will look up ONLY hardcoded table definitions first, 
//! should be able to fallback to reading new ones off disk
use thiserror::Error;

use super::super::objects::PgTable;

pub struct DefinitionLookup {}

impl DefinitionLookup {
    pub fn getDefinition(name: String) -> Result<PgTable, DefinitionLookupError> {
        Err(DefinitionLookupError::TableNotFound())
    }
}

#[derive(Error, Debug)]
pub enum DefinitionLookupError {
    #[error("No table found")]
    TableNotFound(),
}