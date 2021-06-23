//! This command will look up ONLY hardcoded table definitions first,
//! should be able to fallback to reading new ones off disk
use super::super::super::constants::TableDefinitions;
use super::super::objects::Table;
use std::sync::Arc;
use thiserror::Error;

pub struct DefinitionLookup {}

impl DefinitionLookup {
    pub fn get_definition(name: String) -> Result<Arc<Table>, DefinitionLookupError> {
        let system_tables = TableDefinitions::VALUES;
        for i in &system_tables {
            if i.value().name == name {
                return Ok(i.value());
            }
        }

        Err(DefinitionLookupError::TableDoesNotExist(name))
    }
}

#[derive(Debug, Error)]
pub enum DefinitionLookupError {
    #[error("{0} is not a valid table")]
    TableDoesNotExist(String),
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_find_pg_class() {
        let pg_class_def = DefinitionLookup::get_definition("pg_class".to_string()).unwrap();
        assert_eq!(pg_class_def.name, "pg_class".to_string());
    }

    #[test]
    fn test_no_such_class() {
        let pg_class_def = DefinitionLookup::get_definition("something_random".to_string());
        match pg_class_def {
            Ok(_) => assert!(false),
            Err(DefinitionLookupError::TableDoesNotExist(_)) => assert!(true),
        }
    }
}
