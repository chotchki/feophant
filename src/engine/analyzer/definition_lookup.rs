//! This command will look up ONLY hardcoded table definitions first,
//! should be able to fallback to reading new ones off disk

use super::super::super::constants::TableDefinitions;
use super::super::io::RowManager;
use super::super::objects::Table;
use super::super::transactions::TransactionId;
use std::sync::Arc;
use thiserror::Error;
use tokio::pin;
use tokio_stream::StreamExt;

#[derive(Clone, Debug)]
pub struct DefinitionLookup {
    row_manager: RowManager,
}

impl DefinitionLookup {
    pub fn new(row_manager: RowManager) -> DefinitionLookup {
        DefinitionLookup { row_manager }
    }

    pub async fn get_definition(
        &self,
        tran_id: TransactionId,
        name: String,
    ) -> Result<Arc<Table>, DefinitionLookupError> {
        //System Tables always load
        let system_tables = TableDefinitions::VALUES;
        for i in &system_tables {
            if i.value().name == name {
                return Ok(i.value());
            }
        }

        //Now we have to search
        let pg_class = TableDefinitions::PgClass.value();
        let row_stream = self.row_manager.clone().get_stream(tran_id, pg_class);
        pin!(row_stream);
        while let Some(row) = row_stream.next().await {
            //Have to debate now if I should push the visable rows down a level, I will have to implement
            //the sql visibility rules there if I do. I think that makes sense though
            println!("Got {:?}", row);
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
    use super::super::super::io::IOManager;
    use super::super::super::transactions::TransactionManager;
    use super::*;
    use tokio::sync::RwLock;

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    fn test_find_pg_class() {
        let pm = Arc::new(RwLock::new(IOManager::new()));
        let tm = TransactionManager::new();
        let rm = RowManager::new(pm, tm);
        let dl = DefinitionLookup::new(rm);

        let tran_id = TransactionId::new(1);

        let pg_class_def = aw!(dl.get_definition(tran_id, "pg_class".to_string())).unwrap();
        assert_eq!(pg_class_def.name, "pg_class".to_string());
    }

    #[test]
    fn test_no_such_class() {
        let pm = Arc::new(RwLock::new(IOManager::new()));
        let tm = TransactionManager::new();
        let rm = RowManager::new(pm, tm);
        let dl = DefinitionLookup::new(rm);

        let tran_id = TransactionId::new(1);

        let pg_class_def = aw!(dl.get_definition(tran_id, "something_random".to_string()));
        match pg_class_def {
            Ok(_) => assert!(false),
            Err(DefinitionLookupError::TableDoesNotExist(_)) => assert!(true),
        }
    }
}
