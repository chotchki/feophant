//! Postgres doc: https://www.postgresql.org/docs/current/catalog-pg-class.html

use super::Attribute;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    pub id: Uuid,
    pub name: String,
    pub attributes: Vec<Attribute>,
}

impl Table {
    pub fn new_existing(id: Uuid, name: String, attributes: Vec<Attribute>) -> Table {
        Table {
            id,
            name,
            attributes,
        }
    }

    //TODO this new isn't really useable since the child attributes also need the link to the table
    //Also writing unit tests are VERY painful, might need to disconnect in memory from on disk storage
    pub fn new(name: String, attributes: Vec<Attribute>) -> Table {
        Table::new_existing(Uuid::new_v4(), name, attributes)
    }

    pub fn get_column_index(&self, name: String) -> Result<usize, TableError> {
        for i in 0..self.attributes.len() {
            if self.attributes[i].name == name {
                return Ok(i);
            }
        }

        Err(TableError::ColumnDoesNotExist(name))
    }
}

#[derive(Error, Debug)]
pub enum TableError {
    #[error("Column named {0} does not exist")]
    ColumnDoesNotExist(String),
}
