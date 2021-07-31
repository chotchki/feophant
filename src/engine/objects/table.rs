//! Postgres doc: https://www.postgresql.org/docs/current/catalog-pg-class.html

use std::sync::Arc;

use super::{types::SqlTypeDefinition, Attribute};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    pub id: Uuid,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub sql_type: Arc<SqlTypeDefinition>,
}

impl Table {
    pub fn new(id: Uuid, name: String, attributes: Vec<Attribute>) -> Table {
        let sql_type = SqlTypeDefinition(
            attributes
                .iter()
                .map(|a| (a.name.clone(), a.sql_type.clone()))
                .collect(),
        );

        Table {
            id,
            name,
            attributes,
            sql_type: Arc::new(sql_type),
        }
    }

    //TODO might not be need any more with the type
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
