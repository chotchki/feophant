//! Postgres doc: https://www.postgresql.org/docs/current/catalog-pg-class.html

use std::sync::Arc;

use super::{types::SqlTypeDefinition, Attribute, Constraint, Index};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    pub id: Uuid,
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Arc<Index>>,
    pub sql_type: Arc<SqlTypeDefinition>,
}

impl Table {
    pub fn new(
        id: Uuid,
        name: String,
        attributes: Vec<Attribute>,
        constraints: Vec<Constraint>,
        indexes: Vec<Arc<Index>>,
    ) -> Table {
        let sql_type = Arc::new(SqlTypeDefinition::new(&attributes));
        Table {
            id,
            name,
            attributes,
            constraints,
            indexes,
            sql_type,
        }
    }

    //TODO might not be need any more with the type
    pub fn get_column_index(&self, name: &str) -> Result<usize, TableError> {
        for i in 0..self.attributes.len() {
            if self.attributes[i].name == name {
                return Ok(i);
            }
        }

        Err(TableError::ColumnDoesNotExist(name.to_string()))
    }
}

#[derive(Error, Debug)]
pub enum TableError {
    #[error("Column named {0} does not exist")]
    ColumnDoesNotExist(String),
}
