//! Postgres doc: https://www.postgresql.org/docs/current/catalog-pg-class.html

use uuid::Uuid;

use super::Attribute;

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

    pub fn new(name: String, attributes: Vec<Attribute>) -> Table {
        Table::new_existing(Uuid::new_v4(), name, attributes)
    }
}
