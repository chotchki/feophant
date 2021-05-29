//!Postgres Doc: https://www.postgresql.org/docs/current/catalog-pg-attribute.html

use serde::{Serialize, Deserialize};
use uuid::Uuid;

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct Attribute {
    id: Uuid,
    name: String,
    parent: Uuid,
}

impl Attribute {
    pub fn new(id: Uuid, name: String, parent: Uuid) -> Attribute {
        Attribute{
            id: id,
            name: name,
            parent: parent
        }
    }
}