//!Postgres Doc: https://www.postgresql.org/docs/current/catalog-pg-attribute.html

use super::super::super::constants::DeserializeTypes;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Attribute {
    id: Uuid,
    name: String,
    sql_type: DeserializeTypes,
}

impl Attribute {
    pub fn new(id: Uuid, name: String, sql_type: DeserializeTypes) -> Attribute {
        Attribute { id, name, sql_type }
    }
}
