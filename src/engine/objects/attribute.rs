//!Postgres Doc: https://www.postgresql.org/docs/current/catalog-pg-attribute.html

use uuid::Uuid;

use super::super::super::constants::DeserializeTypes;

#[derive(Clone, Debug)]
pub struct Attribute {
    id: Uuid,
    name: String,
    sql_type: DeserializeTypes,
}

impl Attribute {
    pub fn new(id: Uuid, name: String, sql_type: DeserializeTypes) -> Attribute {
        Attribute{
            id: id,
            name: name,
            sql_type: sql_type
        }
    }
}