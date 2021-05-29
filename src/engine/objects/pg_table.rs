use serde::{Serialize, Deserialize};
use uuid::Uuid;

use super::PgAttribute;

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct PgTable {
    pub id: Uuid,
    pub name: String,
    pub attributes: Vec<PgAttribute>   
}

impl PgTable {
    pub fn new_existing(id: Uuid, name: String, attributes: Vec<PgAttribute> ) -> PgTable {
        PgTable{
            id: id,
            name: name,
            attributes: attributes
        }
    }

    pub fn new(name: String, attributes: Vec<PgAttribute>) -> PgTable {
        PgTable::new_existing(Uuid::new_v4(), name, attributes)
    }
}