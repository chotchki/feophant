use serde::{Serialize, Deserialize};
use uuid::Uuid;

use super::PgAttribute;

#[derive(Serialize, Deserialize, Debug)]
pub struct PgTable {
    pub id: Uuid,
    pub name: String,
    pub attributes: Vec<PgAttribute>   
}

impl PgTable {
    pub fn new(id: Uuid, name: String, attributes: Vec<PgAttribute> ) -> PgTable {
        PgTable{
            id: id,
            name: name,
            attributes: attributes
        }
    }
}