use serde::{Serialize, Deserialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct PgAttribute {
    id: Uuid,
    name: String,
    parent: Uuid,
}

impl PgAttribute {
    pub fn new(id: Uuid, name: String, parent: Uuid) -> PgAttribute {
        PgAttribute{
            id: id,
            name: name,
            parent: parent
        }
    }
}