use uuid::Uuid;
use super::PgAttribute;

pub struct PgTable {
    id: Uuid,
    name: String,
    attributes: Vec<PgAttribute>   
}