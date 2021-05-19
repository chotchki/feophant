use uuid::Uuid;

pub struct PgAttribute {
    id: Uuid,
    parent: Uuid,
}