use uuid::Uuid;

use super::{Attribute, Table};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Index {
    pub id: Uuid,
    pub pg_class_id: Uuid,
    pub name: String,
    pub table: Arc<Table>,
    pub columns: Vec<Attribute>,
    pub unique: bool,
}
