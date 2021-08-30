use uuid::Uuid;

use super::types::SqlTypeDefinition;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct Index {
    pub id: Uuid,
    pub name: String,
    pub columns: Arc<SqlTypeDefinition>,
    pub unique: bool,
}
