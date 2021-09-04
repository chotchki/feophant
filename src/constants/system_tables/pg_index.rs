use crate::constants::Nullable;
use crate::engine::objects::{
    types::{BaseSqlTypesMapper, SqlTypeDefinition},
    Attribute, Constraint, Index, PrimaryKeyConstraint, Table,
};
use hex_literal::hex;
use std::sync::Arc;
use uuid::Uuid;

pub const ID: Uuid = Uuid::from_bytes(hex!("3AB3B076A0EA46E186130F088D06FA02"));
pub const NAME: &str = "pg_index";

pub const COLUMN_ID: &str = "id";
pub const COLUMN_CLASS_ID: &str = "class_id";
pub const COLUMN_NAME: &str = "name";
pub const COLUMN_ATTRIBUTES: &str = "attributes";
pub const COLUMN_UNIQUE: &str = "unique";

pub fn get_columns() -> Vec<Attribute> {
    vec![
        Attribute::new(
            COLUMN_ID.to_string(),
            BaseSqlTypesMapper::Uuid,
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            COLUMN_CLASS_ID.to_string(),
            BaseSqlTypesMapper::Uuid,
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            COLUMN_NAME.to_string(),
            BaseSqlTypesMapper::Text,
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            COLUMN_ATTRIBUTES.to_string(),
            BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Integer)),
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            COLUMN_UNIQUE.to_string(),
            BaseSqlTypesMapper::Bool,
            Nullable::NotNull,
            None,
        ),
    ]
}

pub fn get_index(attrs: &[Attribute]) -> Arc<Index> {
    Arc::new(Index {
        id: Uuid::from_bytes(hex!("5F59466782874C568F1C0C09E99C9249")),
        name: NAME.to_string() + "_name_index",
        columns: Arc::new(SqlTypeDefinition::new(&[attrs[2].clone()])),
        unique: true,
    })
}

pub fn get_table() -> Arc<Table> {
    let columns = get_columns();
    let index = get_index(&columns);
    Arc::new(Table::new(
        ID,
        NAME.to_string(),
        columns,
        vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
            name: NAME.to_string() + "_primary_key",
            index: index.clone(),
        })],
        vec![index],
    ))
}
