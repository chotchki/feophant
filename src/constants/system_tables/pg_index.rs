use crate::constants::Nullable;
use crate::engine::objects::{
    types::{BaseSqlTypesMapper, SqlTypeDefinition},
    Attribute, Constraint, Index, PrimaryKeyConstraint, Table,
};
use hex_literal::hex;
use std::sync::Arc;
use uuid::Uuid;

pub const id: Uuid = Uuid::from_bytes(hex!("3AB3B076A0EA46E186130F088D06FA02"));
pub const name: &str = "pg_index";

pub const column_id: &str = "id";
pub const column_class_id: &str = "class_id";
pub const column_name: &str = "name";
pub const column_attributes: &str = "attributes";
pub const column_unique: &str = "unique";

pub fn get_columns() -> Vec<Attribute> {
    vec![
        Attribute::new(
            column_id.to_string(),
            BaseSqlTypesMapper::Uuid,
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            column_class_id.to_string(),
            BaseSqlTypesMapper::Uuid,
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            column_name.to_string(),
            BaseSqlTypesMapper::Text,
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            column_attributes.to_string(),
            BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Integer)),
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            column_unique.to_string(),
            BaseSqlTypesMapper::Bool,
            Nullable::NotNull,
            None,
        ),
    ]
}

pub fn get_index(attrs: &Vec<Attribute>) -> Arc<Index> {
    Arc::new(Index {
        id: Uuid::from_bytes(hex!("5F59466782874C568F1C0C09E99C9249")),
        name: name.to_string() + "_name_index",
        columns: Arc::new(SqlTypeDefinition::new(&[attrs[2].clone()])),
        unique: true,
    })
}

pub fn get_table() -> Arc<Table> {
    let columns = get_columns();
    let index = get_index(&columns);
    Arc::new(Table::new(
        id,
        name.to_string(),
        columns,
        vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
            name: name.to_string() + "_primary_key",
            index: index.clone(),
        })],
        vec![index],
    ))
}
