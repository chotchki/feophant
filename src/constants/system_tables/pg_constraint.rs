use crate::constants::Nullable;
use crate::engine::objects::{
    types::{BaseSqlTypesMapper, SqlTypeDefinition},
    Attribute, Constraint, Index, PrimaryKeyConstraint, Table,
};
use hex_literal::hex;
use std::sync::Arc;
use uuid::Uuid;

pub const id: Uuid = Uuid::from_bytes(hex!("DB6AB6BB401B4071BE52763C0C550600"));
pub const name: &str = "pg_constraint";

pub const column_id: &str = "id";
pub const column_class_id: &str = "class_id";
pub const column_index_id: &str = "index_id";
pub const column_name: &str = "name";
pub const column_type: &str = "type";

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
            column_index_id.to_string(),
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
            column_type.to_string(),
            BaseSqlTypesMapper::Text,
            Nullable::NotNull,
            None,
        ),
    ]
}

pub fn get_index(attrs: &Vec<Attribute>) -> Arc<Index> {
    Arc::new(Index {
        id: Uuid::from_bytes(hex!("27182DE783AB42D8B5DD43BFC0154F0F")),
        name: name.to_string() + "_name_index",
        columns: Arc::new(SqlTypeDefinition::new(&[attrs[3].clone()])),
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
