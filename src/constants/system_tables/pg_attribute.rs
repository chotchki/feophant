use crate::constants::Nullable;
use crate::engine::objects::{
    types::{BaseSqlTypesMapper, SqlTypeDefinition},
    Attribute, Constraint, Index, PrimaryKeyConstraint, Table,
};
use hex_literal::hex;
use std::sync::Arc;
use uuid::Uuid;

pub const id: Uuid = Uuid::from_bytes(hex!("EE89957F3E9F482C836DDA6C349AC632"));
pub const name: &str = "pg_attribute";

pub const column_class_id: &str = "class_id";
pub const column_name: &str = "name";
pub const column_sql_type: &str = "type_name";
pub const column_column_num: &str = "column_num";
pub const column_nullable: &str = "nullable";

pub fn get_columns() -> Vec<Attribute> {
    vec![
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
            column_sql_type.to_string(),
            BaseSqlTypesMapper::Text, //TODO join to pg_type instead, for now its a string
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            column_column_num.to_string(),
            BaseSqlTypesMapper::Integer,
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            column_nullable.to_string(),
            BaseSqlTypesMapper::Bool,
            Nullable::NotNull,
            None,
        ),
    ]
}

pub fn get_index(attrs: &Vec<Attribute>) -> Arc<Index> {
    Arc::new(Index {
        id: Uuid::from_bytes(hex!("516B20412CF145A2AD9E39A8BDEB30A8")),
        name: name.to_string() + "_name_index",
        columns: Arc::new(SqlTypeDefinition::new(&[attrs[1].clone()])),
        unique: true,
    })
}

pub fn get_table() -> Arc<Table> {
    let columns = get_columns();
    let index = get_index(&columns);
    Arc::new(Table::new(
        id,
        name.to_string(),
        get_columns(),
        vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
            name: index.name.clone() + "_primary_key",
            index: index.clone(),
        })],
        vec![index],
    ))
}
