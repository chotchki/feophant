use crate::constants::Nullable;
use crate::engine::objects::{
    types::{BaseSqlTypesMapper, SqlTypeDefinition},
    Attribute, Constraint, Index, PrimaryKeyConstraint, Table,
};
use hex_literal::hex;
use std::sync::Arc;
use uuid::Uuid;

pub const ID: Uuid = Uuid::from_bytes(hex!("EE89957F3E9F482C836DDA6C349AC632"));
pub const NAME: &str = "pg_attribute";

pub const COLUMN_CLASS_ID: &str = "class_id";
pub const COLUMN_NAME: &str = "name";
pub const COLUMN_SQL_TYPE: &str = "type_name";
pub const COLUMN_COLUMN_NUM: &str = "column_num";
pub const COLUMN_NULLABLE: &str = "nullable";

pub fn get_columns() -> Vec<Attribute> {
    vec![
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
            COLUMN_SQL_TYPE.to_string(),
            BaseSqlTypesMapper::Text, //TODO join to pg_type instead, for now its a string
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            COLUMN_COLUMN_NUM.to_string(),
            BaseSqlTypesMapper::Integer,
            Nullable::NotNull,
            None,
        ),
        Attribute::new(
            COLUMN_NULLABLE.to_string(),
            BaseSqlTypesMapper::Bool,
            Nullable::NotNull,
            None,
        ),
    ]
}

pub fn get_index(attrs: &[Attribute]) -> Arc<Index> {
    Arc::new(Index {
        id: Uuid::from_bytes(hex!("516B20412CF145A2AD9E39A8BDEB30A8")),
        name: NAME.to_string() + "_name_index",
        columns: Arc::new(SqlTypeDefinition::new(&[attrs[1].clone()])),
        unique: true,
    })
}

pub fn get_table() -> Arc<Table> {
    let columns = get_columns();
    let index = get_index(&columns);
    Arc::new(Table::new(
        ID,
        NAME.to_string(),
        get_columns(),
        vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
            name: index.name.clone() + "_primary_key",
            index: index.clone(),
        })],
        vec![index],
    ))
}
