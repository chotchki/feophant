use crate::constants::Nullable;
use crate::engine::objects::{
    types::{BaseSqlTypesMapper, SqlTypeDefinition},
    Attribute, Constraint, Index, PrimaryKeyConstraint, Table,
};
use hex_literal::hex;
use std::sync::Arc;
use uuid::Uuid;

pub const ID: Uuid = Uuid::from_bytes(hex!("EE919E33D9054F4889537EBB6CC911EB"));
pub const NAME: &str = "pg_class";

pub const COLUMN_ID: &str = "id";
pub const COLUMN_NAME: &str = "name";

pub fn get_columns() -> Vec<Attribute> {
    vec![
        Attribute::new(
            COLUMN_ID.to_string(),
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
    ]
}

pub fn get_index(attrs: &Vec<Attribute>) -> Arc<Index> {
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
        columns,
        vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
            name: NAME.to_string() + "_primary_key",
            index: index.clone(),
        })],
        vec![index],
    ))
}
