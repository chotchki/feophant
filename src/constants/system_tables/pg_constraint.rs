use crate::constants::Nullable;
use crate::engine::objects::{
    types::{BaseSqlTypesMapper, SqlTypeDefinition},
    Attribute, Constraint, Index, PrimaryKeyConstraint, Table,
};
use hex_literal::hex;
use std::sync::Arc;
use uuid::Uuid;

pub const ID: Uuid = Uuid::from_bytes(hex!("DB6AB6BB401B4071BE52763C0C550600"));
pub const NAME: &str = "pg_constraint";

pub const COLUMN_ID: &str = "id";
pub const COLUMN_CLASS_ID: &str = "class_id";
pub const COLUMN_INDEX_ID: &str = "index_id";
pub const COLUMN_NAME: &str = "name";
pub const COLUMN_TYPE: &str = "type";

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
            COLUMN_INDEX_ID.to_string(),
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
            COLUMN_TYPE.to_string(),
            BaseSqlTypesMapper::Text,
            Nullable::NotNull,
            None,
        ),
    ]
}

pub fn get_index(attrs: &Vec<Attribute>) -> Arc<Index> {
    Arc::new(Index {
        id: Uuid::from_bytes(hex!("27182DE783AB42D8B5DD43BFC0154F0F")),
        name: NAME.to_string() + "_name_index",
        columns: Arc::new(SqlTypeDefinition::new(&[attrs[3].clone()])),
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
