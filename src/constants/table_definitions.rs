//! This defines all the system internal tables so we can bootstrap the system

use super::super::engine::objects::{Attribute, Table};
use crate::constants::Nullable;
use crate::engine::objects::types::BaseSqlTypesMapper;
use hex_literal::hex;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Copy, Clone)]
pub enum TableDefinitions {
    PgAttribute, //Columns
    PgClass,     //Tables
}

impl TableDefinitions {
    //TODO Should this be removed?
    pub const VALUES: [TableDefinitions; 2] =
        [TableDefinitions::PgAttribute, TableDefinitions::PgClass];
    pub fn value(self) -> Arc<Table> {
        match self {
            TableDefinitions::PgClass => Arc::new(Table::new(
                Uuid::from_bytes(hex!("EE919E33D9054F4889537EBB6CC911EB")),
                "pg_class".to_string(),
                vec![
                    Attribute::new(
                        "id".to_string(),
                        BaseSqlTypesMapper::Uuid,
                        Nullable::NotNull,
                        None,
                    ),
                    Attribute::new(
                        "name".to_string(),
                        BaseSqlTypesMapper::Text,
                        Nullable::NotNull,
                        None,
                    ),
                ],
            )),
            TableDefinitions::PgAttribute => Arc::new(Table::new(
                Uuid::from_bytes(hex!("EE89957F3E9F482C836DDA6C349AC632")),
                "pg_attribute".to_string(),
                vec![
                    Attribute::new(
                        "attrelid".to_string(),
                        BaseSqlTypesMapper::Uuid,
                        Nullable::NotNull,
                        None,
                    ),
                    Attribute::new(
                        "attname".to_string(),
                        BaseSqlTypesMapper::Text,
                        Nullable::NotNull,
                        None,
                    ),
                    Attribute::new(
                        "atttypid".to_string(),
                        BaseSqlTypesMapper::Text, //TODO join to pg_type instead, for now its a string
                        Nullable::NotNull,
                        None,
                    ),
                    Attribute::new(
                        "attnum".to_string(),
                        BaseSqlTypesMapper::Integer,
                        Nullable::NotNull,
                        None,
                    ),
                    Attribute::new(
                        "attnotnull".to_string(),
                        BaseSqlTypesMapper::Bool,
                        Nullable::NotNull,
                        None,
                    ),
                ],
            )),
        }
    }
}
