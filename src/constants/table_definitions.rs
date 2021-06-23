//! This defines all the system internal tables so we can bootstrap the system

use hex_literal::hex;
use std::sync::Arc;
use uuid::Uuid;

use super::super::constants::DeserializeTypes;
use super::super::engine::objects::{Attribute, Table};

#[derive(Copy, Clone)]
pub enum TableDefinitions {
    PgAttribute, //Columns
    PgClass,     //Tables
}

impl TableDefinitions {
    pub const VALUES: [TableDefinitions; 2] =
        [TableDefinitions::PgAttribute, TableDefinitions::PgClass];
    pub fn value(self) -> Arc<Table> {
        match self {
            TableDefinitions::PgClass => Arc::new(Table::new_existing(
                Uuid::from_bytes(hex!("EE919E33D9054F4889537EBB6CC911EB")),
                "pg_class".to_string(),
                vec![
                    Attribute::new_existing(
                        Uuid::from_bytes(hex!("3BC7F6F30FAA4084AA9F463CB323A1A5")),
                        Uuid::from_bytes(hex!("EE919E33D9054F4889537EBB6CC911EB")),
                        "id".to_string(),
                        DeserializeTypes::Uuid,
                    ),
                    Attribute::new_existing(
                        Uuid::from_bytes(hex!("1C1D1831357A493AAE048AA560E351A2")),
                        Uuid::from_bytes(hex!("EE919E33D9054F4889537EBB6CC911EB")),
                        "name".to_string(),
                        DeserializeTypes::Text,
                    ),
                ],
            )),
            TableDefinitions::PgAttribute => Arc::new(Table::new_existing(
                Uuid::from_bytes(hex!("EE89957F3E9F482C836DDA6C349AC632")),
                "pg_attribute".to_string(),
                vec![
                    Attribute::new_existing(
                        Uuid::from_bytes(hex!("C24D0F5C66884C5E8642457BA23E301F")),
                        Uuid::from_bytes(hex!("EE89957F3E9F482C836DDA6C349AC632")),
                        "attrelid".to_string(),
                        DeserializeTypes::Uuid,
                    ),
                    Attribute::new_existing(
                        Uuid::from_bytes(hex!("2F591C814DBC4B7DA96DC8EB4698FC63")),
                        Uuid::from_bytes(hex!("EE89957F3E9F482C836DDA6C349AC632")),
                        "attname".to_string(),
                        DeserializeTypes::Text,
                    ),
                    Attribute::new_existing(
                        Uuid::from_bytes(hex!("D36A417A25D44019B8AA91DF9783EDEA")),
                        Uuid::from_bytes(hex!("EE89957F3E9F482C836DDA6C349AC632")),
                        "atttypid".to_string(),
                        DeserializeTypes::Text, //TODO join to pg_type instead, for now its a string
                    ),
                    Attribute::new_existing(
                        Uuid::from_bytes(hex!("73479C7B65EA474DA0CE2812DD0143F9")),
                        Uuid::from_bytes(hex!("EE89957F3E9F482C836DDA6C349AC632")),
                        "attnum".to_string(),
                        DeserializeTypes::Integer,
                    ),
                ],
            )),
        }
    }
}
