//! This defines all the system internal tables so we can bootstrap the system

use hex_literal::hex;
use uuid::Uuid;

use super::super::constants::DeserializeTypes;
use super::super::engine::objects::{Attribute, Table};

#[derive(Copy, Clone)]
pub enum TableDefinitions {
    PgClass, //Tables
}

impl TableDefinitions {
    pub const VALUES: [TableDefinitions; 1] = [TableDefinitions::PgClass];
    pub fn value(self) -> Table {
        use TableDefinitions::*;
        match self {
            PgClass => Table::new_existing(
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
                        DeserializeTypes::Uuid,
                    ),
                ],
            ),
        }
    }
}
