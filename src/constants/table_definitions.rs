//! This defines all the system internal tables so we can bootstrap the system

use hex_literal::hex;
use uuid::Uuid;

use super::super::engine::objects::Table;

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
                Vec::new(),
            ),
        }
    }
}
