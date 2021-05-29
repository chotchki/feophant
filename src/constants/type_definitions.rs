//! This defines all the system types so we can bootstrap the system

use hex_literal::hex;

use super::super::engine::objects::SqlType;

#[derive(Copy,Clone)]
pub enum TypeDefinitions {
    Uuid,
    Varchar
}

impl TypeDefinitions {
    pub const values: [TypeDefinitions; 1] = [TypeDefinitions::Uuid, TypeDefinitions::Varchar];
    pub fn value(self) -> dyn SqlType {
        use TypeDefinitions::*;
        match self {
            Uuid => {
                SqlType::new_existing(uuid::Uuid::from_bytes(hex!("C0CE4AFB34D949BFA02E0759C91D605A")), "uuid".to_string(), Vec::new())
            }
        }
    }
}