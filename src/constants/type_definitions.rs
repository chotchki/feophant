//! This defines all the system types so we can bootstrap the system

use hex_literal::hex;

use super::super::engine::objects::SqlType;

#[derive(Copy,Clone)]
pub enum TypeDefinitions {
    UuidType,
    TextType
}

impl TypeDefinitions {
    pub const values: [TypeDefinitions; 2] = [TypeDefinitions::UuidType, TypeDefinitions::TextType];
    pub fn value(self) -> Box<dyn SqlType> {
        use TypeDefinitions::*;
        match self {
            UuidType => {
                UuidType::new_existing(uuid::Uuid::from_bytes(hex!("C0CE4AFB34D949BFA02E0759C91D605A")), "uuid".to_string(), Vec::new())
            }
        }
    }
}