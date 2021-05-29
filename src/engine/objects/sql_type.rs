//! I'm thinking that I treat this like the system tables, have a lookup stuct
//! 
//! Postgres Doc: https://www.postgresql.org/docs/current/catalog-pg-type.html
use bytes::Bytes;
use thiserror::Error;
use uuid::Uuid;

pub trait SqlType {
    fn serialize(&self) -> Bytes;
    fn deserialize(bytes: Bytes) -> Result<Box<Self>, SqlTypeError>;
}

#[derive(Error, Debug)]
pub enum SqlTypeError {
    #[error("Not enough space for a uuid, got {0}")]
    LengthTooShort(usize),
}