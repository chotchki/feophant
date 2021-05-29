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
    #[error("Buffer passed to deserialize is empty")]
    EmptyBuffer(),
    #[error("Buffer too short to deserialize")]
    BufferTooShort(),
    #[error("Length encoded {0}, length found {1}")]
    InvalidStringLength(usize, usize),
    #[error("Invalid utf8")]
    InvalidUtf8(#[from] std::string::FromUtf8Error)
}