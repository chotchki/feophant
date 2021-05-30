use bytes::Bytes;
use uuid::Uuid;

use super::super::SqlType;
use super::super::SqlTypeError;

pub struct UuidType {
    data: Uuid
}

impl UuidType {
    pub fn new(data: uuid::Uuid) -> UuidType {
        UuidType {
            data: data
        }
    }

    pub fn get(&self) -> uuid::Uuid {
        self.data
    }
}