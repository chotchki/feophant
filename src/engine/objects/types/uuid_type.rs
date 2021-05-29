use bytes::Bytes;
use uuid::Uuid;

use super::super::SqlType;
use super::super::SqlTypeError;

pub struct UuidType {
    data: Uuid
}

impl SqlType for UuidType {
    fn serialize(&self) -> Bytes {
        Bytes::copy_from_slice(self.data.as_bytes())
    }

    fn deserialize(bytes: Bytes) -> Result<Box<UuidType>, SqlTypeError> {
        if bytes.len() < 16 {
            return Err(SqlTypeError::LengthTooShort(bytes.len()));
        }
        let mut dest = [0; 16];
        dest.copy_from_slice(&bytes.slice(0..bytes.len()));

        let value = UuidType {
            data: Uuid::from_bytes(dest)
        };

        Ok(Box::new(value))
    }
}