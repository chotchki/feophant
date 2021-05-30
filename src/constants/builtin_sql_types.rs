use bytes::{Buf, Bytes};
use thiserror::Error;

pub enum BuiltinSqlTypes {
    Text(String),
    Uuid(uuid::Uuid),
}

//This is effectively a selector for BuiltinSqlTypes since I can't figure out a better method :(
#[derive(Clone, Debug)]
pub enum DeserializeTypes {
    Text,
    Uuid,
}

impl BuiltinSqlTypes {
    pub const VALUES: [DeserializeTypes; 2] = [DeserializeTypes::Text, DeserializeTypes::Uuid];
    fn serialize(&self) -> Bytes {
        match *self {
            BuiltinSqlTypes::Uuid(ref value) => Bytes::copy_from_slice(value.as_bytes()),
            BuiltinSqlTypes::Text(ref value) => {
                let mut length = value.len();

                let mut buff = Vec::new();

                while length > 0 {
                    let last_length = length as u8;
                    let mut digit: u8 = last_length & 0x7f;
                    length >>= 7;
                    if length > 0 {
                        digit |= 0x80;
                    }
                    buff.push(digit);
                }

                buff.extend_from_slice(value.as_bytes());

                Bytes::copy_from_slice(&buff)
            }
        }
    }

    fn deserialize(target_type: DeserializeTypes, mut bytes: Bytes) -> Result<Self, SqlTypeError> {
        match target_type {
            DeserializeTypes::Uuid => {
                if bytes.len() < 16 {
                    return Err(SqlTypeError::LengthTooShort(bytes.len()));
                }
                let mut dest = [0; 16];
                dest.copy_from_slice(&bytes.slice(0..bytes.len()));

                let value = BuiltinSqlTypes::Uuid(uuid::Uuid::from_bytes(dest));

                Ok(value)
            }
            DeserializeTypes::Text => {
                if bytes.is_empty() {
                    return Err(SqlTypeError::EmptyBuffer());
                }

                let mut length: usize = 0;

                let mut high_bit = 1;
                let mut loop_count = 0;
                while high_bit == 1 {
                    if !bytes.has_remaining() {
                        return Err(SqlTypeError::BufferTooShort());
                    }

                    let b = bytes.get_u8();
                    high_bit = b >> 7;

                    let mut low_bits: usize = (b & 0x7f).into();
                    low_bits <<= 7 * loop_count;
                    loop_count += 1;

                    length += low_bits;
                }

                if length != bytes.remaining() {
                    return Err(SqlTypeError::InvalidStringLength(length, bytes.remaining()));
                }

                let value_str = String::from_utf8(bytes.slice(0..length).to_vec())
                    .map_err(SqlTypeError::InvalidUtf8)?;

                let value = BuiltinSqlTypes::Text(value_str);

                Ok(value)
            }
        }
    }
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
    InvalidUtf8(#[from] std::string::FromUtf8Error),
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    fn roundtrip(input: String) -> String {
        let stype = BuiltinSqlTypes::Text(input);
        let serialized = stype.serialize();
        let result = BuiltinSqlTypes::deserialize(DeserializeTypes::Text, serialized).unwrap();
        match result {
            BuiltinSqlTypes::Text(t) => t,
            BuiltinSqlTypes::Uuid(_) => {
                panic!("Well this test failed!");
            }
        }
    }

    #[test]
    fn test_short_roundtrip() {
        let test = "Short String";
        let output = roundtrip(test.to_string());

        assert_eq!(output, test);
    }

    #[test]
    fn test_long_roundtrip() {
        let test = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec vel porta enim. Sed interdum egestas velit et porttitor. Vestibulum sollicitudin mi enim, in fringilla lectus tincidunt quis. Morbi eget.";
        let output = roundtrip(test.to_string());

        assert_eq!(output, test);
    }
}
