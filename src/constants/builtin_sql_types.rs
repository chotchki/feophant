use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
use std::mem;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub enum BuiltinSqlTypes {
    Text(String),
    Uuid(uuid::Uuid),
}

//This is effectively a selector for BuiltinSqlTypes since I can't figure out a better method :(
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum DeserializeTypes {
    Text,
    Uuid,
}

impl BuiltinSqlTypes {
    pub const VALUES: [DeserializeTypes; 2] = [DeserializeTypes::Text, DeserializeTypes::Uuid];

    //Used to map if we have the types linked up right
    pub fn type_matches(&self, right: DeserializeTypes) -> bool {
        match *self {
            BuiltinSqlTypes::Uuid(_) => match right {
                DeserializeTypes::Uuid => return true,
                _ => return false,
            },
            BuiltinSqlTypes::Text(_) => match right {
                DeserializeTypes::Text => return true,
                _ => return false,
            },
        }
    }

    pub fn serialize(&self) -> Bytes {
        match *self {
            BuiltinSqlTypes::Uuid(ref value) => {
                let mut buff = BytesMut::with_capacity(mem::size_of::<u128>());
                buff.put_u128_le(value.as_u128());
                buff.freeze()
            }
            BuiltinSqlTypes::Text(ref value) => {
                let mut length = value.len();

                let mut buff = BytesMut::with_capacity((length + 6) / 7);

                while length > 0 {
                    let last_length = length as u8;
                    let mut digit: u8 = last_length & 0x7f;
                    length >>= 7;
                    if length > 0 {
                        digit |= 0x80;
                    }
                    buff.put_u8(digit);
                }

                buff.extend_from_slice(value.as_bytes());

                buff.freeze()
            }
        }
    }

    pub fn deserialize(
        target_type: DeserializeTypes,
        mut buffer: impl Buf,
    ) -> Result<Self, SqlTypeError> {
        match target_type {
            DeserializeTypes::Uuid => {
                if buffer.remaining() < mem::size_of::<u128>() {
                    return Err(SqlTypeError::LengthTooShort(buffer.remaining()));
                }
                let dest = buffer.get_u128_le();
                let value = BuiltinSqlTypes::Uuid(uuid::Uuid::from_u128(dest));

                Ok(value)
            }
            DeserializeTypes::Text => {
                if !buffer.has_remaining() {
                    return Err(SqlTypeError::EmptyBuffer());
                }

                let mut length: usize = 0;
                let mut high_bit = 1;
                let mut loop_count = 0;
                while high_bit == 1 {
                    if !buffer.has_remaining() {
                        return Err(SqlTypeError::BufferTooShort());
                    }

                    let b = buffer.get_u8();
                    high_bit = b >> 7;

                    let mut low_bits: usize = (b & 0x7f).into();
                    low_bits <<= 7 * loop_count;
                    loop_count += 1;

                    length += low_bits;
                }

                if length > buffer.remaining() {
                    return Err(SqlTypeError::InvalidStringLength(
                        length,
                        buffer.remaining(),
                    ));
                }

                let value_buff = buffer.copy_to_bytes(length);
                let value_str =
                    String::from_utf8(value_buff.to_vec()).map_err(SqlTypeError::InvalidUtf8)?;

                let value = BuiltinSqlTypes::Text(value_str);

                Ok(value)
            }
        }
    }
}

impl fmt::Display for BuiltinSqlTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuiltinSqlTypes::Uuid(ref value) => {
                write!(f, "{}", value)
            }
            BuiltinSqlTypes::Text(ref value) => {
                write!(f, "{}", value)
            }
        }
    }
}

impl fmt::Display for DeserializeTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeserializeTypes::Uuid => {
                write!(f, "Uuid")
            }
            DeserializeTypes::Text => {
                write!(f, "Text")
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
