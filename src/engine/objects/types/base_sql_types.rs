use crate::engine::io::{encode_size, expected_encoded_size, parse_size, SizeError};
use bytes::{Buf, BufMut, BytesMut};
use std::{
    fmt::{self, Display, Formatter},
    mem::size_of,
    num::ParseIntError,
    str::ParseBoolError,
    string::FromUtf8Error,
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum BaseSqlTypes {
    Array(Vec<BaseSqlTypes>),
    Bool(bool),
    Integer(u32),
    Text(String),
    Uuid(uuid::Uuid),
}

/// BaseSqlTypesMapper exists to map to SqlType without imposing the cost of an empty version
///
/// This will exist until this RFC is fixed: https://github.com/rust-lang/rfcs/pull/2593
#[derive(Clone, Debug, PartialEq)]
pub enum BaseSqlTypesMapper<'a> {
    Array(&'a BaseSqlTypesMapper<'a>),
    Bool,
    Integer,
    Text,
    Uuid,
}

impl BaseSqlTypes {
    pub fn deserialize(
        target_type: &BaseSqlTypesMapper,
        buffer: &mut impl Buf,
    ) -> Result<Self, BaseSqlTypesError> {
        match target_type {
            BaseSqlTypesMapper::Array(a) => {
                let count = parse_size(buffer)?;
                let mut items = vec![];

                for i in 0..count {
                    items.push(Self::deserialize(a, buffer)?);
                }
                Ok(BaseSqlTypes::Array(items))
            }
            BaseSqlTypesMapper::Bool => {
                if buffer.remaining() < size_of::<u8>() {
                    return Err(BaseSqlTypesError::InsufficentBuffer(
                        size_of::<u8>(),
                        buffer.remaining(),
                    ));
                }
                let dest = buffer.get_u8();
                let value = match dest {
                    0x0 => BaseSqlTypes::Bool(false),
                    _ => BaseSqlTypes::Bool(true),
                };

                Ok(value)
            }
            BaseSqlTypesMapper::Integer => {
                if buffer.remaining() < size_of::<u32>() {
                    return Err(BaseSqlTypesError::InsufficentBuffer(
                        size_of::<u32>(),
                        buffer.remaining(),
                    ));
                }

                let dest = buffer.get_u32_le();
                Ok(BaseSqlTypes::Integer(dest))
            }
            BaseSqlTypesMapper::Uuid => {
                if buffer.remaining() < size_of::<u128>() {
                    return Err(BaseSqlTypesError::InsufficentBuffer(
                        size_of::<u128>(),
                        buffer.remaining(),
                    ));
                }
                let dest = buffer.get_u128_le();
                Ok(BaseSqlTypes::Uuid(uuid::Uuid::from_u128(dest)))
            }
            BaseSqlTypesMapper::Text => {
                let length = parse_size(buffer)?;

                if length > buffer.remaining() {
                    return Err(BaseSqlTypesError::InsufficentBuffer(
                        length,
                        buffer.remaining(),
                    ));
                }

                //TODO find a way to skip a copy here
                let value_buff = buffer.copy_to_bytes(length);
                let value_str = String::from_utf8(value_buff.to_vec())?;

                Ok(BaseSqlTypes::Text(value_str))
            }
        }
    }

    /// Provides the expected size of the serialized form so repeated serialization
    /// is not needed to find space.
    pub fn expected_encoded_size(&self) -> usize {
        match self {
            Self::Array(ref a) => a.iter().fold(0, |acc, x| acc + x.expected_encoded_size()),
            Self::Bool(_) => size_of::<bool>(),
            Self::Integer(_) => size_of::<u32>(),
            Self::Uuid(_) => size_of::<Uuid>(),
            Self::Text(ref t) => expected_encoded_size(t.len()) + t.len(),
        }
    }

    pub fn parse(target_type: BaseSqlTypesMapper, buffer: &str) -> Result<Self, BaseSqlTypesError> {
        match target_type {
            BaseSqlTypesMapper::Array(a) => todo!("Bed time, need to fix!"),
            BaseSqlTypesMapper::Bool => Ok(BaseSqlTypes::Bool(buffer.parse::<bool>()?)),
            BaseSqlTypesMapper::Integer => Ok(BaseSqlTypes::Integer(buffer.parse::<u32>()?)),
            BaseSqlTypesMapper::Uuid => Ok(BaseSqlTypes::Uuid(uuid::Uuid::parse_str(&buffer)?)),
            BaseSqlTypesMapper::Text => Ok(BaseSqlTypes::Text(buffer.to_string())),
        }
    }

    pub fn serialize(&self, buffer: &mut BytesMut) {
        match *self {
            Self::Array(ref value) => {
                encode_size(buffer, value.len());
                for v in value {
                    v.serialize(buffer);
                }
            }
            Self::Bool(ref value) => {
                if *value {
                    buffer.put_u8(0x1);
                } else {
                    buffer.put_u8(0x0);
                }
            }
            Self::Integer(ref value) => {
                buffer.put_u32_le(*value);
            }
            Self::Uuid(ref value) => {
                buffer.put_u128_le(value.as_u128());
            }
            Self::Text(ref value) => {
                encode_size(buffer, value.len());
                buffer.extend_from_slice(value.as_bytes());
            }
        }
    }

    /// Used to map if we have the types linked up right.
    /// We can't check for the array subtypes to be right
    pub fn type_matches(self, right: BaseSqlTypesMapper) -> bool {
        match (self, right) {
            (Self::Array(a), BaseSqlTypesMapper::Array(b)) => true,
            (Self::Bool(_), BaseSqlTypesMapper::Bool) => true,
            (Self::Integer(_), BaseSqlTypesMapper::Integer) => true,
            (Self::Text(_), BaseSqlTypesMapper::Text) => true,
            (Self::Uuid(_), BaseSqlTypesMapper::Uuid) => true,
            (_, _) => false,
        }
    }
}

impl Display for BaseSqlTypes {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BaseSqlTypes::Array(ref value) => {
                write!(f, "{:#?}", value)
            }
            BaseSqlTypes::Bool(ref value) => {
                write!(f, "{}", value)
            }
            BaseSqlTypes::Integer(ref value) => {
                write!(f, "{}", value)
            }
            BaseSqlTypes::Uuid(ref value) => {
                write!(f, "{}", value)
            }
            BaseSqlTypes::Text(ref value) => {
                write!(f, "{}", value)
            }
        }
    }
}

impl<'a> Display for BaseSqlTypesMapper<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BaseSqlTypesMapper::Array(a) => {
                write!(f, "Array of {}", **a)
            }
            BaseSqlTypesMapper::Bool => {
                write!(f, "Bool")
            }
            BaseSqlTypesMapper::Integer => {
                write!(f, "Integer")
            }
            BaseSqlTypesMapper::Uuid => {
                write!(f, "Uuid")
            }
            BaseSqlTypesMapper::Text => {
                write!(f, "Text")
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum BaseSqlTypesError {
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("Length needed {0}, length found {1}")]
    InsufficentBuffer(usize, usize),
    #[error(transparent)]
    InvalidBool(#[from] ParseBoolError),
    #[error(transparent)]
    InvalidInt(#[from] ParseIntError),
    #[error(transparent)]
    InvalidUuid(#[from] uuid::Error),
    #[error(transparent)]
    SizeError(#[from] SizeError),
}

#[cfg(test)]
mod tests {
    use hex_literal::hex;
    use uuid::Uuid;

    use super::*;

    fn roundtrip(input: String) -> String {
        let stype = BaseSqlTypes::Text(input);
        let mut buffer = BytesMut::with_capacity(stype.expected_encoded_size());
        stype.serialize(&mut buffer);
        let mut fbuff = buffer.freeze();
        let result = BaseSqlTypes::deserialize(&BaseSqlTypesMapper::Text, &mut fbuff).unwrap();
        match result {
            BaseSqlTypes::Text(t) => t,
            _ => {
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
    fn test_builtin_display() {
        let text = BaseSqlTypes::Bool(true);
        assert_eq!(text.to_string(), "true");

        let text = BaseSqlTypes::Integer(5);
        assert_eq!(text.to_string(), "5");

        let text = BaseSqlTypes::Text("FOOBAR".to_string());
        assert_eq!(text.to_string(), "FOOBAR");

        let text = BaseSqlTypes::Uuid(Uuid::from_bytes(hex!("EE919E33D9054F4889537EBB6CC911EB")));
        assert_eq!(text.to_string(), "ee919e33-d905-4f48-8953-7ebb6cc911eb");

        assert_eq!(BaseSqlTypesMapper::Bool.to_string(), "Bool");
        assert_eq!(BaseSqlTypesMapper::Integer.to_string(), "Integer");
        assert_eq!(BaseSqlTypesMapper::Text.to_string(), "Text");
        assert_eq!(BaseSqlTypesMapper::Uuid.to_string(), "Uuid");
    }

    #[test]
    fn test_long_roundtrip() {
        let test = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec vel porta enim. Sed interdum egestas velit et porttitor. Vestibulum sollicitudin mi enim, in fringilla lectus tincidunt quis. Morbi eget.";
        let output = roundtrip(test.to_string());

        assert_eq!(output, test);
    }

    #[test]
    fn test_integer_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let test = "5";
        let parse_str = BaseSqlTypes::parse(BaseSqlTypesMapper::Integer, test)?;
        match parse_str {
            BaseSqlTypes::Integer(i) => {
                assert_eq!(i, 5);
                i
            }
            _ => panic!("Wrong type"),
        };

        let mut buffer = BytesMut::with_capacity(parse_str.expected_encoded_size());
        parse_str.serialize(&mut &mut buffer);
        let mut parse_serial = buffer.freeze();
        let reparse = BaseSqlTypes::deserialize(&BaseSqlTypesMapper::Integer, &mut parse_serial)?;
        match reparse {
            BaseSqlTypes::Integer(i) => {
                assert_eq!(i, 5)
            }
            _ => panic!("Wrong type"),
        };

        Ok(())
    }

    #[test]
    //Used to map if we have the types linked up right
    pub fn test_type_matches() {
        assert!(BaseSqlTypes::Bool(true).type_matches(BaseSqlTypesMapper::Bool));
        assert!(!BaseSqlTypes::Bool(true).type_matches(BaseSqlTypesMapper::Integer));
        assert!(!BaseSqlTypes::Bool(true).type_matches(BaseSqlTypesMapper::Uuid));
        assert!(!BaseSqlTypes::Bool(true).type_matches(BaseSqlTypesMapper::Text));

        assert!(!BaseSqlTypes::Integer(0).type_matches(BaseSqlTypesMapper::Bool));
        assert!(BaseSqlTypes::Integer(0).type_matches(BaseSqlTypesMapper::Integer));
        assert!(!BaseSqlTypes::Integer(0).type_matches(BaseSqlTypesMapper::Uuid));
        assert!(!BaseSqlTypes::Integer(0).type_matches(BaseSqlTypesMapper::Text));

        assert!(!BaseSqlTypes::Uuid(uuid::Uuid::new_v4()).type_matches(BaseSqlTypesMapper::Bool));
        assert!(!BaseSqlTypes::Uuid(uuid::Uuid::new_v4()).type_matches(BaseSqlTypesMapper::Integer));
        assert!(BaseSqlTypes::Uuid(uuid::Uuid::new_v4()).type_matches(BaseSqlTypesMapper::Uuid));
        assert!(!BaseSqlTypes::Uuid(uuid::Uuid::new_v4()).type_matches(BaseSqlTypesMapper::Text));

        assert!(!BaseSqlTypes::Text("foo".to_string()).type_matches(BaseSqlTypesMapper::Bool));
        assert!(!BaseSqlTypes::Text("foo".to_string()).type_matches(BaseSqlTypesMapper::Integer));
        assert!(!BaseSqlTypes::Text("foo".to_string()).type_matches(BaseSqlTypesMapper::Uuid));
        assert!(BaseSqlTypes::Text("foo".to_string()).type_matches(BaseSqlTypesMapper::Text));
    }
}
