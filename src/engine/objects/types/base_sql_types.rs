use crate::engine::io::{
    encode_size, expected_encoded_size, parse_size, SelfEncodedSize, SizeError,
};
use bytes::{Buf, BufMut};
use nom::{
    error::{convert_error, VerboseError},
    Finish,
};
use std::{
    fmt::{self, Display, Formatter},
    mem::size_of,
    num::ParseIntError,
    str::{FromStr, ParseBoolError, Utf8Error},
    sync::Arc,
};
use thiserror::Error;
use uuid::Uuid;

use super::parse_type;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BaseSqlTypes {
    //TODO consider making it an Arc since I don't mutate just copy
    Array(Vec<BaseSqlTypes>),
    Bool(bool),
    Integer(u32),
    //TODO consider making it an Arc since I don't mutate just copy
    Text(String),
    Uuid(uuid::Uuid),
}

/// BaseSqlTypesMapper exists to map to SqlType without imposing the cost of an empty version
///
/// This will exist until this RFC is brought back: https://github.com/rust-lang/rfcs/pull/2593
#[derive(Clone, Debug, PartialEq)]
pub enum BaseSqlTypesMapper {
    Array(Arc<BaseSqlTypesMapper>),
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
                let mut items = Vec::with_capacity(count);

                for _ in 0..count {
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

                let value_buff = buffer.copy_to_bytes(length);
                let value_str = std::str::from_utf8(&value_buff)?;

                Ok(BaseSqlTypes::Text(value_str.to_string()))
            }
        }
    }

    pub fn parse(target_type: BaseSqlTypesMapper, buffer: &str) -> Result<Self, BaseSqlTypesError> {
        match target_type {
            //TODO Need to fix this to support array literal parsing
            //See here: https://www.postgresql.org/docs/current/arrays.html
            BaseSqlTypesMapper::Array(a) => todo!("Bed time, need to fix!"),
            BaseSqlTypesMapper::Bool => Ok(BaseSqlTypes::Bool(buffer.parse::<bool>()?)),
            BaseSqlTypesMapper::Integer => Ok(BaseSqlTypes::Integer(buffer.parse::<u32>()?)),
            BaseSqlTypesMapper::Uuid => Ok(BaseSqlTypes::Uuid(uuid::Uuid::parse_str(buffer)?)),
            BaseSqlTypesMapper::Text => Ok(BaseSqlTypes::Text(buffer.to_string())),
        }
    }

    pub fn serialize(&self, buffer: &mut impl BufMut) {
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
                buffer.put_slice(value.as_bytes());
            }
        }
    }

    /// Used to map if we have the types linked up right.
    pub fn type_matches(&self, right: &BaseSqlTypesMapper) -> bool {
        match (self, right) {
            //TODO unit test on arrays
            (Self::Array(a), BaseSqlTypesMapper::Array(b)) => {
                if a.is_empty() {
                    //We match if empty since this means we can still write it to disk
                    return true;
                }
                match (&a[0], b.as_ref()) {
                    (Self::Bool(_), BaseSqlTypesMapper::Bool) => true,
                    (Self::Integer(_), BaseSqlTypesMapper::Integer) => true,
                    (Self::Text(_), BaseSqlTypesMapper::Text) => true,
                    (Self::Uuid(_), BaseSqlTypesMapper::Uuid) => true,
                    (_, _) => false,
                }
            }
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

impl Display for BaseSqlTypesMapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            //TODO Write unit test!
            BaseSqlTypesMapper::Array(ref a) => match a.as_ref() {
                BaseSqlTypesMapper::Array(ref aa) => write!(f, "Array({})", **aa),
                BaseSqlTypesMapper::Bool => write!(f, "Array(Bool)"),
                BaseSqlTypesMapper::Integer => write!(f, "Array(Integer)"),
                BaseSqlTypesMapper::Uuid => write!(f, "Array(Uuid)"),
                BaseSqlTypesMapper::Text => write!(f, "Array(Text)"),
            },
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

impl FromStr for BaseSqlTypesMapper {
    type Err = BaseSqlTypesError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parse_type::<VerboseError<&str>>(s).finish() {
            Ok((_, sql_type)) => Ok(sql_type),
            Err(e) => Err(BaseSqlTypesError::ParseError(convert_error(s, e))),
        }
    }
}

impl SelfEncodedSize for BaseSqlTypes {
    /// Provides the expected size of the serialized form so repeated serialization
    /// is not needed to find space.
    fn encoded_size(&self) -> usize {
        match self {
            Self::Array(ref a) => {
                expected_encoded_size(a.len()) + a.iter().fold(0, |acc, x| acc + x.encoded_size())
            }
            Self::Bool(_) => size_of::<bool>(),
            Self::Integer(_) => size_of::<u32>(),
            Self::Uuid(_) => size_of::<Uuid>(),
            Self::Text(ref t) => expected_encoded_size(t.len()) + t.len(),
        }
    }
}

#[derive(Error, Debug)]
pub enum BaseSqlTypesError {
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
    #[error("Length needed {0}, length found {1}")]
    InsufficentBuffer(usize, usize),
    #[error(transparent)]
    InvalidBool(#[from] ParseBoolError),
    #[error(transparent)]
    InvalidInt(#[from] ParseIntError),
    #[error("Invalid type {0}")]
    InvalidType(String),
    #[error(transparent)]
    InvalidUuid(#[from] uuid::Error),
    #[error("SQL Parse Error {0}")]
    ParseError(String),
    #[error(transparent)]
    SizeError(#[from] SizeError),
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use hex_literal::hex;
    use uuid::Uuid;

    use super::*;

    fn roundtrip(input: String) -> String {
        let stype = BaseSqlTypes::Text(input);
        let mut buffer = BytesMut::with_capacity(stype.encoded_size());
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
    fn test_exact_encoding() {
        let test = BaseSqlTypes::Text("Short String".to_string());
        assert_eq!(13, test.encoded_size()); //12 bytes plus 1 length

        let mut buffer = BytesMut::with_capacity(test.encoded_size());
        test.serialize(&mut buffer);
        let buff = buffer.freeze();
        assert_eq!(13, buff.len());

        let test = BaseSqlTypes::Text("Short".repeat(40)); //Should be 200 chars
        assert_eq!(202, test.encoded_size()); //200 bytes plus 2 length

        let mut buffer = BytesMut::with_capacity(test.encoded_size());
        test.serialize(&mut buffer);
        let buff = buffer.freeze();
        assert_eq!(202, buff.len());
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

        let mut buffer = BytesMut::with_capacity(parse_str.encoded_size());
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
    fn test_array_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let array = BaseSqlTypes::Array(vec![BaseSqlTypes::Integer(1), BaseSqlTypes::Integer(2)]);

        let mut buffer = BytesMut::new();
        array.serialize(&mut buffer);

        let mut buffer = buffer.freeze();

        let parse = BaseSqlTypes::deserialize(
            &BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Integer)),
            &mut buffer,
        )?;

        assert_eq!(parse, array);

        Ok(())
    }

    #[test]
    //Used to map if we have the types linked up right
    pub fn test_type_matches() {
        assert!(BaseSqlTypes::Bool(true).type_matches(&BaseSqlTypesMapper::Bool));
        assert!(!BaseSqlTypes::Bool(true).type_matches(&BaseSqlTypesMapper::Integer));
        assert!(!BaseSqlTypes::Bool(true).type_matches(&BaseSqlTypesMapper::Uuid));
        assert!(!BaseSqlTypes::Bool(true).type_matches(&BaseSqlTypesMapper::Text));

        assert!(!BaseSqlTypes::Integer(0).type_matches(&BaseSqlTypesMapper::Bool));
        assert!(BaseSqlTypes::Integer(0).type_matches(&BaseSqlTypesMapper::Integer));
        assert!(!BaseSqlTypes::Integer(0).type_matches(&BaseSqlTypesMapper::Uuid));
        assert!(!BaseSqlTypes::Integer(0).type_matches(&BaseSqlTypesMapper::Text));

        assert!(!BaseSqlTypes::Uuid(uuid::Uuid::new_v4()).type_matches(&BaseSqlTypesMapper::Bool));
        assert!(
            !BaseSqlTypes::Uuid(uuid::Uuid::new_v4()).type_matches(&BaseSqlTypesMapper::Integer)
        );
        assert!(BaseSqlTypes::Uuid(uuid::Uuid::new_v4()).type_matches(&BaseSqlTypesMapper::Uuid));
        assert!(!BaseSqlTypes::Uuid(uuid::Uuid::new_v4()).type_matches(&BaseSqlTypesMapper::Text));

        assert!(!BaseSqlTypes::Text("foo".to_string()).type_matches(&BaseSqlTypesMapper::Bool));
        assert!(!BaseSqlTypes::Text("foo".to_string()).type_matches(&BaseSqlTypesMapper::Integer));
        assert!(!BaseSqlTypes::Text("foo".to_string()).type_matches(&BaseSqlTypesMapper::Uuid));
        assert!(BaseSqlTypes::Text("foo".to_string()).type_matches(&BaseSqlTypesMapper::Text));
    }

    #[test]
    pub fn test_encoded_size() {
        assert_eq!(
            BaseSqlTypes::Array(vec![BaseSqlTypes::Integer(1), BaseSqlTypes::Integer(2)])
                .encoded_size(),
            9
        );
        assert_eq!(
            BaseSqlTypes::Array(vec![
                BaseSqlTypes::Text("Test".to_string()),
                BaseSqlTypes::Text("Test".to_string())
            ])
            .encoded_size(),
            11
        );
        assert_eq!(BaseSqlTypes::Bool(true).encoded_size(), 1);
        assert_eq!(BaseSqlTypes::Integer(1).encoded_size(), 4);
        assert_eq!(BaseSqlTypes::Text("Test".to_string()).encoded_size(), 5);
        assert_eq!(BaseSqlTypes::Uuid(Uuid::new_v4()).encoded_size(), 16);
    }
}
