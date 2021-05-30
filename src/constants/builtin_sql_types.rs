use bytes::{Buf,Bytes};
use uuid::Uuid;

use super::super::engine::objects::{SqlType,SqlTypeError};
use super::super::engine::objects::types::{TextType,UuidType};

pub enum BuiltinSqlTypes {
    Uuid(UuidType),
    Text(TextType)
}

impl BuiltinSqlTypes {
    fn serialize(&self) -> Bytes {
        match *self {
            BuiltinSqlTypes::Uuid(ref value) => {
                Bytes::copy_from_slice(value.get().as_bytes())
            },
            BuiltinSqlTypes::Text(ref value) => {
                let mut length = value.get().len();

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
        
                buff.extend_from_slice(value.get().as_bytes());
        
                Bytes::copy_from_slice(&buff)
            },
        }
    }

    fn deserialize(target_type: &str, mut bytes: Bytes) -> Result<Self, SqlTypeError> {
        match target_type {
            "uuid" => {
                if bytes.len() < 16 {
                    return Err(SqlTypeError::LengthTooShort(bytes.len()));
                }
                let mut dest = [0; 16];
                dest.copy_from_slice(&bytes.slice(0..bytes.len()));
        
                let value = BuiltinSqlTypes::Uuid(
                    UuidType::new(uuid::Uuid::from_bytes(dest))
                );
        
                Ok(value)
            },
            "text" => {
                if bytes.len() == 0 {
                    return Err(SqlTypeError::EmptyBuffer())
                }
        
                let mut length:usize = 0;
        
                let mut high_bit = 1;
                let mut loop_count = 0;
                while high_bit == 1 {
                    if !bytes.has_remaining() {
                        return Err(SqlTypeError::BufferTooShort());
                    }
        
                    let b = bytes.get_u8();
                    high_bit = b >> 7;
        
                    let mut low_bits:usize = (b & 0x7f).into();
                    low_bits = low_bits<<(7*loop_count);
                    loop_count = loop_count + 1;
        
                    length = length + low_bits;
                }
        
                if length != bytes.remaining() {
                    return Err(SqlTypeError::InvalidStringLength(length, bytes.remaining()));
                }
                
                let value_str = String::from_utf8(bytes.slice(0..length).to_vec()).map_err(|e| SqlTypeError::InvalidUtf8(e))?;
        
                let value = BuiltinSqlTypes::Text(
                    TextType::new(value_str)
                );
        
                Ok(value)
            }
            _ => panic!("Should not get here")
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    fn roundtrip(input: String) -> TextType {
        let stype = BuiltinSqlTypes::Text(TextType::new(input.to_string()));
        let serialized = stype.serialize();
        let result = BuiltinSqlTypes::deserialize("text", serialized).unwrap();
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

        assert_eq!(output.get(), test);
    }

    #[test]
    fn test_long_roundtrip() {
        let test = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec vel porta enim. Sed interdum egestas velit et porttitor. Vestibulum sollicitudin mi enim, in fringilla lectus tincidunt quis. Morbi eget.";
        let output = roundtrip(test.to_string());

        assert_eq!(output.get(), test);
    }
}