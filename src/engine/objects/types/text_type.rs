use bytes::{Buf,Bytes};

use super::super::SqlType;
use super::super::SqlTypeError;

pub struct TextType {
    data: String
}

impl TextType {
    pub fn new(data: String) -> TextType {
        TextType {
            data
        }
    }

    pub fn get(&self) -> String {
        self.data.clone()
    }
}

impl SqlType for TextType {
    fn serialize(&self) -> Bytes {
        let mut length = self.data.len();

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

        buff.extend_from_slice(self.data.as_bytes());

        Bytes::copy_from_slice(&buff)
    }

    fn deserialize(mut bytes: Bytes) -> Result<Box<TextType>, SqlTypeError> {
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

        let value = TextType {
            data: value_str
        };

        Ok(Box::new(value))
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    fn roundtrip(input: String) -> Box<TextType> {
        let stype = TextType::new(input.to_string());
        let serialized = stype.serialize();
        TextType::deserialize(serialized).unwrap()
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