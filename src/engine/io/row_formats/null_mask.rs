//! Implementation of the null bit flags to know if a column is null or not
//! I'm not using a standard library because the bitvector library collides with nom
use crate::engine::objects::SqlTuple;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

pub struct NullMask {}

impl NullMask {
    pub fn serialize(input: &SqlTuple) -> Bytes {
        if input.0.len() == 0 {
            return Bytes::new();
        }

        let mut buffer = BytesMut::new();
        let mut any_null = false;

        let mut value: u8 = 0;
        let mut mask: u8 = 0x80;
        let mut i = 0;
        loop {
            if input.0[i].is_none() {
                value |= mask;
                any_null = true;
            }

            if (i + 1) == input.0.len() {
                if (i + 1) % 8 != 0 {
                    buffer.put_u8(value);
                }
                break;
            }

            if (i + 1) % 8 == 0 && i > 0 {
                buffer.put_u8(value);
                value = 0;
                mask = 0x80;
            } else {
                mask >>= 1;
            }

            i += 1;
        }

        if !any_null {
            return Bytes::new();
        }
        buffer.freeze()
    }

    pub fn parse(buffer: &mut impl Buf, column_count: usize) -> Result<Vec<bool>, NullMaskError> {
        let mut nulls = vec![];

        if buffer.remaining() <= column_count / 8 {
            return Err(NullMaskError::BufferTooShort(
                buffer.remaining(),
                column_count / 8,
            ));
        }

        let mut remaining_columns = column_count;
        while remaining_columns > 0 {
            let mut temp = buffer.get_u8();
            for _ in 0..8 {
                if temp & 0x80 > 0 {
                    nulls.push(true);
                } else {
                    nulls.push(false);
                }
                temp <<= 1;
            }
            remaining_columns = remaining_columns.saturating_sub(8);
        }

        //This is needed since we encode more values than columns
        nulls.resize(column_count, false);

        Ok(nulls)
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum NullMaskError {
    #[error("Buffer too short to parse found {0} bytes, need {1}")]
    BufferTooShort(usize, usize),
}

#[cfg(test)]
mod tests {
    use crate::constants::BuiltinSqlTypes;

    use super::*;
    use hex_literal::hex;

    fn get_tuple() -> SqlTuple {
        SqlTuple(vec![
            None,
            Some(BuiltinSqlTypes::Bool(true)),
            None,
            Some(BuiltinSqlTypes::Bool(true)),
            None,
            Some(BuiltinSqlTypes::Bool(true)),
            None,
            Some(BuiltinSqlTypes::Bool(true)),
            None,
            Some(BuiltinSqlTypes::Bool(true)),
        ])
    }

    #[test]
    fn test_null_mask_serialize() {
        let test = get_tuple();

        let result = NullMask::serialize(&test);

        assert_eq!(hex!("aa 80").to_vec(), result.to_vec());
    }

    #[test]
    fn test_null_mask_single() {
        let test = SqlTuple(vec![None]);

        let result = NullMask::serialize(&test);

        assert_eq!(hex!("80").to_vec(), result.to_vec());
    }

    #[test]
    fn test_null_mask_all_false() {
        let test = SqlTuple(vec![
            Some(BuiltinSqlTypes::Bool(true)),
            Some(BuiltinSqlTypes::Bool(true)),
            Some(BuiltinSqlTypes::Bool(true)),
        ]);

        let result = NullMask::serialize(&test);

        assert_eq!(Bytes::new(), result);
    }

    #[test]
    fn test_null_mask_parse() -> Result<(), Box<dyn std::error::Error>> {
        let test = vec![
            true, false, true, false, true, false, true, false, true, false,
        ];

        let res = NullMask::parse(&mut Bytes::from_static(&hex!("aa 80")), 10)?;

        assert_eq!(res, test);
        Ok(())
    }

    #[test]
    fn test_null_mask_parse_short() -> Result<(), Box<dyn std::error::Error>> {
        let res = NullMask::parse(&mut Bytes::from_static(&hex!("80")), 9);
        assert_eq!(res, Err(NullMaskError::BufferTooShort(1, 1)));
        Ok(())
    }

    #[test]
    fn test_null_mask_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let test = get_tuple();

        let end = vec![
            true, false, true, false, true, false, true, false, true, false, false, false,
        ];

        let mut result = NullMask::serialize(&test);
        assert_eq!(Bytes::from_static(&hex!("aa 80")), result);
        let parse = NullMask::parse(&mut result, 12)?;

        assert_eq!(end, parse);
        Ok(())
    }
}
