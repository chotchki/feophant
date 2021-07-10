//! Implementation of the null bit flags to know if a column is null or not
//! I'm not using a standard library because the bitvector library collides with nom
use bytes::{BufMut, Bytes, BytesMut};

use crate::engine::objects::SqlTuple;

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

    pub fn parse(input: &Bytes, column_count: usize) -> Vec<bool> {
        let mut buffer = vec![];

        for b in input {
            let mut temp = *b;
            for _ in 0..8 {
                if temp & 0x80 > 0 {
                    buffer.push(true);
                } else {
                    buffer.push(false);
                }
                temp <<= 1;
            }
        }

        buffer.resize(column_count, false);
        buffer
    }
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
    fn test_null_mask_parse() {
        let test = vec![
            true, false, true, false, true, false, true, false, true, false,
        ];

        let result = NullMask::parse(&Bytes::from_static(&hex!("aa 80")), 10);

        assert_eq!(result, test);
    }

    #[test]
    fn test_null_mask_parse_short() {
        let test = vec![true, false, false, false, false, false, false, false, false];

        let result = NullMask::parse(&Bytes::from_static(&hex!("80")), 9);

        assert_eq!(result, test);
    }

    #[test]
    fn test_null_mask_roundtrip() {
        let test = get_tuple();

        let end = vec![
            true, false, true, false, true, false, true, false, true, false, false, false,
        ];

        let result = NullMask::serialize(&test);
        assert_eq!(Bytes::from_static(&hex!("aa 80")), result);
        let parse = NullMask::parse(&result, 12);

        assert_eq!(end, parse);
    }
}
