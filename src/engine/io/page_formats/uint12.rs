//We are only going to support 4kb pages to match most common underlying I/O subsystems
use bytes::{Buf, BufMut};
use std::convert::TryFrom;
use std::fmt;
use std::mem::size_of;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use thiserror::Error;

use crate::constants::PAGE_SIZE;
use crate::engine::io::ConstEncodedSize;

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub struct UInt12(u16);

impl UInt12 {
    fn is_in_range(val: u16) -> bool {
        val < PAGE_SIZE
    }

    fn clamp(val: u16) -> u16 {
        if val > PAGE_SIZE - 1 {
            return PAGE_SIZE - 1;
        }
        // Otherwise return val itself
        val
    }

    pub fn new(val: u16) -> Result<UInt12, UInt12Error> {
        if UInt12::is_in_range(val) {
            Ok(UInt12(val))
        } else {
            Err(UInt12Error::ValueTooLargeU16(val))
        }
    }

    pub fn to_u16(self) -> u16 {
        self.0
    }

    pub fn to_usize(self) -> usize {
        usize::try_from(self.0).unwrap()
    }

    pub fn max() -> UInt12 {
        UInt12(PAGE_SIZE - 1)
    }

    pub fn serialize_packed(buffer: &mut impl BufMut, args: &[UInt12]) {
        let mut left = true;
        let mut combined = None;

        for a in args {
            if left {
                buffer.put_u8((a.to_u16() & 0x00FF) as u8);
                combined = Some(((a.to_u16() & 0xFF00) >> 4) as u8);
                left = false;
            } else {
                buffer.put_u8(combined.unwrap() | ((a.to_u16() & 0xFF00) >> 8) as u8);
                buffer.put_u8((a.to_u16() & 0x00FF) as u8);
                combined = None;
                left = true;
            }
        }

        if let Some(s) = combined {
            buffer.put_u8(s)
        }
    }

    pub fn parse_packed(
        buffer: &mut impl Buf,
        expected_count: usize,
    ) -> Result<Vec<UInt12>, UInt12Error> {
        let mut items = vec![];
        let mut count = 0;

        let mut left: u16 = 0;
        let mut middle: u16 = 0;

        while items.len() < expected_count {
            if !buffer.has_remaining() {
                return Err(UInt12Error::InsufficentData(buffer.remaining()));
            }

            match count % 3 {
                0 => {
                    left = buffer.get_u8() as u16;
                }
                1 => {
                    middle = buffer.get_u8() as u16;
                    let item = UInt12::new(left | (middle & 0x00F0) << 4)?;
                    items.push(item);
                }
                2 => {
                    let right = buffer.get_u8() as u16;
                    let item = UInt12::new(right | (middle & 0x000F) << 8)?;
                    items.push(item);
                }
                _ => panic!("Modular math is broken."),
            }

            count += 1;
        }

        Ok(items)
    }
}

impl Add for UInt12 {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        UInt12(UInt12::clamp(self.0.saturating_add(other.0)))
    }
}

impl AddAssign for UInt12 {
    fn add_assign(&mut self, other: Self) {
        *self = UInt12(UInt12::clamp(self.0.saturating_add(other.0)))
    }
}

impl Sub for UInt12 {
    type Output = Self;
    fn sub(self, other: Self) -> Self::Output {
        UInt12(UInt12::clamp(self.0.saturating_sub(other.0)))
    }
}

impl SubAssign for UInt12 {
    fn sub_assign(&mut self, other: Self) {
        *self = UInt12(UInt12::clamp(self.0.saturating_sub(other.0)))
    }
}

impl TryFrom<usize> for UInt12 {
    type Error = UInt12Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let max = PAGE_SIZE as usize;
        if value >= max {
            return Err(UInt12Error::ValueTooLargeUSize(value));
        }

        Ok(UInt12(value as u16))
    }
}

impl fmt::Display for UInt12 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ConstEncodedSize for UInt12 {
    fn encoded_size() -> usize {
        size_of::<u16>()
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum UInt12Error {
    #[error("Not enough data to parse, got {0}")]
    InsufficentData(usize),
    #[error("usize too large for UInt12 got {0}")]
    ValueTooLargeUSize(usize),
    #[error("u16 too large for UInt12 got {0}")]
    ValueTooLargeU16(u16),
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_normal() -> Result<(), Box<dyn std::error::Error>> {
        let test = UInt12::new(1)?;

        assert_eq!(test.to_u16(), 1);

        Ok(())
    }

    #[test]
    fn test_math() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = UInt12::new(1)?;

        test += UInt12::new(1)?;
        test -= UInt12::new(1)?;

        assert_eq!(test.to_u16(), 1);

        Ok(())
    }

    #[test]
    fn test_subtraction() -> Result<(), Box<dyn std::error::Error>> {
        let left = UInt12::new(10)?;
        let right = UInt12::new(5)?;

        let result = left - right;

        assert_eq!(result, right);

        Ok(())
    }

    #[test]
    fn test_usize() -> Result<(), Box<dyn std::error::Error>> {
        let large: usize = 400;
        let test = UInt12::try_from(large)?;

        assert_eq!(test.to_u16(), 400);

        Ok(())
    }

    #[test]
    fn test_fail_usize() {
        let large: usize = 40000;
        let test = UInt12::try_from(large);

        assert!(test.is_err());
    }

    fn roundtrip(input: Vec<UInt12>, serial_len: usize) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = BytesMut::new();
        UInt12::serialize_packed(&mut buffer, &input);
        let mut buffer = buffer.freeze();
        assert_eq!(buffer.len(), serial_len);
        let test_rt = UInt12::parse_packed(&mut buffer, input.len())?;
        assert_eq!(test_rt, input);

        Ok(())
    }

    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        //Value that gave me a lovely bug
        roundtrip(vec![UInt12::new(0)?], 2)?;

        //Test numbers were picked to give a distingishable binary pattern for troubleshooting
        roundtrip(vec![UInt12::new(2730)?], 2)?;

        roundtrip(vec![UInt12::new(2730)?, UInt12::new(1365)?], 3)?;

        roundtrip(
            vec![UInt12::new(2730)?, UInt12::new(1365)?, UInt12::new(2730)?],
            5,
        )?;

        Ok(())
    }
}
