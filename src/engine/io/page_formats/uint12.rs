//We are only going to support 4kb pages to match most common underlying I/O subsystems
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::convert::TryFrom;
use std::fmt;
use std::mem;
use std::mem::size_of;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use thiserror::Error;

use crate::engine::io::ConstEncodedSize;

const PAGE_SIZE: u16 = 4096;

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

    pub fn to_u16(&self) -> u16 {
        self.0
    }

    pub fn to_usize(&self) -> usize {
        usize::try_from(self.0).unwrap()
    }

    pub fn max() -> UInt12 {
        UInt12(PAGE_SIZE - 1)
    }

    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(mem::size_of::<u16>());
        buf.put_u16_le(self.0);
        buf.freeze()
    }

    pub fn parse(buffer: &mut impl Buf) -> Result<Self, UInt12Error> {
        if buffer.remaining() < mem::size_of::<u16>() {
            return Err(UInt12Error::InsufficentData(buffer.remaining()));
        }

        let raw_value = buffer.get_u16_le();

        let value = UInt12::new(raw_value)?;

        Ok(value)
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
}
