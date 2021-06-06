//We are only going to support 4kb pages to match most common underlying I/O subsystems
use std::convert::TryFrom;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use thiserror::Error;

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

    pub fn new(val: u16) -> Option<UInt12> {
        if UInt12::is_in_range(val) {
            Some(UInt12(val))
        } else {
            None
        }
    }

    pub fn to_u16(&self) -> u16 {
        self.0
    }

    pub fn max() -> UInt12 {
        UInt12(PAGE_SIZE - 1)
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
            return Err(UInt12Error::ValueTooLarge(value));
        }

        Ok(UInt12(value as u16))
    }
}

#[derive(Debug, Error)]
pub enum UInt12Error {
    #[error("Value too large for UInt12 got {0}")]
    ValueTooLarge(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal() {
        let test = UInt12::new(1).unwrap();

        assert_eq!(test.to_u16(), 1);
    }

    #[test]
    fn test_math() {
        let mut test = UInt12::new(1).unwrap();

        test += UInt12::new(1).unwrap();
        test -= UInt12::new(1).unwrap();

        assert_eq!(test.to_u16(), 1);
    }

    #[test]
    fn test_usize() {
        let large: usize = 400;
        let test = UInt12::try_from(large).unwrap();

        assert_eq!(test.to_u16(), 400);
    }

    #[test]
    fn test_fail_usize() {
        let large: usize = 4000;
        let test = UInt12::try_from(large);

        assert!(test.is_err());
    }
}
