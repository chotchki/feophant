//We are only going to support 4kb pages to match most common underlying I/O subsystems

const PAGE_SIZE: u16 = 4096;

#[derive(Debug, PartialEq)]
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

    pub fn add(&self, other: UInt12) -> UInt12 {
        UInt12(UInt12::clamp(self.0.saturating_add(other.0)))
    }

    pub fn subtract(&self, other: UInt12) -> UInt12 {
        UInt12(UInt12::clamp(self.0.saturating_sub(other.0)))
    }

    pub fn to_u16(&self) -> u16 {
        self.0
    }

    pub fn max() -> UInt12 {
        UInt12(PAGE_SIZE - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal() {
        let test = UInt12::new(1).unwrap();

        assert_eq!(test.to_u16(), 1);
    }
}
