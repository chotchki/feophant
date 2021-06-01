//We are only going to support 4kb pages to match most common underlying I/O subsystems

pub const PAGE_SIZE: u16 = 4096;

#[derive(Debug, PartialEq)]
pub struct PageOffset(u16);

impl PageOffset {
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

    pub fn new(val: u16) -> Option<PageOffset> {
        if PageOffset::is_in_range(val) {
            Some(PageOffset(val))
        } else {
            None
        }
    }

    pub fn add(&self, other: PageOffset) -> PageOffset {
        PageOffset(PageOffset::clamp(self.0.saturating_add(other.0)))
    }

    pub fn subtract(&self, other: PageOffset) -> PageOffset {
        PageOffset(PageOffset::clamp(self.0.saturating_sub(other.0)))
    }

    pub fn to_u16(&self) -> u16 {
        self.0
    }

    pub fn max() -> PageOffset {
        PageOffset(PAGE_SIZE - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal() {
        let test = PageOffset::new(1).unwrap();

        assert_eq!(test.to_u16(), 1);
    }
}
