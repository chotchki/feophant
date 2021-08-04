use std::{
    fmt,
    mem::size_of,
    ops::{AddAssign, Deref},
};

use crate::engine::io::ConstEncodedSize;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageOffset(pub usize);

impl AddAssign for PageOffset {
    fn add_assign(&mut self, other: Self) {
        self.0.add_assign(other.0);
    }
}

impl ConstEncodedSize for PageOffset {
    fn encoded_size() -> usize {
        size_of::<usize>()
    }
}

impl fmt::Display for PageOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_assign() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = PageOffset(1);
        test += PageOffset(2);
        assert_eq!(test, PageOffset(3));
        Ok(())
    }
}
