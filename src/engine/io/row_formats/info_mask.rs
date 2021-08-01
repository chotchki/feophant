//! Bit flags for things such as nullable.
//! See here: https://doxygen.postgresql.org/htup__details_8h_source.html

use std::mem::size_of;

use crate::engine::io::ConstEncodedSize;

bitflags! {
    pub struct InfoMask: u8 {
        const HAS_NULL = 0b00000001;
    }
}

impl ConstEncodedSize for InfoMask {
    fn encoded_size() -> usize {
        size_of::<u8>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoded_size() {
        assert_eq!(1, InfoMask::encoded_size())
    }
}
