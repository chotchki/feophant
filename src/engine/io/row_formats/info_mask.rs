//! Bit flags for things such as nullable.
//! See here: https://doxygen.postgresql.org/htup__details_8h_source.html

bitflags! {
    pub struct InfoMask: u8 {
        const HAS_NULL = 0b00000001;
    }
}
