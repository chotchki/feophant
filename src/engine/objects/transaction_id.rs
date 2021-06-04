//! A simple wrapper around a primitive so I can play with transaction id sizes.

#[derive(Copy, Clone)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn new(value: u64) -> TransactionId {
        TransactionId(value)
    }

    pub fn get_u64(&self) -> u64 {
        self.0
    }
}
