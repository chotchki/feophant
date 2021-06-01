//! A simple wrapper around a primitive so I can play with transaction id sizes.

pub struct TransactionId(u64);

impl TransactionId {
    pub fn new(value: u64) -> TransactionId {
        TransactionId(value)
    }
}
