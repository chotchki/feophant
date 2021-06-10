//! A simple wrapper around a primitive so I can play with transaction id sizes.
use std::convert::TryFrom;
use std::fmt;
use std::num::TryFromIntError;
use thiserror::Error;

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn new(value: u64) -> TransactionId {
        TransactionId(value)
    }

    pub fn get_u64(&self) -> u64 {
        self.0
    }

    pub fn checked_add(self, rhs: usize) -> Result<TransactionId, TransactionIdError> {
        let rhs_u64 = u64::try_from(rhs)?;
        match self.0.checked_add(rhs_u64) {
            Some(s) => Ok(TransactionId::new(s)),
            None => Err(TransactionIdError::LimitReached()),
        }
    }

    pub fn checked_sub(self, rhs: TransactionId) -> Result<usize, TransactionIdError> {
        match self.0.checked_sub(rhs.get_u64()) {
            Some(s) => Ok(usize::try_from(s)?),
            None => Err(TransactionIdError::Underflow(self, rhs)),
        }
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Error, Debug)]
pub enum TransactionIdError {
    #[error("Unable to convert usize to u64")]
    ConversionError(#[from] TryFromIntError),
    #[error("Underflow on subtraction left: {0} right: {1}")]
    Underflow(TransactionId, TransactionId),
    #[error("Exceeded counter limit, at the moment your only option is reimporting the database.")]
    LimitReached(),
}
