//! Provides an incrementing sequence based counter that is always 64-bit
use super::super::engine::objects::TransactionId;

use atomic_counter::{AtomicCounter, ConsistentCounter};
use std::convert::TryInto;
use std::num::TryFromIntError;
use thiserror::Error;

pub struct TransactionGenerator {
    offset: u64,
    counter: ConsistentCounter,
}

impl TransactionGenerator {
    pub fn new(offset: u64) -> TransactionGenerator {
        TransactionGenerator {
            offset,
            counter: ConsistentCounter::new(0),
        }
    }

    pub fn next(&self) -> Result<TransactionId, TransactionGeneratorError> {
        let next: u64 = self
            .counter
            .inc()
            .try_into()
            .map_err(TransactionGeneratorError::ConversionError)?;
        match self.offset.checked_add(next) {
            Some(s) => Ok(TransactionId::new(s)),
            None => Err(TransactionGeneratorError::LimitReached()),
        }
    }
}

#[derive(Error, Debug)]
pub enum TransactionGeneratorError {
    #[error("Could not convert usize to u64, you must have a super fancy computer!")]
    ConversionError(#[from] TryFromIntError),
    #[error("Exceeded counter limit, restart server to fix!")]
    LimitReached(),
}
