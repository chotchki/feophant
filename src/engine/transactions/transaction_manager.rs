//! This is the interface to transaction visability (clog in postgres).
use super::{TransactionId, TransactionIdError, TransactionStatus};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
pub struct TransactionManager {
    tran_min: TransactionId, //Used to index the known transactions array
    known_trans: Arc<RwLock<Vec<TransactionStatus>>>,
}

impl TransactionManager {
    pub fn new() -> TransactionManager {
        let tran_min = TransactionId::new(1); //Must start at 1 since 0 is used for active rows
        let known_trans = Arc::new(RwLock::new(vec![TransactionStatus::Aborted])); //First transaction will be cancelled
        TransactionManager {
            tran_min,
            known_trans,
        }
    }

    pub async fn start_trans(&mut self) -> Result<TransactionId, TransactionManagerError> {
        let mut known_trans = self.known_trans.write().await;

        known_trans.push(TransactionStatus::InProgress);

        Ok(self.tran_min.checked_add(known_trans.len() - 1)?)
    }

    pub async fn get_status(
        &mut self,
        tran_id: TransactionId,
    ) -> Result<TransactionStatus, TransactionManagerError> {
        if tran_id < self.tran_min {
            return Err(TransactionManagerError::TooOld(tran_id, self.tran_min));
        }

        let known_trans = self.known_trans.read().await;

        if tran_id > self.tran_min.checked_add(known_trans.len())? {
            return Err(TransactionManagerError::InTheFuture(
                tran_id,
                self.tran_min,
                known_trans.len(),
            ));
        }

        let index = tran_id.checked_sub(self.tran_min)?;

        Ok(known_trans[index])
    }

    async fn update_trans(
        &mut self,
        tran_id: TransactionId,
        new_status: TransactionStatus,
    ) -> Result<(), TransactionManagerError> {
        if tran_id < self.tran_min {
            return Err(TransactionManagerError::TooOld(tran_id, self.tran_min));
        }

        let mut known_trans = self.known_trans.write().await;

        if tran_id > self.tran_min.checked_add(known_trans.len())? {
            return Err(TransactionManagerError::InTheFuture(
                tran_id,
                self.tran_min,
                known_trans.len(),
            ));
        }

        let index = tran_id.checked_sub(self.tran_min)?;

        if known_trans[index] != TransactionStatus::InProgress {
            return Err(TransactionManagerError::NotInProgress(
                tran_id,
                known_trans[index],
            ));
        }

        known_trans[index] = new_status;

        Ok(())
    }

    pub async fn commit_trans(
        &mut self,
        tran_id: TransactionId,
    ) -> Result<(), TransactionManagerError> {
        self.update_trans(tran_id, TransactionStatus::Commited)
            .await
    }

    pub async fn abort_trans(
        &mut self,
        tran_id: TransactionId,
    ) -> Result<(), TransactionManagerError> {
        self.update_trans(tran_id, TransactionStatus::Aborted).await
    }

    //TODO work on figuring out how to save / load this
    pub fn serialize() {}

    pub fn parse() {}
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Error, Debug)]
pub enum TransactionManagerError {
    #[error(transparent)]
    TransactionIdError(#[from] TransactionIdError),
    #[error("Transaction Id {0} too low compared to {1}")]
    TooOld(TransactionId, TransactionId),
    #[error("Transaction Id {0} exceeds the min {1} and size {2}")]
    InTheFuture(TransactionId, TransactionId, usize),
    #[error("Transaction Id {0} not in progress, found {1}")]
    NotInProgress(TransactionId, TransactionStatus),
}

#[cfg(test)]
mod tests {
    #![allow(unused_must_use)]
    use super::*;

    #[tokio::test]
    async fn tran_man_statuses() -> Result<(), Box<dyn std::error::Error>> {
        let mut tm = TransactionManager::new();
        let tran1 = tm.start_trans().await?;
        let tran2 = tm.start_trans().await?;

        assert_ne!(tran1, tran2);
        assert!(tran1 < tran2);

        assert_eq!(tm.get_status(tran1).await?, TransactionStatus::InProgress);
        assert_eq!(tm.get_status(tran2).await?, TransactionStatus::InProgress);

        assert!(tm.commit_trans(tran1).await.is_ok());
        assert!(tm.commit_trans(tran1).await.is_err());

        assert_eq!(tm.get_status(tran1).await?, TransactionStatus::Commited);
        assert_eq!(tm.get_status(tran2).await?, TransactionStatus::InProgress);

        assert!(tm.abort_trans(tran2).await.is_ok());
        assert!(tm.abort_trans(tran2).await.is_err());

        assert_eq!(tm.get_status(tran1).await?, TransactionStatus::Commited);
        assert_eq!(tm.get_status(tran2).await?, TransactionStatus::Aborted);

        Ok(())
    }
}
