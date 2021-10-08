use std::sync::Arc;

use super::file_manager::{FileManager, FileManagerError};
use crate::engine::io::page_formats::{PageId, PageOffset};
use moka::future::Cache;
use thiserror::Error;
use tokio::sync::RwLock;

/// The LockManager is used for cooperative access to pages in the system.
///
/// Before accessing the I/O layer you must get a read or write lock on
/// the page you need to access. Only AFTER you have the lock you should
/// ask for the page.
///
/// TODO: Find a way that I can do this in a type enforcing way.

#[derive(Clone)]
pub struct LockManager {
    file_manager: Arc<FileManager>,
    locks: Cache<(PageId, PageOffset), Arc<RwLock<()>>>,
}

impl LockManager {
    pub fn new(file_manager: Arc<FileManager>) -> LockManager {
        LockManager {
            file_manager,
            locks: Cache::new(1000),
        }
    }

    pub async fn get_offset(&self, page_id: PageId) -> Result<PageOffset, LockManagerError> {
        Ok(self.file_manager.get_offset(&page_id).await?)
    }

    pub async fn get_offset_non_zero(
        &self,
        page_id: PageId,
    ) -> Result<PageOffset, LockManagerError> {
        let mut offset = PageOffset(0);
        while offset == PageOffset(0) {
            offset = self.file_manager.get_offset(&page_id).await?;
        }
        Ok(offset)
    }

    pub async fn get_lock(&self, page_id: PageId, offset: PageOffset) -> Arc<RwLock<()>> {
        self.locks
            .get_or_insert_with(
                (page_id, offset),
                async move { Arc::new(RwLock::const_new(())) },
            )
            .await
    }
}

#[derive(Debug, Error)]
pub enum LockManagerError {
    #[error(transparent)]
    FileManagerError(#[from] FileManagerError),
}
