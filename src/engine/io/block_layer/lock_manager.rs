use std::sync::Arc;

use crate::engine::io::page_formats::{PageId, PageOffset};
use moka::future::Cache;
use thiserror::Error;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// The LockManager is used for cooperative access to pages in the system.
///
/// Before accessing the I/O layer you must get a read or write lock on
/// the page you need to access. Only AFTER you have the lock you should
/// ask for the page.
#[derive(Clone)]
pub struct LockManager {
    locks: Cache<(PageId, PageOffset), Arc<RwLock<(PageId, PageOffset)>>>,
}

impl LockManager {
    pub fn new() -> LockManager {
        LockManager {
            locks: Cache::new(1000),
        }
    }

    async fn get_lock(
        &self,
        page_id: PageId,
        offset: PageOffset,
    ) -> Arc<RwLock<(PageId, PageOffset)>> {
        self.locks
            .get_or_insert_with((page_id, offset), async move {
                Arc::new(RwLock::const_new((page_id, offset)))
            })
            .await
    }

    pub async fn read(
        &self,
        page_id: PageId,
        offset: PageOffset,
    ) -> OwnedRwLockReadGuard<(PageId, PageOffset)> {
        self.get_lock(page_id, offset).await.read_owned().await
    }

    pub async fn write(
        &self,
        page_id: PageId,
        offset: PageOffset,
    ) -> OwnedRwLockWriteGuard<(PageId, PageOffset)> {
        self.get_lock(page_id, offset).await.write_owned().await
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Error)]
pub enum LockManagerError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_locking() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        //todo!("Figure out the new model");

        Ok(())
    }
}
