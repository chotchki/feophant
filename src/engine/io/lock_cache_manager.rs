use super::{
    page_formats::{PageId, PageOffset},
    FileManager, FileManagerError,
};
use bytes::BytesMut;
use lru::LruCache;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

#[derive(Clone, Debug)]
pub struct LockCacheManager {
    //TODO I don't like these massive single hashes protected with a single lock
    //     Long term I need to make a fixed hashmap and evict them myself.
    //     Holding on this since I might be able to work around it
    cache: Arc<Mutex<LruCache<(PageId, PageOffset), Arc<RwLock<Option<BytesMut>>>>>>,
    file_manager: Arc<FileManager>,
}

impl LockCacheManager {
    pub fn new(file_manager: Arc<FileManager>) -> LockCacheManager {
        LockCacheManager {
            //TODO The unbounded nature of the cache worries me. I think I'll have to manage its eviction carefully
            cache: Arc::new(Mutex::new(LruCache::unbounded())),
            file_manager,
        }
    }

    pub async fn get_offset(&self, page_id: PageId) -> Result<PageOffset, LockCacheManagerError> {
        Ok(self.file_manager.get_offset(&page_id).await?)
    }

    pub async fn get_page(
        &self,
        page_id: PageId,
        offset: PageOffset,
    ) -> Result<OwnedRwLockReadGuard<Option<BytesMut>>, LockCacheManagerError> {
        Ok(self
            .get_page_internal(page_id, offset)
            .await?
            .read_owned()
            .await)
    }

    pub async fn get_page_for_update(
        &self,
        page_id: PageId,
        offset: PageOffset,
    ) -> Result<OwnedRwLockWriteGuard<Option<BytesMut>>, LockCacheManagerError> {
        Ok(self
            .get_page_internal(page_id, offset)
            .await?
            .write_owned()
            .await)
    }

    async fn get_page_internal(
        &self,
        page_id: PageId,
        offset: PageOffset,
    ) -> Result<Arc<RwLock<Option<BytesMut>>>, LockCacheManagerError> {
        let mut cache = self.cache.lock().await;
        match cache.get(&(page_id, offset)) {
            Some(s) => return Ok(s.clone()),
            None => {
                //Cache miss, let's make the RwLock and drop the mutex
                let page_lock = Arc::new(RwLock::new(None));
                let mut page_lock_write = page_lock.write().await;
                cache.put((page_id, offset), page_lock.clone());
                drop(cache);

                //Now we can load the underlying page without blocking everyone
                match self.file_manager.get_page(&page_id, &offset).await? {
                    Some(s) => {
                        page_lock_write.replace(s);
                    }
                    None => {}
                };
                drop(page_lock_write);

                Ok(page_lock)
            }
        }
    }

    pub async fn update_page(
        &self,
        page_id: PageId,
        offset: PageOffset,
        guard: OwnedRwLockWriteGuard<Option<BytesMut>>,
    ) -> Result<(), LockCacheManagerError> {
        let page = match guard.as_ref() {
            Some(s) => s.clone(),
            None => {
                return Err(LockCacheManagerError::PageMissing());
            }
        };
        Ok(self
            .file_manager
            .update_page(&page_id, &offset, page.freeze())
            .await?)
    }

    pub async fn add_page(
        &self,
        page_id: PageId,
        offset: PageOffset,
        guard: OwnedRwLockWriteGuard<Option<BytesMut>>,
    ) -> Result<(), LockCacheManagerError> {
        let page = match guard.as_ref() {
            Some(s) => s.clone(),
            None => {
                return Err(LockCacheManagerError::PageMissing());
            }
        };
        Ok(self
            .file_manager
            .add_page(&page_id, &offset, page.freeze())
            .await?)
    }
}

#[derive(Debug, Error)]
pub enum LockCacheManagerError {
    #[error(transparent)]
    FileManagerError(#[from] FileManagerError),
    #[error("Cannot update a page without contents")]
    PageMissing(),
}
