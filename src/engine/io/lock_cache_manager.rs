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
            Some(s) => Ok(s.clone()),
            None => {
                //Cache miss, let's make the RwLock and drop the mutex
                let page_lock = Arc::new(RwLock::new(None));
                let mut page_lock_write = page_lock.write().await;
                cache.put((page_id, offset), page_lock.clone());
                drop(cache);

                //Now we can load the underlying page without blocking everyone
                if let Some(s) = self.file_manager.get_page(&page_id, &offset).await? {
                    page_lock_write.replace(s);
                }
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::{constants::PAGE_SIZE, engine::io::page_formats::PageType};

    use super::*;

    fn get_test_page(fill: u8) -> BytesMut {
        let mut test_page = BytesMut::with_capacity(PAGE_SIZE as usize);
        let free_space = vec![fill; PAGE_SIZE as usize];
        test_page.extend_from_slice(&free_space);
        test_page
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let lm = LockCacheManager::new(fm);

        let page_id = PageId {
            resource_key: Uuid::new_v4(),
            page_type: PageType::Data,
        };

        let first_offset = lm.get_offset(page_id).await?;
        assert_eq!(first_offset, PageOffset(0));

        let first_handle = lm.get_page(page_id, first_offset).await?;
        assert_eq!(first_handle.as_ref(), None);
        drop(first_handle);

        let mut second_handle = lm.get_page_for_update(page_id, first_offset).await?;
        assert_eq!(second_handle.as_ref(), None);

        let page = get_test_page(1);
        second_handle.replace(page);

        lm.update_page(page_id, first_offset, second_handle).await?;

        let third_handle = lm.get_page(page_id, first_offset).await?;
        let page2 = get_test_page(1);
        assert_eq!(third_handle.as_ref(), Some(&page2));

        let fourth_offset = lm.get_offset(page_id).await?;
        assert_eq!(fourth_offset, PageOffset(1));

        let mut fourth_handle = lm.get_page_for_update(page_id, fourth_offset).await?;
        assert_eq!(fourth_handle.as_ref(), None);

        let page3 = get_test_page(2);
        fourth_handle.replace(page3);
        lm.add_page(page_id, fourth_offset, fourth_handle).await?;

        let mut fifth_handle = lm.get_page_for_update(page_id, fourth_offset).await?;
        let fifth_page = fifth_handle
            .as_mut()
            .ok_or(LockCacheManagerError::PageMissing())?;
        fifth_page.clear();

        let page4 = get_test_page(3);
        fifth_page.extend_from_slice(&page4[0..page4.len()]);
        lm.update_page(page_id, fourth_offset, fifth_handle).await?;

        let mut sixth_handle = lm.get_page_for_update(page_id, fourth_offset).await?;
        let sixth_page = sixth_handle
            .as_mut()
            .ok_or(LockCacheManagerError::PageMissing())?;

        let test_page = get_test_page(3);
        assert_eq!(sixth_page, &test_page);

        Ok(())
    }
}
