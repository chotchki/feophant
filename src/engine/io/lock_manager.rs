//! Lock Manager provides a way to lock a page offset until all the readers and writers are released.
//!
//! TODO: Figure out if the write lock can be used to write back changed pages.

use std::{
    collections::{hash_map::RandomState, HashMap},
    sync::{
        atomic::{AtomicI8, Ordering},
        Arc, Weak,
    },
};

use tokio::sync::{RwLock, RwLockWriteGuard};

use super::page_formats::{PageId, PageOffset};

/// Every 10 write locks held will include a scan for dead weak refs and their removal.
/// TODO: Figure out if this makes sense or even if it should decend into the children.
///     Now that I think about this, I don't think a table's locks will EVER get removed.
///     Need to figure out descent.
const CLEANUP_RATE: i8 = 10;

#[derive(Debug, Clone)]
pub struct LockManager {
    locks: Arc<RwLock<HashMap<PageId, LockManagerEntry>>>,
    cleanup_count: Arc<AtomicI8>,
}

impl LockManager {
    pub fn new() -> LockManager {
        LockManager {
            locks: Arc::new(RwLock::new(HashMap::new())),
            cleanup_count: Arc::new(AtomicI8::new(0)),
        }
    }

    pub async fn get_lock(&self, page_id: &PageId, offset: &PageOffset) -> Arc<RwLock<u8>> {
        let lm = self.locks.read().await;

        match lm.get(page_id) {
            Some(s1) => s1.get_lock(offset).await,
            None => {
                drop(lm);
                self.insert_key(page_id, offset).await
            }
        }
    }

    async fn insert_key(&self, page_id: &PageId, offset: &PageOffset) -> Arc<RwLock<u8>> {
        let mut lm = self.locks.write().await;
        let lock = match lm.get(page_id) {
            Some(s3) => {
                //Got write lock and the entry is there already
                s3.get_lock(offset).await
            }
            None => {
                //Get failed, recreate
                let entry = LockManagerEntry::new();
                let new_lock = entry.get_lock(offset).await;
                lm.insert(*page_id, entry);
                new_lock
            }
        };

        self.cleanup(&mut lm).await;

        lock
    }

    async fn cleanup(
        &self,
        lm: &mut RwLockWriteGuard<'_, HashMap<PageId, LockManagerEntry, RandomState>>,
    ) {
        let clean_count = self.cleanup_count.fetch_add(1, Ordering::Relaxed);
        if clean_count > CLEANUP_RATE {
            //Fix once drain_filter shows up: https://github.com/rust-lang/rust/issues/59618
            let mut keys_to_remove = vec![];

            for (u, le) in lm.iter() {
                if le.len().await == 0 {
                    keys_to_remove.push(*u);
                }
            }

            for k in keys_to_remove {
                lm.remove(&k);
            }

            self.cleanup_count
                .fetch_sub(CLEANUP_RATE, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone)]
pub struct LockManagerEntry {
    inner_locks: Arc<RwLock<HashMap<PageOffset, Weak<RwLock<u8>>>>>,
    inner_cleanup_count: Arc<AtomicI8>,
}

impl LockManagerEntry {
    pub fn new() -> LockManagerEntry {
        LockManagerEntry {
            inner_locks: Arc::new(RwLock::new(HashMap::new())),
            inner_cleanup_count: Arc::new(AtomicI8::new(0)),
        }
    }

    pub async fn get_lock(&self, offset: &PageOffset) -> Arc<RwLock<u8>> {
        let le = self.inner_locks.read().await;

        match le.get(offset) {
            Some(s) => match s.upgrade() {
                Some(s1) => s1,
                None => {
                    //Weak failed, need to recreate
                    drop(le);
                    return self.insert_key(offset).await;
                }
            },
            None => {
                //Key doesn't exist need to create
                drop(le);
                return self.insert_key(offset).await;
            }
        }
    }

    pub async fn len(&self) -> usize {
        let le = self.inner_locks.read().await;
        le.len()
    }

    async fn insert_key(&self, offset: &PageOffset) -> Arc<RwLock<u8>> {
        let mut le = self.inner_locks.write().await;
        let lock = match le.get(offset) {
            Some(s2) => match s2.upgrade() {
                Some(s3) => {
                    //Got write lock and its magically there
                    s3
                }
                None => {
                    //Weak failed, recreate
                    let new_lock = Arc::new(RwLock::new(0));
                    le.insert(*offset, Arc::downgrade(&new_lock));
                    new_lock
                }
            },
            None => {
                //Get failed, recreate
                let new_lock = Arc::new(RwLock::new(0));
                le.insert(*offset, Arc::downgrade(&new_lock));
                new_lock
            }
        };
        self.cleanup(&mut le);

        lock
    }

    fn cleanup(&self, le: &mut RwLockWriteGuard<HashMap<PageOffset, Weak<RwLock<u8>>>>) {
        let inner_clean_count = self.inner_cleanup_count.fetch_add(1, Ordering::Relaxed);
        if inner_clean_count > CLEANUP_RATE {
            //Fix once drain_filter shows up: https://github.com/rust-lang/rust/issues/59618
            let mut keys_to_remove = vec![];

            for (po, w) in le.iter() {
                if w.strong_count() == 0 {
                    keys_to_remove.push(*po);
                }
            }

            for k in keys_to_remove {
                le.remove(&k);
            }

            self.inner_cleanup_count
                .fetch_sub(CLEANUP_RATE, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lock_manager_entries() -> Result<(), Box<dyn std::error::Error>> {
        let le = LockManagerEntry::new();

        //Get a lock twice for read and see if they are equal
        let lock1 = le.get_lock(&PageOffset(0)).await;
        let lock2 = le.get_lock(&PageOffset(0)).await;

        assert_eq!(Arc::as_ptr(&lock1), Arc::as_ptr(&lock2));
        assert_eq!(Arc::strong_count(&lock1), 2);

        //Get a bunch of locks and make sure they get cleaned up
        {
            let mut locks = vec![];
            for i in 0..100 {
                locks.push(le.get_lock(&PageOffset(i)).await);
            }
            assert_eq!(le.len().await, 100);
        }
        for i in 0..=CLEANUP_RATE as usize {
            le.get_lock(&PageOffset(i)).await;
        }
        assert!(le.len().await < 110);

        Ok(())
    }
}
