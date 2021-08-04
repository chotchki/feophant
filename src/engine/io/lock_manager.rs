//! Thought process here, I implement a lock manager that you request a uuid + offset (effectively a page) for reading / writing.
//!
//! Lock manager done, need some headache inducing unit tests and a cleanup method. Right now I'm thinking of cleanup after every
//! so many write locks held.

use std::{
    collections::{hash_map::RandomState, HashMap},
    sync::{
        atomic::{AtomicI8, Ordering},
        Arc, Weak,
    },
};

use tokio::sync::{RwLock, RwLockWriteGuard};
use uuid::Uuid;

use super::page_formats::PageOffset;

/// Every 10 write locks held will include a scan for dead weak refs and their removal.
const CLEANUP_RATE: i8 = 10;

#[derive(Debug, Clone)]
pub struct LockManager {
    locks: Arc<RwLock<HashMap<Uuid, LockManagerEntry>>>,
    cleanup_count: Arc<AtomicI8>,
}

impl LockManager {
    pub fn new() -> LockManager {
        LockManager {
            locks: Arc::new(RwLock::new(HashMap::new())),
            cleanup_count: Arc::new(AtomicI8::new(0)),
        }
    }

    pub async fn get_lock(&self, resource_key: &Uuid, offset: &PageOffset) -> Arc<RwLock<u8>> {
        let lm = self.locks.read().await;

        match lm.get(resource_key) {
            Some(s1) => {
                return s1.get_lock(&offset).await;
            }
            None => {
                //Weak failed, need to recreate
                drop(lm);
                return self.insert_key(resource_key, offset).await;
            }
        }
    }

    async fn insert_key(&self, resource_key: &Uuid, offset: &PageOffset) -> Arc<RwLock<u8>> {
        let mut lm = self.locks.write().await;
        let lock = match lm.get(resource_key) {
            Some(s3) => {
                //Got write lock and the entry is there already
                s3.get_lock(offset).await
            }
            None => {
                //Get failed, recreate
                let entry = LockManagerEntry::new();
                let new_lock = entry.get_lock(offset).await;
                lm.insert(*resource_key, entry);
                new_lock
            }
        };

        self.cleanup(&mut lm);

        lock
    }

    async fn cleanup(
        &self,
        lm: &mut RwLockWriteGuard<'_, HashMap<Uuid, LockManagerEntry, RandomState>>,
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
                Some(s1) => {
                    return s1;
                }
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
