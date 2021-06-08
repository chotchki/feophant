//! Eventually this will handle reading / writing pages from disk but for now, hashmap + vector!
//!
//! Was stupid with the implementation, should have supported an append api only since vector only works that way
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec::Vec;
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::super::objects::Table;

#[derive(Debug)]
pub struct IOManager {
    data: RwLock<HashMap<Uuid, Vec<Bytes>>>, //Yes this is the naive implementation
}

impl IOManager {
    pub fn new() -> IOManager {
        IOManager {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_page(&self, table: Arc<Table>, offset: usize) -> Option<Bytes> {
        let read_lock = self.data.read().await;

        let value = read_lock.get(&table.id)?;

        let page = value.get(offset)?;
        let copy = page.slice(0..page.len());
        Some(copy)
    }

    pub async fn add_page(&self, table: Arc<Table>, page: Bytes) {
        let mut write_lock = self.data.write().await;

        match write_lock.get_mut(&table.id) {
            Some(v) => v.push(page),
            None => {
                let vec_holder = vec![page];
                write_lock.insert(table.id, vec_holder);
            }
        }
    }

    pub async fn update_page(
        &self,
        table: Arc<Table>,
        page: Bytes,
        offset: usize,
    ) -> Result<(), IOManagerError> {
        let mut write_lock = self.data.write().await;

        let value = write_lock.get_mut(&table.id);
        if value.is_none() {
            return Err(IOManagerError::NoSuchTable(table.name.clone()));
        }

        let existing_value = value.unwrap();
        if existing_value.len() < offset {
            return Err(IOManagerError::InvalidPage(offset));
        }
        existing_value[offset] = page;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum IOManagerError {
    #[error("No such table {0}")]
    NoSuchTable(String),
    #[error("Invalid Page number {0}")]
    InvalidPage(usize),
}

#[cfg(test)]
mod tests {
    #![allow(unused_must_use)]
    use super::super::super::objects::Table;
    use super::*;
    use bytes::{BufMut, BytesMut};

    //Async testing help can be found here: https://blog.x5ff.xyz/blog/async-tests-tokio-rust/
    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    fn get_bytes(data: u8) -> Bytes {
        let mut buf = BytesMut::with_capacity(4096);
        for _ in 0..4095 {
            buf.put_u8(data);
        }
        buf.freeze()
    }

    #[test]
    fn test_get_and_put() {
        let buf_frozen = get_bytes(1);

        let pm = IOManager::new();
        let table = Arc::new(Table::new("test".to_string(), Vec::new()));

        aw!(pm.add_page(table.clone(), buf_frozen.clone()));
        let check = aw!(pm.get_page(table, 0)).unwrap();
        assert_eq!(check, buf_frozen.clone());
    }

    #[test]
    fn test_edit_page() {
        let buf_1 = get_bytes(1);
        let buf_2 = get_bytes(2);

        let pm = IOManager::new();
        let table = Arc::new(Table::new("test".to_string(), Vec::new()));

        aw!(pm.add_page(table.clone(), buf_1.clone()));
        aw!(pm.add_page(table.clone(), buf_1.clone()));
        let check_1 = aw!(pm.get_page(table.clone(), 1)).unwrap();
        assert_eq!(buf_1.clone(), check_1.clone());

        aw!(pm.update_page(table.clone(), buf_2.clone(), 1));
        let check_2 = aw!(pm.get_page(table, 1)).unwrap();
        assert_eq!(buf_2.clone(), check_2.clone());
        assert_ne!(buf_1.clone(), check_2.clone());
    }
}
