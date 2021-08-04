//! Eventually this will handle reading / writing pages from disk but for now, hashmap + vector!
//!
//! Was stupid with the implementation, should have supported an append api only since vector only works that way
use super::page_formats::{PageOffset, UInt12, UInt12Error};
use async_stream::stream;
use bytes::Bytes;
use futures::stream::Stream;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::vec::Vec;
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct IOManager {
    data: Arc<RwLock<HashMap<Uuid, Vec<Bytes>>>>, //Yes this is the naive implementation
}

impl IOManager {
    pub fn new() -> IOManager {
        IOManager {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    //Extracted the actual logic into its own method so I could implement stream
    async fn get_page_int(
        d: Arc<RwLock<HashMap<Uuid, Vec<Bytes>>>>,
        resource_key: &Uuid,
        offset: &PageOffset,
    ) -> Option<Bytes> {
        let read_lock = d.read().await;

        let value = read_lock.get(resource_key)?;

        let page = value.get(offset.0)?;
        let copy = page.slice(0..page.len());
        Some(copy)
    }

    pub async fn get_page(&self, resource_key: &Uuid, offset: &PageOffset) -> Option<Bytes> {
        IOManager::get_page_int(self.data.clone(), resource_key, offset).await
    }

    pub fn get_stream(&self, resource_key: Uuid) -> impl Stream<Item = Bytes> {
        let data = self.data.clone();
        stream! {
            let mut page_num = PageOffset(0);
            loop {
                match IOManager::get_page_int(data.clone(), &resource_key, &page_num).await {
                    Some(p) => {
                        yield p;
                    },
                    None => {
                        return ();
                    }
                }
                page_num += PageOffset(1);
            }
        }
    }

    pub async fn add_page(
        &self,
        resource_key: &Uuid,
        page: Bytes,
    ) -> Result<PageOffset, IOManagerError> {
        let size = UInt12::try_from(page.len() - 1)?;
        if size != UInt12::max() {
            return Err(IOManagerError::InvalidPageSize(PageOffset(page.len())));
        }

        let mut write_lock = self.data.write().await;

        match write_lock.get_mut(resource_key) {
            Some(v) => {
                let offset = v.len();
                v.push(page);
                return Ok(PageOffset(offset));
            }
            None => {
                let vec_holder = vec![page];
                write_lock.insert(*resource_key, vec_holder);
                return Ok(PageOffset(0));
            }
        }
    }

    pub async fn update_page(
        &self,
        resource_key: &Uuid,
        page: Bytes,
        offset: &PageOffset,
    ) -> Result<(), IOManagerError> {
        let size = UInt12::try_from(page.len() - 1)?;
        if size != UInt12::max() {
            return Err(IOManagerError::InvalidPageSize(PageOffset(page.len())));
        }

        let mut write_lock = self.data.write().await;

        let value = write_lock.get_mut(resource_key);
        if value.is_none() {
            return Err(IOManagerError::NoSuchResource(*resource_key));
        }

        let existing_value = value.unwrap();
        if existing_value.len() < offset.0 {
            return Err(IOManagerError::InvalidPage(*offset));
        }
        existing_value[offset.0] = page;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum IOManagerError {
    #[error("No such resource {0}")]
    NoSuchResource(Uuid),
    #[error("Invalid Page number {0}")]
    InvalidPage(PageOffset),
    #[error("Invalid Page size of {0}")]
    InvalidPageSize(PageOffset),
    #[error(transparent)]
    UInt12Error(#[from] UInt12Error),
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
        for _ in 0..=4095 {
            buf.put_u8(data);
        }
        buf.freeze()
    }

    #[test]
    fn test_get_and_put() {
        let buf_frozen = get_bytes(1);

        let pm = IOManager::new();
        let table = Arc::new(Table::new(Uuid::new_v4(), "test".to_string(), Vec::new()));

        aw!(pm.add_page(&table.id, buf_frozen.clone()));
        let check = aw!(pm.get_page(&table.id, &PageOffset(0))).unwrap();
        assert_eq!(check, buf_frozen.clone());
    }

    #[test]
    fn test_edit_page() {
        let buf_1 = get_bytes(1);
        let buf_2 = get_bytes(2);

        let pm = IOManager::new();
        let table = Arc::new(Table::new(Uuid::new_v4(), "test".to_string(), Vec::new()));

        aw!(pm.add_page(&table.id, buf_1.clone()));
        aw!(pm.add_page(&table.id, buf_1.clone()));
        let check_1 = aw!(pm.get_page(&table.id, &PageOffset(1))).unwrap();
        assert_eq!(buf_1.clone(), check_1.clone());

        aw!(pm.update_page(&table.id, buf_2.clone(), &PageOffset(1)));
        let check_2 = aw!(pm.get_page(&table.id, &PageOffset(1))).unwrap();
        assert_eq!(buf_2.clone(), check_2.clone());
        assert_ne!(buf_1.clone(), check_2.clone());
    }
}
