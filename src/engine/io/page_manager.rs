//! Eventually this will handle reading / writing pages from disk but for now, hashmap + vector!
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc,RwLock};
use std::vec::Vec;
use thiserror::Error;
use uuid::Uuid;

use super::super::objects::PgTable;

const PAGE_SIZE: usize = 4096; //4KB Pages

pub struct PageManager {
    data: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Vec<Bytes>>>>>> //Yes this is the naive implementation
}

impl PageManager {
    pub fn new() -> PageManager {
        PageManager {
            data: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    pub fn get_page(table: PgTable, offset: usize) -> Result<Bytes, PageManagerError>{
        Err(PageManagerError::NotImplemented())
    }

    pub fn put_page(table: PgTable, offset: usize) -> Result<Bytes, PageManagerError> {
        Err(PageManagerError::NotImplemented())
    }
}

#[derive(Debug, Error)]
pub enum PageManagerError {
    #[error("I should implement this")]
    NotImplemented()
}