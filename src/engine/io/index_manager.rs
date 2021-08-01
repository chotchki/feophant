//! Start with a single bucket

//! Insert key and pointer to record
//!     read root
//!         search through buckets,

use uuid::Uuid;

use super::page_formats::ItemIdData;
use super::IOManager;
use crate::engine::objects::{Index, SqlTuple};

//TODO Support something other than btrees
//TODO Support searching on a non primary index column

#[derive(Clone, Debug)]
pub struct IndexManager {
    io_manager: IOManager,
}

impl IndexManager {
    pub fn new(io_manager: IOManager) -> IndexManager {
        IndexManager { io_manager }
    }

    fn add(index: Uuid, key: SqlTuple, item_ptr: ItemIdData) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_example() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
