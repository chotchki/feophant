//! Start with a single bucket

//! Insert key and pointer to record
//!     read root
//!         search through buckets,

//TODO Support something other than btrees

#[derive(Clone, Debug)]
pub struct IndexManager {
    io_manager: IOManager,
}

impl IndexManager {
    pub fn new(io_manager: IOManager) -> IndexManager {
        IndexManager { io_manager }
    }

    fn add(index: Index, key: Vec<BuiltInTypes>, item_ptr: ItemIdData) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_net_frames_poorly() {
        NetworkFrame::authentication_ok();
        NetworkFrame::ready_for_query();
        NetworkFrame::error_response(
            PgErrorLevels::Error,
            PgErrorCodes::SystemError,
            "test".to_string(),
        );
        assert!(true);
    }
}
