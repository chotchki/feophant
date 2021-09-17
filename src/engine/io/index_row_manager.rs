//!This is a layer above the index manager to handle finding active rows based on an index

use std::sync::Arc;

use crate::engine::objects::{Index, SqlTuple, Table};

use super::{IndexManager, VisibleRowManager};

pub struct IndexRowManager {
    index_manager: IndexManager,
    vis_row_man: VisibleRowManager,
}

impl IndexRowManager {
    pub fn new(index_manager: IndexManager, vis_row_man: VisibleRowManager) -> IndexRowManager {
        IndexRowManager {
            index_manager,
            vis_row_man,
        }
    }

    pub fn get_rows_matching_key(
        table: Arc<Table>,
        index: Arc<Index>,
        key: SqlTuple,
    ) -> Vec<SqlTuple> {
        todo!("Don't call me yet!")
    }
}
