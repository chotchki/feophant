//! The current goal of the row manager is to provide an interative interface over a table's pages.
//!
//! It will provide a raw scan over a table, insert and update.
//!
//! NO LOCKING or transaction control yet. I did implement it at the IO layer but its probably wrong.

use super::super::super::constants::BuiltinSqlTypes;
use super::super::objects::{Attribute, Table, TransactionId};
use super::page_formats::{PageData, PageDataError};
use super::row_formats::RowData;
use super::{IOManager, IOManagerError};
use std::slice::Iter;
use std::sync::Arc;

use bytes::BytesMut;
use thiserror::Error;

#[derive(Debug)]
pub struct RowManager {
    io_manager: IOManager,
}

impl RowManager {
    fn new(io_manager: IOManager) -> RowManager {
        RowManager { io_manager }
    }

    pub async fn insert_row(
        &mut self,
        current_tran_id: TransactionId,
        table: Arc<Table>,
        row: RowData,
    ) -> Result<(), RowManagerError> {
        let row_len = row.serialize().len();

        let mut page_num = 0;
        loop {
            let page_bytes = self.io_manager.get_page(table.clone(), page_num).await;
            match page_bytes {
                Some(p) => {
                    let mut page = PageData::parse(table.clone(), p)?;
                    if page.can_fit(row_len) {
                        page.store(row)?;
                        let new_page_bytes = page.serialize();
                        self.io_manager
                            .update_page(table, new_page_bytes, page_num)
                            .await?;
                        return Ok(());
                    } else {
                        continue;
                    }
                }
                None => {
                    let mut new_page = PageData::new(table.clone());
                    new_page.store(row)?; //TODO Will NOT handle overly large rows
                    self.io_manager.add_page(table, new_page.serialize()).await;
                    return Ok(());
                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum RowManagerError {
    #[error("Page Data Parse Error")]
    PageDataParseError(#[from] PageDataError),
    #[error("IO Manager Error")]
    IOManagerError(#[from] IOManagerError),
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::super::super::objects::Table;
    use super::*;
    use bytes::{BufMut, BytesMut};
    use uuid::Uuid;

    //Async testing help can be found here: https://blog.x5ff.xyz/blog/async-tests-tokio-rust/
    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }
}
