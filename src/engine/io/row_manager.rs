//! This is the next level above a page manager but is still a naive interface.
//!
//! The goal of the row manager is to provide an interative interface over a table's pages. If that ends up being too complicated I'll break it down more.

// to insert a row

use super::super::super::constants::BuiltinSqlTypes;
use super::super::objects::{Attribute, Table};
use super::PageManager;

use bytes::BytesMut;
use thiserror::Error;

#[derive(Debug)]
pub struct RowManager {
    page_manager: PageManager,
}

impl RowManager {
    fn new(page_manager: PageManager) -> RowManager {
        RowManager { page_manager }
    }

    fn insert_row(
        tran_id: u64,
        table: Table,
        data: Vec<Option<BuiltinSqlTypes>>,
    ) -> Result<(), RowManagerError> {
        let row_buffer = BytesMut::new();

        //Assemble the data into a row -> row length.
        //  Option is used to figure out the null
        //Scan forward for a page with enough free space for the row plus the pointers
        //  If no page found, add a new one on the end
        //  Make the page skeleton format
        //Take the page and add the row in
        //Save the page

        Err(RowManagerError::NotImplemented())
    }
}

#[derive(Error, Debug)]
pub enum RowManagerError {
    #[error("I should develop more")]
    NotImplemented(),
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
