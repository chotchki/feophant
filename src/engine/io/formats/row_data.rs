//! Encodes / decodes a row into a byte array based on the supplied specification
//! Format from here: https://www.postgresql.org/docs/current/storage-page-layout.html
//! As always I'm only implementing what I need and will extend once I need more
use super::super::super::super::constants::BuiltinSqlTypes;
use super::super::super::objects::{Table, TransactionId};
use super::InfoMask;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

pub struct RowData {
    min: TransactionId,
    max: Option<TransactionId>,
    user_data: Vec<Option<BuiltinSqlTypes>>,
}

impl RowData {
    pub fn new(
        min: TransactionId,
        max: Option<TransactionId>,
        user_data: Vec<Option<BuiltinSqlTypes>>,
    ) -> RowData {
        RowData {
            min,
            max,
            user_data,
        }
    }

    pub fn serialize(&self, table: Table) -> Bytes {
        let mut buffer = BytesMut::new();
        buffer.put_u64_le(self.min.get_u64());
        buffer.put_u64_le(self.max.unwrap_or(TransactionId::new(0)).get_u64());

        let mut mask = InfoMask::empty();
        for i in self.user_data.iter() {
            match i {
                Some(_) => {}
                None => {
                    mask = InfoMask::HAS_NULL;
                }
            }
        }
        buffer.put_u8(mask.bits());

        buffer.freeze()
    }

    pub fn parse(table: Table, row_data: Bytes) -> Result<RowData, RowDataError> {
        Err(RowDataError::Unknown())
    }
}
/*
    min: TransactionId,
    max: Option<TransactionId>, //0 is spec
    info_mask: InfoMask,        //At the moment only good for if there are null columns
    null_fields: Option<Bytes>,
    user_data: Vec<BuiltinSqlTypes>,
*/

#[derive(Debug, Error)]
pub enum RowDataError {
    #[error("No idea")]
    Unknown(),
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_row_data_roundtrip() {}
}
