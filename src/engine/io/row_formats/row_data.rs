//! Encodes / decodes a row into a byte array based on the supplied specification
//! Format from here: https://www.postgresql.org/docs/current/storage-page-layout.html
//! As always I'm only implementing what I need and will extend once I need more
//!
//! TODO Need to chew on if I should split the meta data and user data between two types
//!
use super::super::super::super::constants::{BuiltinSqlTypes, DeserializeTypes, SqlTypeError};
use super::super::super::objects::Table;
use super::super::super::transactions::TransactionId;
use super::{InfoMask, ItemPointer, ItemPointerError, NullMask};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
use std::mem;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct RowData {
    table: Arc<Table>,
    pub min: TransactionId,
    pub max: Option<TransactionId>,
    pub item_pointer: ItemPointer,
    pub user_data: Vec<Option<BuiltinSqlTypes>>,
}

impl RowData {
    pub fn new(
        table: Arc<Table>,
        min: TransactionId,
        max: Option<TransactionId>,
        item_pointer: ItemPointer,
        user_data: Vec<Option<BuiltinSqlTypes>>,
    ) -> Result<RowData, RowDataError> {
        if table.attributes.len() != user_data.len() {
            return Err(RowDataError::TableRowSizeMismatch(
                table.attributes.len(),
                user_data.len(),
            ));
        }
        for (data, column) in user_data.iter().zip(table.attributes.clone()) {
            match data {
                Some(d) => {
                    if !d.type_matches(column.sql_type) {
                        return Err(RowDataError::TableRowTypeMismatch(
                            d.clone(),
                            column.sql_type,
                        ));
                    }
                }
                None => {} //TODO Should handle whether is null even allowed, will get there
            }
        }

        Ok(RowData {
            table,
            min,
            max,
            item_pointer,
            user_data,
        })
    }

    pub fn serialize(&self) -> Bytes {
        let mut buffer = BytesMut::new();
        buffer.put_u64_le(self.min.get_u64());
        buffer.put_u64_le(self.max.unwrap_or(TransactionId::new(0)).get_u64());

        buffer.put(self.item_pointer.serialize());

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

        let nulls = NullMask::serialize(&self.user_data);
        buffer.put(nulls);

        for data in &self.user_data {
            if data.is_none() {
                continue;
            }

            let data_bytes = data.as_ref().unwrap().serialize();
            buffer.extend_from_slice(&data_bytes);
        }

        buffer.freeze()
    }

    pub fn parse(table: Arc<Table>, mut row_buffer: impl Buf) -> Result<RowData, RowDataError> {
        if row_buffer.remaining() < mem::size_of::<TransactionId>() {
            return Err(RowDataError::MissingMinData(
                mem::size_of::<TransactionId>(),
                row_buffer.remaining(),
            ));
        }
        let min = TransactionId::new(row_buffer.get_u64_le());

        if row_buffer.remaining() < mem::size_of::<TransactionId>() {
            return Err(RowDataError::MissingMaxData(
                mem::size_of::<TransactionId>(),
                row_buffer.remaining(),
            ));
        }
        let max_temp = row_buffer.get_u64_le();
        let max = match max_temp {
            0 => None,
            _ => Some(TransactionId::new(max_temp)),
        };

        let item_pointer = ItemPointer::parse(&mut row_buffer)?;

        let null_mask = RowData::get_null_mask(table.clone(), &mut row_buffer)?;

        let mut user_data = vec![];
        for (column, mask) in table.attributes.iter().zip(null_mask.iter()) {
            if *mask {
                user_data.push(None);
            } else {
                user_data.push(Some(BuiltinSqlTypes::deserialize(
                    column.sql_type,
                    &mut row_buffer,
                )?));
            }
        }

        RowData::new(table, min, max, item_pointer, user_data)
    }

    //Gets the null mask, if it doesn't exist it will return a vector of all not nulls
    fn get_null_mask(
        table: Arc<Table>,
        mut row_buffer: impl Buf,
    ) -> Result<Vec<bool>, RowDataError> {
        if row_buffer.remaining() < mem::size_of::<InfoMask>() {
            return Err(RowDataError::MissingInfoMaskData(
                mem::size_of::<TransactionId>(),
                row_buffer.remaining(),
            ));
        }

        let mask = InfoMask::from_bits_truncate(row_buffer.get_u8()); //Ignoring unused bits
        if !mask.contains(InfoMask::HAS_NULL) {
            return Ok(vec![false; table.attributes.len()]);
        }

        let columns_rounded = (table.attributes.len() + 7) / 8; //From https://users.rust-lang.org/t/solved-rust-round-usize-to-nearest-multiple-of-8/25549
        if row_buffer.remaining() < columns_rounded {
            return Err(RowDataError::MissingNullMaskData(
                columns_rounded,
                row_buffer.remaining(),
            ));
        }

        let null_mask_raw = row_buffer.copy_to_bytes(columns_rounded);
        Ok(NullMask::parse(&null_mask_raw, table.attributes.len()))
    }
}

impl fmt::Display for RowData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RowData\n")?;
        write!(f, "\tTable: {}\n", self.table.name)?;
        write!(f, "\tMin Tran: {}\n", self.min)?;
        match self.max {
            Some(m) => write!(f, "\tMax Tran: {}\n", m),
            None => write!(f, "\tMax Tran: Unset\n"),
        }?;
        write!(f, "\t{}\n", self.item_pointer)?;
        for column in &self.user_data {
            match column {
                Some(c) => write!(f, "\t{}\n", c),
                None => write!(f, "\tNull\n"),
            }?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum RowDataError {
    #[error("Table definition length {0} does not match columns passed {1}")]
    TableRowSizeMismatch(usize, usize),
    #[error("Table definition type {0} does not match column passed {1}")]
    TableRowTypeMismatch(BuiltinSqlTypes, DeserializeTypes),
    #[error("Not enough min data need {0} got {1}")]
    MissingMinData(usize, usize),
    #[error("Not enough max data need {0} got {1}")]
    MissingMaxData(usize, usize),
    #[error("Not enough infomask data need {0} got {1}")]
    MissingInfoMaskData(usize, usize),
    #[error("Not enough null mask data need {0} got {1}")]
    MissingNullMaskData(usize, usize),
    #[error("Unable to parse type {0}")]
    ColumnParseError(#[from] SqlTypeError),
    #[error(transparent)]
    ItemPointerError(#[from] ItemPointerError),
}

#[cfg(test)]
mod tests {
    use super::super::super::super::objects::Attribute;
    use super::super::super::page_formats::UInt12;
    use super::*;

    fn getItemPointer() -> ItemPointer {
        ItemPointer::new(0, UInt12::new(0).unwrap())
    }

    #[test]
    fn test_row_data_single_text() {
        let table = Arc::new(Table::new(
            "test_table".to_string(),
            vec![Attribute::new(
                uuid::Uuid::new_v4(),
                "header".to_string(),
                DeserializeTypes::Text,
            )],
        ));

        let test = RowData::new(
            table.clone(),
            TransactionId::new(1),
            None,
            getItemPointer(),
            vec![Some(BuiltinSqlTypes::Text("this is a test".to_string()))],
        )
        .unwrap();

        let test_serial = test.serialize();
        let test_parse = RowData::parse(table, test_serial).unwrap();
        assert_eq!(test, test_parse);
    }

    #[test]
    fn test_row_data_double_text() {
        let table = Arc::new(Table::new(
            "test_table".to_string(),
            vec![
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header".to_string(),
                    DeserializeTypes::Text,
                ),
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header2".to_string(),
                    DeserializeTypes::Text,
                ),
            ],
        ));

        let test = RowData::new(
            table.clone(),
            TransactionId::new(1),
            None,
            getItemPointer(),
            vec![
                Some(BuiltinSqlTypes::Text("this is a test".to_string())),
                Some(BuiltinSqlTypes::Text("this is not a test".to_string())),
            ],
        )
        .unwrap();

        let test_serial = test.serialize();
        let test_parse = RowData::parse(table, test_serial).unwrap();
        assert_eq!(test, test_parse);
    }

    #[test]
    fn test_row_uuid_roundtrip() {
        let table = Arc::new(Table::new(
            "test_table".to_string(),
            vec![Attribute::new(
                uuid::Uuid::new_v4(),
                "header".to_string(),
                DeserializeTypes::Uuid,
            )],
        ));

        let test = RowData::new(
            table.clone(),
            TransactionId::new(1),
            None,
            getItemPointer(),
            vec![Some(BuiltinSqlTypes::Uuid(uuid::Uuid::new_v4()))],
        )
        .unwrap();

        let test_serial = test.serialize();
        let test_parse = RowData::parse(table, test_serial).unwrap();
        assert_eq!(test, test_parse);
    }

    #[test]
    fn test_row_uuid_double_roundtrip() {
        let table = Arc::new(Table::new(
            "test_table".to_string(),
            vec![
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header".to_string(),
                    DeserializeTypes::Uuid,
                ),
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header2".to_string(),
                    DeserializeTypes::Uuid,
                ),
            ],
        ));

        let test = RowData::new(
            table.clone(),
            TransactionId::new(1),
            None,
            getItemPointer(),
            vec![
                Some(BuiltinSqlTypes::Uuid(uuid::Uuid::new_v4())),
                Some(BuiltinSqlTypes::Uuid(uuid::Uuid::new_v4())),
            ],
        )
        .unwrap();

        let test_serial = test.serialize();
        let test_parse = RowData::parse(table, test_serial).unwrap();
        assert_eq!(test, test_parse);
    }

    #[test]
    fn test_row_uuid_double_opt_roundtrip() {
        let table = Arc::new(Table::new(
            "test_table".to_string(),
            vec![
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header".to_string(),
                    DeserializeTypes::Uuid,
                ),
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header2".to_string(),
                    DeserializeTypes::Uuid,
                ),
            ],
        ));

        let test = RowData::new(
            table.clone(),
            TransactionId::new(1),
            None,
            getItemPointer(),
            vec![Some(BuiltinSqlTypes::Uuid(uuid::Uuid::new_v4())), None],
        )
        .unwrap();

        let test_serial = test.serialize();
        println!("{:?}", test_serial.len());
        let test_parse = RowData::parse(table, test_serial).unwrap();
        assert_eq!(test, test_parse);
    }

    #[test]
    fn test_row_complex_data_roundtrip() {
        let table = Arc::new(Table::new(
            "test_table".to_string(),
            vec![
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header".to_string(),
                    DeserializeTypes::Text,
                ),
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "id".to_string(),
                    DeserializeTypes::Uuid,
                ),
                Attribute::new(
                    uuid::Uuid::new_v4(),
                    "header3".to_string(),
                    DeserializeTypes::Text,
                ),
            ],
        ));

        let test = RowData::new(table.clone(),
            TransactionId::new(1),
            None,
            getItemPointer(),
            vec![
                Some(BuiltinSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BuiltinSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ],
        ).unwrap();

        let test_serial = test.serialize();
        let test_parse = RowData::parse(table, test_serial).unwrap();
        assert_eq!(test, test_parse);
    }
}
