//! Encodes / decodes a row into a byte array based on the supplied specification
//! Format from here: https://www.postgresql.org/docs/current/storage-page-layout.html
//! As always I'm only implementing what I need and will extend once I need more
use super::super::super::objects::Table;
use super::super::super::transactions::TransactionId;
use super::null_mask::NullMaskError;
use super::{InfoMask, ItemPointer, ItemPointerError, NullMask};
use crate::engine::io::format_traits::{Parseable, Serializable};
use crate::engine::io::{ConstEncodedSize, EncodedSize, SelfEncodedSize};
use crate::engine::objects::types::{BaseSqlTypes, BaseSqlTypesError, SqlTypeDefinition};
use crate::engine::objects::SqlTuple;
use bytes::{Buf, BufMut};
use std::fmt;
use std::mem::size_of;
use std::sync::Arc;
use thiserror::Error;

/// Holds information about a particular row in a table as well as metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct RowData {
    ///Type that defines the row
    pub sql_type: Arc<SqlTypeDefinition>,
    ///Lowest transaction this row is valid for (still need to check that transaction's status)
    pub min: TransactionId,
    ///Max transaction this row is valid for OR None for still valid (still need to check max's status)
    pub max: Option<TransactionId>,
    ///Page + Offset where this row is stored on disk
    pub item_pointer: ItemPointer,
    ///Columns stored in this row
    pub user_data: SqlTuple,
}

impl RowData {
    pub fn new(
        sql_type: Arc<SqlTypeDefinition>,
        min: TransactionId,
        max: Option<TransactionId>,
        item_pointer: ItemPointer,
        user_data: SqlTuple,
    ) -> RowData {
        RowData {
            sql_type,
            min,
            max,
            item_pointer,
            user_data,
        }
    }

    pub fn get_column(&self, name: &str) -> Result<Option<BaseSqlTypes>, RowDataError> {
        for i in 0..self.sql_type.len() {
            if self.sql_type[i].0 == *name {
                return Ok(self.user_data.0[i].clone());
            }
        }

        Err(RowDataError::ColumnDoesNotExist(name.to_string()))
    }

    pub fn get_column_not_null(&self, name: &str) -> Result<BaseSqlTypes, RowDataError> {
        self.get_column(name)?
            .ok_or_else(|| RowDataError::UnexpectedNull(name.to_string()))
    }

    pub fn parse(table: Arc<Table>, row_buffer: &mut impl Buf) -> Result<RowData, RowDataError> {
        if row_buffer.remaining() < TransactionId::encoded_size() {
            return Err(RowDataError::MissingMinData(
                size_of::<TransactionId>(),
                row_buffer.remaining(),
            ));
        }
        let min = TransactionId::new(row_buffer.get_u64_le());

        if row_buffer.remaining() < TransactionId::encoded_size() {
            return Err(RowDataError::MissingMaxData(
                size_of::<TransactionId>(),
                row_buffer.remaining(),
            ));
        }
        let max_temp = row_buffer.get_u64_le();
        let max = match max_temp {
            0 => None,
            _ => Some(TransactionId::new(max_temp)),
        };

        let item_pointer = ItemPointer::parse(row_buffer)?;

        let null_mask = RowData::get_null_mask(table.clone(), row_buffer)?;

        let mut user_data = SqlTuple(vec![]);
        for (column, mask) in table.attributes.iter().zip(null_mask.iter()) {
            if *mask {
                user_data.0.push(None);
            } else {
                user_data.0.push(Some(BaseSqlTypes::deserialize(
                    &column.sql_type,
                    row_buffer,
                )?));
            }
        }

        Ok(RowData::new(
            table.sql_type.clone(),
            min,
            max,
            item_pointer,
            user_data,
        ))
    }

    //Gets the null mask, if it doesn't exist it will return a vector of all not nulls
    fn get_null_mask(
        table: Arc<Table>,
        row_buffer: &mut impl Buf,
    ) -> Result<Vec<bool>, RowDataError> {
        if row_buffer.remaining() < size_of::<InfoMask>() {
            return Err(RowDataError::MissingInfoMaskData(
                size_of::<TransactionId>(),
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

        let mut null_mask_raw = row_buffer.copy_to_bytes(columns_rounded);
        Ok(NullMask::parse(&mut null_mask_raw, table.attributes.len())?)
    }
}

impl fmt::Display for RowData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "RowData")?;
        writeln!(f, "\tType: {}", self.sql_type)?;
        writeln!(f, "\tMin Tran: {}", self.min)?;
        match self.max {
            Some(m) => writeln!(f, "\tMax Tran: {}", m),
            None => writeln!(f, "\tMax Tran: Unset"),
        }?;
        writeln!(f, "\t{}", self.item_pointer)?;
        for column in &self.user_data.0 {
            match column {
                Some(c) => writeln!(f, "\t{}", c),
                None => writeln!(f, "\tNull"),
            }?;
        }
        Ok(())
    }
}

impl EncodedSize<&SqlTuple> for RowData {
    fn encoded_size(input: &SqlTuple) -> usize {
        size_of::<u64>()
            + size_of::<u64>()
            + ItemPointer::encoded_size()
            + InfoMask::encoded_size()
            + NullMask::encoded_size(input)
            + input.encoded_size()
    }
}

impl Serializable for RowData {
    fn serialize(&self, buffer: &mut impl BufMut) {
        buffer.put_u64_le(self.min.get_u64());
        buffer.put_u64_le(self.max.unwrap_or_else(|| TransactionId::new(0)).get_u64());
        self.item_pointer.serialize(buffer);

        //If there is null we add it to the flags and write a nullmask
        let mut mask = InfoMask::empty();
        if self.user_data.iter().any(|x| x.is_none()) {
            mask = InfoMask::HAS_NULL;
            buffer.put_u8(mask.bits());
            buffer.put(NullMask::serialize(&self.user_data));
        } else {
            buffer.put_u8(mask.bits());
        }

        self.user_data.serialize(buffer);
    }
}

#[derive(Debug, Error)]
pub enum RowDataError {
    #[error(transparent)]
    BaseSqlTypes(#[from] BaseSqlTypesError),
    #[error("Table definition length {0} does not match columns passed {1}")]
    TableRowSizeMismatch(usize, usize),
    #[error("Not enough min data need {0} got {1}")]
    MissingMinData(usize, usize),
    #[error("Not enough max data need {0} got {1}")]
    MissingMaxData(usize, usize),
    #[error("Not enough infomask data need {0} got {1}")]
    MissingInfoMaskData(usize, usize),
    #[error("Not enough null mask data need {0} got {1}")]
    MissingNullMaskData(usize, usize),
    #[error(transparent)]
    NullMaskError(#[from] NullMaskError),
    #[error(transparent)]
    ItemPointerError(#[from] ItemPointerError),
    #[error("Column named {0} does not exist")]
    ColumnDoesNotExist(String),
    #[error("Column null when ask not to be {0}")]
    UnexpectedNull(String),
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::constants::Nullable;
    use crate::engine::io::page_formats::PageOffset;
    use crate::engine::objects::types::BaseSqlTypesMapper;

    use super::super::super::super::objects::Attribute;
    use super::super::super::page_formats::UInt12;
    use super::*;

    fn get_item_pointer() -> ItemPointer {
        ItemPointer::new(PageOffset(0), UInt12::new(0).unwrap())
    }

    #[test]
    fn test_row_data_single_text() -> Result<(), Box<dyn std::error::Error>> {
        let table = Arc::new(Table::new(
            uuid::Uuid::new_v4(),
            "test_table".to_string(),
            vec![Attribute::new(
                "header".to_string(),
                BaseSqlTypesMapper::Text,
                Nullable::NotNull,
                None,
            )],
            vec![],
            vec![],
        ));

        let test = RowData::new(
            table.sql_type.clone(),
            TransactionId::new(1),
            None,
            get_item_pointer(),
            SqlTuple(vec![Some(BaseSqlTypes::Text("this is a test".to_string()))]),
        );

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);
        let mut buffer = buffer.freeze();

        let test_parse = RowData::parse(table, &mut buffer)?;
        assert_eq!(test, test_parse);

        Ok(())
    }

    #[test]
    fn test_row_data_double_text() -> Result<(), Box<dyn std::error::Error>> {
        let table = Arc::new(Table::new(
            uuid::Uuid::new_v4(),
            "test_table".to_string(),
            vec![
                Attribute::new(
                    "header".to_string(),
                    BaseSqlTypesMapper::Text,
                    Nullable::NotNull,
                    None,
                ),
                Attribute::new(
                    "header2".to_string(),
                    BaseSqlTypesMapper::Text,
                    Nullable::NotNull,
                    None,
                ),
            ],
            vec![],
            vec![],
        ));

        let test = RowData::new(
            table.sql_type.clone(),
            TransactionId::new(1),
            None,
            get_item_pointer(),
            SqlTuple(vec![
                Some(BaseSqlTypes::Text("this is a test".to_string())),
                Some(BaseSqlTypes::Text("this is not a test".to_string())),
            ]),
        );

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);
        let mut buffer = buffer.freeze();

        let test_parse = RowData::parse(table, &mut buffer)?;
        assert_eq!(test, test_parse);

        Ok(())
    }

    #[test]
    fn test_row_uuid_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let table = Arc::new(Table::new(
            uuid::Uuid::new_v4(),
            "test_table".to_string(),
            vec![Attribute::new(
                "header".to_string(),
                BaseSqlTypesMapper::Uuid,
                Nullable::NotNull,
                None,
            )],
            vec![],
            vec![],
        ));

        let test = RowData::new(
            table.sql_type.clone(),
            TransactionId::new(1),
            None,
            get_item_pointer(),
            SqlTuple(vec![Some(BaseSqlTypes::Uuid(uuid::Uuid::new_v4()))]),
        );

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);
        let mut buffer = buffer.freeze();

        let test_parse = RowData::parse(table, &mut buffer)?;
        assert_eq!(test, test_parse);

        Ok(())
    }

    #[test]
    fn test_row_uuid_double_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let table = Arc::new(Table::new(
            uuid::Uuid::new_v4(),
            "test_table".to_string(),
            vec![
                Attribute::new(
                    "header".to_string(),
                    BaseSqlTypesMapper::Uuid,
                    Nullable::NotNull,
                    None,
                ),
                Attribute::new(
                    "header2".to_string(),
                    BaseSqlTypesMapper::Uuid,
                    Nullable::NotNull,
                    None,
                ),
            ],
            vec![],
            vec![],
        ));

        let test = RowData::new(
            table.sql_type.clone(),
            TransactionId::new(1),
            None,
            get_item_pointer(),
            SqlTuple(vec![
                Some(BaseSqlTypes::Uuid(uuid::Uuid::new_v4())),
                Some(BaseSqlTypes::Uuid(uuid::Uuid::new_v4())),
            ]),
        );

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);
        let mut buffer = buffer.freeze();

        let test_parse = RowData::parse(table, &mut buffer)?;
        assert_eq!(test, test_parse);

        Ok(())
    }

    #[test]
    fn test_row_uuid_double_opt_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let table = Arc::new(Table::new(
            uuid::Uuid::new_v4(),
            "test_table".to_string(),
            vec![
                Attribute::new(
                    "header".to_string(),
                    BaseSqlTypesMapper::Uuid,
                    Nullable::NotNull,
                    None,
                ),
                Attribute::new(
                    "header2".to_string(),
                    BaseSqlTypesMapper::Uuid,
                    Nullable::Null,
                    None,
                ),
            ],
            vec![],
            vec![],
        ));

        let test = RowData::new(
            table.sql_type.clone(),
            TransactionId::new(1),
            None,
            get_item_pointer(),
            SqlTuple(vec![Some(BaseSqlTypes::Uuid(uuid::Uuid::new_v4())), None]),
        );

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);
        let mut buffer = buffer.freeze();

        println!("{:?}", buffer.len());
        let test_parse = RowData::parse(table, &mut buffer)?;
        assert_eq!(test, test_parse);

        Ok(())
    }

    #[test]
    fn test_row_complex_data_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let table = Arc::new(Table::new(
            uuid::Uuid::new_v4(),
            "test_table".to_string(),
            vec![
                Attribute::new(
                    "header".to_string(),
                    BaseSqlTypesMapper::Text,
                    Nullable::NotNull,
                    None,
                ),
                Attribute::new(
                    "id".to_string(),
                    BaseSqlTypesMapper::Uuid,
                    Nullable::Null,
                    None,
                ),
                Attribute::new(
                    "header3".to_string(),
                    BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Integer)),
                    Nullable::NotNull,
                    None,
                ),
            ],
            vec![],
            vec![],
        ));

        let test = RowData::new(
            table.sql_type.clone(),
            TransactionId::new(1),
            None,
            get_item_pointer(),
            SqlTuple(vec![
                Some(BaseSqlTypes::Text("this is a test".to_string())),
                None,
                Some(BaseSqlTypes::Array(vec![
                    BaseSqlTypes::Integer(1),
                    BaseSqlTypes::Integer(2),
                ])),
            ]),
        );

        let mut buffer = BytesMut::new();
        test.serialize(&mut buffer);
        let mut buffer = buffer.freeze();

        let test_parse = RowData::parse(table, &mut buffer)?;
        assert_eq!(test, test_parse);

        let column_val = test_parse.get_column_not_null("header")?;
        assert_eq!(column_val, BaseSqlTypes::Text("this is a test".to_string()));

        Ok(())
    }

    #[test]
    fn test_encoded_size() {
        let tuple = SqlTuple(vec![Some(BaseSqlTypes::Uuid(uuid::Uuid::new_v4())), None]);
        match size_of::<usize>() {
            4 => assert_eq!(40, RowData::encoded_size(&tuple)), //Not 100% certain if correct
            8 => assert_eq!(44, RowData::encoded_size(&tuple)),
            _ => panic!("You're on your own on this arch."),
        }
    }
}
