//!Wrapper type for a row in the database unattached to a table
use crate::constants::BuiltinSqlTypes;
use thiserror::Error;
use uuid::Uuid;

use super::{Attribute, Table};

#[derive(Clone, Debug, PartialEq)]
pub struct SqlTuple(pub Vec<Option<BuiltinSqlTypes>>);

impl SqlTuple {
    //Rewrites the tuple to only provide columns requested in the order requested
    pub fn filter_map(
        &self,
        source: &Table,
        target: &Vec<Attribute>,
    ) -> Result<SqlTuple, SqlTupleError> {
        if self.0.len() != source.attributes.len() {
            return Err(SqlTupleError::SourceLenMismatch(
                self.0.len(),
                source.attributes.len(),
            ));
        }

        let mut output = Vec::with_capacity(target.len());

        'outer: for t in target {
            for s in 0..source.attributes.len() {
                if t.id == source.attributes[s].id {
                    output.push(self.0[s].clone());
                    continue 'outer;
                }
            }
            return Err(SqlTupleError::InvalidColumn(t.id, t.name.clone()));
        }

        Ok(SqlTuple(output))
    }

    pub fn merge(left: &SqlTuple, right: &SqlTuple) -> SqlTuple {
        //Code from here: https://stackoverflow.com/a/56490417
        SqlTuple(
            left.0
                .iter()
                .cloned()
                .chain(right.0.iter().cloned())
                .collect(),
        )
    }
}

#[derive(Debug, Error)]
pub enum SqlTupleError {
    #[error("Tuple length {0} does not match source length {1}")]
    SourceLenMismatch(usize, usize),
    #[error("Requested Column Id: {0} Name: {1} doesn't exist")]
    InvalidColumn(Uuid, String),
}

//TODO This REALLY needs a good unit test
#[cfg(test)]
mod tests {
    use crate::constants::DeserializeTypes;

    use super::*;

    fn get_table() -> Table {
        let tbl_uuid = Uuid::new_v4();
        let attrs = vec![
            Attribute::new(
                tbl_uuid,
                "foo".to_string(),
                DeserializeTypes::Integer,
                crate::constants::Nullable::Null,
            ),
            Attribute::new(
                tbl_uuid,
                "bar".to_string(),
                DeserializeTypes::Text,
                crate::constants::Nullable::NotNull,
            ),
            Attribute::new(
                tbl_uuid,
                "baz".to_string(),
                DeserializeTypes::Text,
                crate::constants::Nullable::NotNull,
            ),
        ];

        Table::new_existing(tbl_uuid, "Test Table".to_string(), attrs)
    }

    #[test]
    fn test_sql_tuple_filter() -> Result<(), Box<dyn std::error::Error>> {
        let tbl = get_table();

        let src_cols = SqlTuple(vec![
            None,
            Some(BuiltinSqlTypes::Text("Test".to_string())),
            Some(BuiltinSqlTypes::Text("Test2".to_string())),
        ]);

        let filtered = src_cols.filter_map(
            &tbl,
            &vec![tbl.attributes[2].clone(), tbl.attributes[1].clone()],
        )?;

        let expected = SqlTuple(vec![
            Some(BuiltinSqlTypes::Text("Test2".to_string())),
            Some(BuiltinSqlTypes::Text("Test".to_string())),
        ]);

        assert_eq!(filtered, expected);

        Ok(())
    }

    #[test]
    fn test_sql_tuple_merge() -> Result<(), Box<dyn std::error::Error>> {
        let left = SqlTuple(vec![
            None,
            Some(BuiltinSqlTypes::Text("Test".to_string())),
            Some(BuiltinSqlTypes::Text("Test2".to_string())),
        ]);

        let right = SqlTuple(vec![
            Some(BuiltinSqlTypes::Text("Test2".to_string())),
            Some(BuiltinSqlTypes::Text("Test".to_string())),
        ]);

        let expected = SqlTuple(vec![
            None,
            Some(BuiltinSqlTypes::Text("Test".to_string())),
            Some(BuiltinSqlTypes::Text("Test2".to_string())),
            Some(BuiltinSqlTypes::Text("Test2".to_string())),
            Some(BuiltinSqlTypes::Text("Test".to_string())),
        ]);

        let merged = SqlTuple::merge(&left, &right);

        assert_eq!(merged, expected);

        Ok(())
    }
}
