//!Wrapper type for a row in the database unattached to a table
use std::ops::Deref;

use crate::engine::io::SelfEncodedSize;

use super::types::{BaseSqlTypes, SqlTypeDefinition};
use bytes::BufMut;
use thiserror::Error;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SqlTuple(pub Vec<Option<BaseSqlTypes>>);

impl SqlTuple {
    //Rewrites the tuple to only provide columns requested in the order requested
    pub fn filter_map(
        self,
        source: &SqlTypeDefinition,
        target: &SqlTypeDefinition,
    ) -> Result<SqlTuple, SqlTupleError> {
        if self.len() != source.len() {
            return Err(SqlTupleError::SourceLenMismatch(self.len(), source.len()));
        }

        let mut output = Vec::with_capacity(target.len());

        //TODO handle possible type conversion OR figure out if it bombs
        'outer: for (t_name, _) in target.iter() {
            for s in 0..source.len() {
                if *t_name == source[s].0 {
                    output.push(self.0[s].clone()); //TODO remove the clone
                    continue 'outer;
                }
            }
            return Err(SqlTupleError::InvalidColumn(t_name.to_string()));
        }

        Ok(SqlTuple(output))
    }

    pub fn merge(left: &SqlTuple, right: &SqlTuple) -> SqlTuple {
        //Code from here: https://stackoverflow.com/a/56490417
        SqlTuple(left.iter().cloned().chain(right.iter().cloned()).collect())
    }

    pub fn serialize(&self, buffer: &mut impl BufMut) {
        for data in &self.0 {
            match data {
                Some(d) => d.serialize(buffer),
                None => {}
            }
        }
    }
}

impl Deref for SqlTuple {
    type Target = Vec<Option<BaseSqlTypes>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SelfEncodedSize for SqlTuple {
    fn encoded_size(&self) -> usize {
        self.iter().fold(0, |acc, col| match col {
            Some(col_s) => acc + col_s.encoded_size(),
            None => acc,
        })
    }
}

#[derive(Debug, Error)]
pub enum SqlTupleError {
    #[error("Tuple length {0} does not match source length {1}")]
    SourceLenMismatch(usize, usize),
    #[error("Requested Column Name: {0} doesn't exist")]
    InvalidColumn(String),
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::engine::objects::types::BaseSqlTypesMapper;

    fn get_src_type() -> SqlTypeDefinition {
        SqlTypeDefinition(vec![
            ("foo".to_string(), BaseSqlTypesMapper::Integer),
            ("bar".to_string(), BaseSqlTypesMapper::Text),
            ("baz".to_string(), BaseSqlTypesMapper::Text),
        ])
    }

    #[test]
    fn test_sql_tuple_filter() -> Result<(), Box<dyn std::error::Error>> {
        let source = get_src_type();

        let target = SqlTypeDefinition(vec![source[2].clone(), source[1].clone()]);

        let src_cols = SqlTuple(vec![
            None,
            Some(BaseSqlTypes::Text("Test".to_string())),
            Some(BaseSqlTypes::Text("Test2".to_string())),
        ]);

        let filtered = src_cols.filter_map(&source, &target)?;

        let expected = SqlTuple(vec![
            Some(BaseSqlTypes::Text("Test2".to_string())),
            Some(BaseSqlTypes::Text("Test".to_string())),
        ]);

        assert_eq!(filtered, expected);

        Ok(())
    }

    #[test]
    fn test_sql_tuple_merge() -> Result<(), Box<dyn std::error::Error>> {
        let left = SqlTuple(vec![
            None,
            Some(BaseSqlTypes::Text("Test".to_string())),
            Some(BaseSqlTypes::Text("Test2".to_string())),
        ]);

        let right = SqlTuple(vec![
            Some(BaseSqlTypes::Text("Test2".to_string())),
            Some(BaseSqlTypes::Text("Test".to_string())),
        ]);

        let expected = SqlTuple(vec![
            None,
            Some(BaseSqlTypes::Text("Test".to_string())),
            Some(BaseSqlTypes::Text("Test2".to_string())),
            Some(BaseSqlTypes::Text("Test2".to_string())),
            Some(BaseSqlTypes::Text("Test".to_string())),
        ]);

        let merged = SqlTuple::merge(&left, &right);

        assert_eq!(merged, expected);

        Ok(())
    }

    #[test]
    fn test_encoded_size() {
        let tuple = SqlTuple(vec![Some(BaseSqlTypes::Uuid(uuid::Uuid::new_v4())), None]);

        let mut buffer = BytesMut::new();
        tuple.serialize(&mut buffer);
        let buffer = buffer.freeze();

        assert_eq!(tuple.encoded_size(), buffer.len());
    }
}
