//!Wrapper type for a row in the database unattached to a table
use std::ops::Deref;

use super::types::{BaseSqlTypes, SqlTypeDefinition};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct SqlTuple(pub Vec<Option<BaseSqlTypes>>);

impl SqlTuple {
    //Rewrites the tuple to only provide columns requested in the order requested
    pub fn filter_map(
        self,
        source: &SqlTypeDefinition,
        target: &SqlTypeDefinition,
    ) -> Result<SqlTuple, SqlTupleError> {
        if self.0.len() != source.len() {
            return Err(SqlTupleError::SourceLenMismatch(self.0.len(), source.len()));
        }

        let mut output = Vec::with_capacity(target.len());

        'outer: for (t_name, t) in target.iter() {
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
        SqlTuple(
            left.0
                .iter()
                .cloned()
                .chain(right.0.iter().cloned())
                .collect(),
        )
    }
}

impl Deref for SqlTuple {
    type Target = Vec<Option<BaseSqlTypes>>;

    fn deref(&self) -> &Self::Target {
        &self.0
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
        let source = SqlTypeDefinition(vec![
            ("foo".to_string(), BaseSqlTypesMapper::Integer),
            ("bar".to_string(), BaseSqlTypesMapper::Text),
            ("baz".to_string(), BaseSqlTypesMapper::Text),
        ]);

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
}
