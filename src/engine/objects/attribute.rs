//!Postgres Doc: https://www.postgresql.org/docs/current/catalog-pg-attribute.html

use std::fmt;

use crate::constants::Nullable;
use uuid::Uuid;

use super::types::BaseSqlTypesMapper;

#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    pub name: String,                 //Column Name
    pub sql_type: BaseSqlTypesMapper, //Underlying type
    pub nullable: Nullable,           //Null constraint
    pub length: Option<usize>,        //Length of variable length columns - constraint
}

impl Attribute {
    pub fn new(
        name: String,
        sql_type: BaseSqlTypesMapper,
        nullable: Nullable,
        length: Option<usize>,
    ) -> Attribute {
        Attribute {
            name,
            sql_type,
            nullable,
            length,
        }
    }
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Attribute name: {0}, sql_type: {1}, nullable: {2}, length: {3:?}",
            self.name, self.sql_type, self.nullable, self.length
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribute_newfmt() -> Result<(), Box<dyn std::error::Error>> {
        let test = Attribute::new(
            "test".to_string(),
            BaseSqlTypesMapper::Text,
            Nullable::NotNull,
            None,
        );
        assert_eq!(
            test.to_string(),
            "Attribute name: test, sql_type: Text, nullable: NotNull, length: None".to_string()
        );

        Ok(())
    }
}
