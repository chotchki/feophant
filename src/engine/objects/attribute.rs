//!Postgres Doc: https://www.postgresql.org/docs/current/catalog-pg-attribute.html

use std::fmt;

use crate::constants::{DeserializeTypes, Nullable};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    id: Uuid,
    pg_class_id: Uuid,
    pub name: String,
    pub sql_type: DeserializeTypes,
    pub nullable: Nullable,
}

impl Attribute {
    pub fn new(
        pg_class_id: Uuid,
        name: String,
        sql_type: DeserializeTypes,
        nullable: Nullable,
    ) -> Attribute {
        Attribute {
            id: Uuid::new_v4(),
            pg_class_id,
            name,
            sql_type,
            nullable,
        }
    }
    pub fn new_existing(
        id: Uuid,
        pg_class_id: Uuid,
        name: String,
        sql_type: DeserializeTypes,
        nullable: Nullable,
    ) -> Attribute {
        Attribute {
            id,
            pg_class_id,
            name,
            sql_type,
            nullable,
        }
    }
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Attribute id:{0}, parent: {1}, name: {2}, sql_type: {3}, nullable: {4}",
            self.id, self.pg_class_id, self.name, self.sql_type, self.nullable
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let test = Attribute::new(
            Uuid::new_v4(),
            "test".to_string(),
            DeserializeTypes::Text,
            Nullable::NotNull,
        );
        let test_existing = Attribute::new_existing(
            test.id.clone(),
            test.pg_class_id.clone(),
            test.name.clone(),
            DeserializeTypes::Text,
            test.nullable,
        );

        assert_eq!(test, test_existing);
    }
}
