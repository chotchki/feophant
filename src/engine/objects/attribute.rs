//!Postgres Doc: https://www.postgresql.org/docs/current/catalog-pg-attribute.html

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
