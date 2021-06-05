//!Postgres Doc: https://www.postgresql.org/docs/current/catalog-pg-attribute.html

use super::super::super::constants::DeserializeTypes;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    id: Uuid,
    pg_class_id: Uuid,
    name: String,
    pub sql_type: DeserializeTypes,
}

impl Attribute {
    pub fn new_existing(
        id: Uuid,
        pg_class_id: Uuid,
        name: String,
        sql_type: DeserializeTypes,
    ) -> Attribute {
        Attribute {
            id,
            pg_class_id,
            name,
            sql_type,
        }
    }

    pub fn new(pg_class_id: Uuid, name: String, sql_type: DeserializeTypes) -> Attribute {
        Attribute {
            id: Uuid::new_v4(),
            pg_class_id,
            name,
            sql_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let test = Attribute::new(Uuid::new_v4(), "test".to_string(), DeserializeTypes::Text);
        let test_existing = Attribute::new_existing(
            test.id.clone(),
            test.pg_class_id.clone(),
            test.name.clone(),
            DeserializeTypes::Text,
        );

        assert_eq!(test, test_existing);
    }
}
