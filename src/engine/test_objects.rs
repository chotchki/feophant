//! Set of functions used for unit testing instead of copying them everywhere

use super::objects::{SqlTuple, Table};
use crate::{
    constants::Nullable,
    engine::objects::{
        types::{BaseSqlTypes, BaseSqlTypesMapper},
        Attribute,
    },
};
use std::sync::Arc;

pub fn get_row(input: String) -> SqlTuple {
    SqlTuple(vec![
            Some(BaseSqlTypes::Text(input)),
            None,
            Some(BaseSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
        ])
}

pub fn get_table() -> Arc<Table> {
    Arc::new(Table::new(
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
                BaseSqlTypesMapper::Text,
                Nullable::NotNull,
                None,
            ),
        ],
        vec![],
        vec![],
    ))
}
