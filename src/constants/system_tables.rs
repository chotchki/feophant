//! This defines all the system internal tables so we can bootstrap the system.

use super::super::engine::objects::Table;
use std::sync::Arc;

pub mod pg_attribute;
pub mod pg_class;
pub mod pg_constraint;
pub mod pg_index;

#[derive(Copy, Clone)]
pub enum SystemTables {
    PgAttribute, //Columns
    PgClass,     //Tables
    PgConstraint,
    PgIndex,
}

impl SystemTables {
    //TODO Should this be removed?
    pub const VALUES: [SystemTables; 4] = [
        SystemTables::PgAttribute,
        SystemTables::PgClass,
        SystemTables::PgConstraint,
        SystemTables::PgIndex,
    ];
    pub fn value(self) -> Arc<Table> {
        match self {
            SystemTables::PgClass => pg_class::get_table(),
            SystemTables::PgAttribute => pg_attribute::get_table(),
            SystemTables::PgConstraint => pg_constraint::get_table(),
            SystemTables::PgIndex => pg_index::get_table(),
        }
    }
}
