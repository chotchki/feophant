//! I'm thinking that I treat this like the system tables, have a lookup stuct
//! 
//! Postgres Doc: https://www.postgresql.org/docs/current/catalog-pg-type.html
use uuid::Uuid;

pub struct SqlType {
    id: Uuid,
    name: String,
    parent: Uuid,
}