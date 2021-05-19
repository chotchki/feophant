//! This defines all the system internal tables so we can bootstrap the system
pub enum TableDefinitions {
    PgClass, //Tables
}

impl TableDefinitions {
    pub const values: [TableDefinitions] = [PgClass];
    pub const fn value(self) -> Bytes {
        use TableDefinitions::*;
        match self {
            SystemError => Bytes::from_static(b"58000")
        }
    }
}