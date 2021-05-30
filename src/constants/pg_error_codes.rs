//! Constant error codes found from here: https://www.postgresql.org/docs/current/errcodes-appendix.html

use bytes::Bytes;

//https://stackoverflow.com/a/62759252/160208
pub enum PgErrorCodes {
    SystemError,
}

impl PgErrorCodes {
    pub const fn value(self) -> Bytes {
        use PgErrorCodes::*;
        match self {
            SystemError => Bytes::from_static(b"58000"),
        }
    }
}
