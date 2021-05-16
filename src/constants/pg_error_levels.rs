//! Constant error codes found from here: https://www.postgresql.org/docs/current/errcodes-appendix.html
#![feature(const_if_match)]

use bytes::{Buf,Bytes};

//https://stackoverflow.com/a/62759252/160208
pub enum PgErrorLevels {
    Error,
    Fatal,
    Panic,
    Warning,
    Notice,
    Debug,
    Info,
    Log
}

impl PgErrorLevels {
    pub const fn value(self) -> Bytes {
        use PgErrorLevels::*;
        match self {
            Error => Bytes::from_static(b"ERROR"),
            Fatal => Bytes::from_static(b"FATAL"),
            Panic => Bytes::from_static(b"PANIC"),
            Warning => Bytes::from_static(b"WARNING"),
            Notice => Bytes::from_static(b"NOTICE"),
            Debug => Bytes::from_static(b"DEBUG"),
            Info => Bytes::from_static(b"INFO"),
            Log => Bytes::from_static(b"LOG"),
        }
    }
}