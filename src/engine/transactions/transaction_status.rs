//! See here for details: http://www.interdb.jp/pg/pgsql05.html#_5.4.
use std::fmt;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TransactionStatus {
    InProgress,
    Commited,
    Aborted,
    //SUB_COMMITTED, Not implementing until I need it
}

impl fmt::Display for TransactionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionStatus::InProgress => {
                write!(f, "InProgress")
            }
            TransactionStatus::Commited => {
                write!(f, "Commited")
            }
            TransactionStatus::Aborted => {
                write!(f, "Aborted")
            }
        }
    }
}
