use std::fmt;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TransactionIsolation {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl fmt::Display for TransactionIsolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionIsolation::ReadCommitted => {
                write!(f, "ReadCommitted")
            }
            TransactionIsolation::RepeatableRead => {
                write!(f, "RepeatableRead")
            }
            TransactionIsolation::Serializable => {
                write!(f, "Serializable")
            }
        }
    }
}
