//! Shows the values of valid transactions for use in visibility checks
//! See rules here: http://www.interdb.jp/pg/pgsql05.html#_5.5.
use super::TransactionId;

#[derive(Clone, Debug)]
pub struct TransactionSnapshot {
    pub min: TransactionId,
    pub max: TransactionId,
    pub in_range: Vec<TransactionId>,
}
