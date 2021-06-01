//! Encodes / decodes a row into a byte array based on the supplied specification
//! Format from here: https://www.postgresql.org/docs/current/storage-page-layout.html
//! As always I'm only implementing what I need and will extend once I need more
use bytes::Bytes;

use super::super::super::objects::TransactionId;
use super::InfoMask;

pub struct RowData {
    t_xmin: TransactionId,
    t_xmax: TransactionId,
    t_infomask: InfoMask, //At the moment only good for if there are null columns
    null_fields: Option<Bytes>,
    user_data: Bytes,
}
