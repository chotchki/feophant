use bytes::Bytes;
use bytes::{BufMut, BytesMut};

use crate::constants::{PgErrorCodes, PgErrorLevels};

#[derive(Clone, Debug)]
pub struct NetworkFrame {
    pub message_type: u8,
    pub payload: Bytes,
}

impl NetworkFrame {
    pub fn new(message_type: u8, payload: Bytes) -> NetworkFrame {
        NetworkFrame {
            message_type: message_type,
            payload: payload,
        }
    }
}

pub fn authentication_ok() -> NetworkFrame {
    NetworkFrame {
        message_type: b'R',
        payload: Bytes::from_static(b"\0\0\0\0"),
    }
}

//Note this claims that the server is ALWAYS ready, even if its not
pub fn ready_for_query() -> NetworkFrame {
    NetworkFrame {
        message_type: b'Z',
        payload: Bytes::from_static(b"I"),
    }
}

//Valid severities can be found here: https://www.postgresql.org/docs/current/protocol-error-fields.html
//Valid error codes can be found here: https://www.postgresql.org/docs/current/errcodes-appendix.html
pub fn error_response(
    severity: PgErrorLevels,
    code: PgErrorCodes,
    message: String,
) -> NetworkFrame {
    let mut buffer = BytesMut::new();
    buffer.put(&b"S"[..]); //Severity
    buffer.put(severity.value());
    buffer.put(&b"\0"[..]);
    buffer.put(&b"M"[..]); //Code
    buffer.put(message.as_bytes());
    buffer.put(&b"\0"[..]);
    buffer.put(&b"C"[..]); //Code
    buffer.put(code.value());
    buffer.put(&b"\0"[..]);
    buffer.put(&b"\0"[..]);

    NetworkFrame {
        message_type: b'N', //Testing notifications
        payload: buffer.freeze(),
    }
}
