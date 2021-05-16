use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct NetworkFrame {
    pub message_type: u8,
    pub payload: Bytes
}

impl NetworkFrame {
    pub fn new(message_type: u8, payload: Bytes) -> NetworkFrame {
        NetworkFrame{
            message_type: message_type,
            payload: payload
        }
    }
}

pub fn authentication_ok() -> NetworkFrame {
    NetworkFrame {
        message_type: b'R',
        payload:  Bytes::from_static(b"\0\0\0\0")
    }
}

//Note this claims that the server is ALWAYS ready, even if its not
pub fn ready_for_query() -> NetworkFrame {
    NetworkFrame {
        message_type: b'Z',
        payload:  Bytes::from_static(b"I")
    }
}
