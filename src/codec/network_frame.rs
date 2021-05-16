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

pub fn AuthenticationOk() -> NetworkFrame {
    NetworkFrame {
        message_type: b'R',
        payload:  Bytes::from_static(b"\0\0\0\0")
    }
}