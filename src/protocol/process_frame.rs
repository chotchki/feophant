use bytes::Bytes;
use hex_literal::hex;

use super::frame::Frame;
use super::connection::Connection;

use tokio::sync::oneshot::Sender;

const SSL_PAYLOAD: Bytes = Bytes::from_static(&hex!("12 34 56 78"));

pub fn process_frame(conn: &Connection, frame: &Frame, sender: Sender<Frame>){
    if frame.message_type == 0 && SSL_PAYLOAD == frame.payload.slice(0..4) { //This is a special case for SSL setup
        conn.startup_done();
    }
}