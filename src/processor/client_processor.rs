use bytes::Bytes;
use nom::error::ErrorKind;
use thiserror::Error;

use crate::codec::{
    authentication_ok,
    NetworkFrame,
    ready_for_query};
use super::ssl_and_gssapi_parser;
use super::startup_parser;

pub struct ClientProcessor {}

impl ClientProcessor {
    pub fn process(&self, frame: NetworkFrame) -> Result<Vec<NetworkFrame>, ClientProcessorError>{
        let payload_buff: &[u8] = &frame.payload;
        if frame.message_type == 0 && ssl_and_gssapi_parser::is_ssl_request(payload_buff){
            debug!("Got a SSL Request, no security here... yet");
            return Ok(vec!(NetworkFrame::new(0, Bytes::from_static(b"N"))))
        } else if frame.message_type == 0 && ssl_and_gssapi_parser::is_gssapi_request(payload_buff) {
            debug!("Got a GSSAPI Request, no security here... yet");
            return Ok(vec!(NetworkFrame::new(0, Bytes::from_static(b"N"))))
        } else if frame.message_type == 0 {
            debug!("Got a startup message!");
            let message = startup_parser::parse_startup(payload_buff).or_else(|_| Err(ClientProcessorError::BadStartup()))?;

            //TODO: Upon getting a startup message we should be checking for a database and user
            //We should also check for configured authentication methods... maybe later!
            //   we're just going to let them in so we can get further on message parsing.
            info!("Just going to let {:?} in", message.get("user"));
            return Ok(vec!(authentication_ok(),ready_for_query()))
        }



        warn!("Got a message we don't understand yet {}", frame.message_type);
        warn!("Payload is {:?}", frame.payload);
        Err(ClientProcessorError::Unknown())
    } 
}

#[derive(Error, Debug)]
pub enum ClientProcessorError {
    #[error("Malformed Startup Packet")]
    BadStartup(),
    #[error("Unknown Message")]
    Unknown(),
}