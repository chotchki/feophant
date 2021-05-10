use bytes::Bytes;
use hex_literal::hex;
use nom::{
    IResult,
    bytes::complete::tag,
    error::ErrorKind::Fix};

use crate::codec::NetworkFrame;

pub struct ClientProcessor {}

impl ClientProcessor {
    pub fn process(&self, frame: NetworkFrame) -> Result<NetworkFrame, nom::Err<nom::error::Error<&[u8]>>>{
        let payload_buff: &[u8] = &frame.payload;
        if frame.message_type == 0 && is_ssl_request(payload_buff){
            debug!("Got a SSL Request, no security here... yet");
            return Ok(NetworkFrame::new(0, Bytes::from_static(b"N")))
        } else if frame.message_type == 0 && is_gssapi_request(payload_buff) {
            debug!("Got a GSSAPI Request, no security here... yet");
            return Ok(NetworkFrame::new(0, Bytes::from_static(b"N")))
        }

        warn!("Got a message we don't understand yet {}", frame.message_type);
        Err(nom::Err::Failure(nom::error::Error::new(b"Not Implemented", Fix)))
    } 
}

fn match_ssl_request(input: &[u8]) -> IResult<&[u8], &[u8]> {
    //From here: https://www.postgresql.org/docs/current/protocol-message-formats.html
    tag(&hex!("04 D2 16 2F"))(input)
}

fn is_ssl_request(input: &[u8]) -> bool {
    match match_ssl_request(input){
        Ok(_) => return true,
        Err(_) => return false
    }
}

fn match_gssapi_request(input: &[u8]) -> IResult<&[u8], &[u8]> {
    tag(&hex!("04 D2 16 30"))(input)
}

fn is_gssapi_request(input: &[u8]) -> bool {
    match match_gssapi_request(input){
        Ok(_) => return true,
        Err(_) => return false
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_ssl_match() {
        let check = is_ssl_request(&hex!("12 34 56 78"));
        let result = true;
        assert_eq!(check, result);
    }

    #[test]
    fn test_ssl_not_match() {
        let check = is_ssl_request(&hex!("12 34 56"));
        let result = false;
        assert_eq!(check, result);
    }
}