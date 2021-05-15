use bytes::Bytes;
use nom::error::ErrorKind;

use crate::codec::NetworkFrame;
use super::ssl_and_gssapi_parser;

pub struct ClientProcessor {}

impl ClientProcessor {
    pub fn process(&self, frame: NetworkFrame) -> Result<NetworkFrame, nom::Err<nom::error::Error<&[u8]>>>{
        let payload_buff: &[u8] = &frame.payload;
        if frame.message_type == 0 && ssl_and_gssapi_parser::is_ssl_request(payload_buff){
            debug!("Got a SSL Request, no security here... yet");
            return Ok(NetworkFrame::new(0, Bytes::from_static(b"N")))
        } else if frame.message_type == 0 && ssl_and_gssapi_parser::is_gssapi_request(payload_buff) {
            debug!("Got a GSSAPI Request, no security here... yet");
            return Ok(NetworkFrame::new(0, Bytes::from_static(b"N")))
        }



        warn!("Got a message we don't understand yet {}", frame.message_type);
        warn!("Payload is {:?}", frame.payload);
        Err(nom::Err::Failure(nom::error::Error::new(b"Not Implemented", ErrorKind::Fix)))
    } 
}


/*fn till_null(i: &[u8]) -> IResult<&[u8], &[u8]> {
    terminated(alpha1, char(b'\0'))(i)
}

fn parse_startup_message(input: &[u8]) -> IResult<&[u8], HashMap<String, String>> {
    let (i, _) = tag(&hex!("00 03 00 00"))(input)?; //Version but don't care
    let (i, items) = many0(till_null)(i)?;
    let m: HashMap<_, _> = items.into_iter().collect();

    let mut it = iterator(i, terminated(tag(b"\0"), tag(b"\0")));

    let parsed = it.map(|v| (v, v.len())).collect::<HashMap<String, String>>();
    let res: IResult<_,_> = it.finish();
    res
}*/