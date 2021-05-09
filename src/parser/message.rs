use hex_literal::hex;
use nom;

let message_type = take!(1)
let message_length = u32!

const SSL_PAYLOAD: [u8; 4] = hex!("12 34 56 78");
let ssl_request = tag!(Bytes::from_static(&hex!("00 00 00 08 12 34 56 78")))
let protocol_version = tag!(Bytes::from_static(&hex!("00 19 66 08")))

let startup_message = message_length

let message_parsers = alt!(ssl_request |

)

fn parse_message(i: &[u8]) -> IResult<&[u8], Request> {
    
}