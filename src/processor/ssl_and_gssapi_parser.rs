use hex_literal::hex;
use nom::{
    IResult,
    bytes::complete::tag};

fn match_ssl_request(input: &[u8]) -> IResult<&[u8], &[u8]> {
    //From here: https://www.postgresql.org/docs/current/protocol-message-formats.html
    tag(&hex!("04 D2 16 2F"))(input)
}

pub fn is_ssl_request(input: &[u8]) -> bool {
    match match_ssl_request(input){
        Ok(_) => return true,
        Err(_) => return false
    }
}

fn match_gssapi_request(input: &[u8]) -> IResult<&[u8], &[u8]> {
    tag(&hex!("04 D2 16 30"))(input)
}

pub fn is_gssapi_request(input: &[u8]) -> bool {
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
        let check = is_ssl_request(&hex!("04 D2 16 2F"));
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