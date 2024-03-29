use hex_literal::hex;
use nom::{bytes::complete::tag, IResult};

pub fn match_ssl_request(input: &[u8]) -> IResult<&[u8], &[u8]> {
    //From here: https://www.postgresql.org/docs/current/protocol-message-formats.html
    tag(&hex!("04 D2 16 2F"))(input)
}

pub fn is_ssl_request(input: &[u8]) -> bool {
    match_ssl_request(input).is_ok()
}

fn match_gssapi_request(input: &[u8]) -> IResult<&[u8], &[u8]> {
    tag(&hex!("04 D2 16 30"))(input)
}

pub fn is_gssapi_request(input: &[u8]) -> bool {
    match_gssapi_request(input).is_ok()
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_ssl_match() {
        assert!(is_ssl_request(&hex!("04 D2 16 2F")));
    }

    #[test]
    fn test_ssl_not_match() {
        assert!(!is_ssl_request(&hex!("12 34 56")));
    }

    #[test]
    fn test_gsspai_match() {
        assert!(is_gssapi_request(&hex!("04 D2 16 30")));
    }
}
