use nom::{
    bytes::complete::{is_not,tag},
    combinator::{iterator,map,map_res},
    Err,
    IResult,
    multi::many_till,
    sequence::pair,
    sequence::terminated
    };
use std::collections::HashMap;

pub fn parse_startup(input: &[u8]) -> Result<HashMap<String, String>, nom::Err<nom::error::Error<&[u8]>>> {
    let (_, items) = parse_key_and_values(input)?;

    let mut result: HashMap<String, String> = HashMap::new();

    for (k,v) in items {
        result.insert(k, v);
    }

    Ok(result)
}

fn parse_key_and_values(input: &[u8]) -> IResult<&[u8], Vec<(String, String)>> {
    map(many_till(pair(till_null, till_null), tag(b"\0")), |(k,v)| k)(input)
}

fn till_null(input: &[u8]) -> IResult<&[u8], String> {
    map_res(terminated(is_not("\0"), tag(b"\0")),|s:&[u8]|String::from_utf8(s.to_vec()))(input)
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_till_null(){
        let test_string = b"user\0";

        let (remaining, result) = till_null(test_string).unwrap();

        assert_eq!("user", result);
        assert_eq!(b"", remaining);
    }

    #[test]
    fn test_parse_key_and_values(){
        let test_string = b"user\0user2\0\0";

        let correct = vec!(("user".to_string(), "user2".to_string()));

        let (remaining, result) = parse_key_and_values(test_string).unwrap();

        assert_eq!(correct, result);
        assert_eq!(b"", remaining);
    }

    #[test]
    fn test_invalid_utf8_till_null(){
        let test_string = b"\xc3\x28\0";

        match till_null(test_string) {
            Ok(_) => assert!(false),
            Err(_) => assert!(true)
        }
    }

    #[test]
    fn test_start_up_string() {
        let startup_mesg = b"user\0some_user\0user2\0some_user\0\0";

        let mut map: HashMap<String, String> = HashMap::new();
        map.insert("user".to_string(), "some_user".to_string());
        map.insert("user2".to_string(), "some_user".to_string());

        let test_map = parse_startup(startup_mesg);

        assert_eq!(map, test_map.unwrap());
    }
}