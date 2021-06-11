//! Top Level of the sql parsing engine
use super::super::objects::Table;
use nom::bytes::complete::{tag_no_case, take_while1};
use nom::character::complete::{alphanumeric1, char, multispace0, multispace1};
use nom::combinator::map_res;
use nom::IResult;

pub struct TopLevel;

impl TopLevel {
    pub fn parse_create_table(input: &[u8]) -> IResult<&[u8], Table> {
        let (input, _) = TopLevel::match_create(input)?;
        let (input, _) = TopLevel::take_whitespace(input)?;
        let (input, _) = TopLevel::match_table(input)?;
        let (input, _) = TopLevel::take_whitespace(input)?;
        let (input, name) = TopLevel::match_table_name(input)?;
        let (input, _) = TopLevel::maybe_take_whitespace(input)?;
        let (input, _) = TopLevel::match_open_paren(input)?;
        let (input, _) = TopLevel::maybe_take_whitespace(input)?;
        let (input, _) = TopLevel::match_close_paren(input)?;

        Ok((input, Table::new(name, vec![])))
    }

    fn maybe_take_whitespace(input: &[u8]) -> IResult<&[u8], &[u8]> {
        multispace0(input)
    }
    fn take_whitespace(input: &[u8]) -> IResult<&[u8], &[u8]> {
        multispace1(input)
    }

    fn match_create(input: &[u8]) -> IResult<&[u8], &[u8]> {
        tag_no_case("create")(input)
    }
    fn match_table(input: &[u8]) -> IResult<&[u8], &[u8]> {
        tag_no_case("table")(input)
    }
    fn match_table_name(input: &[u8]) -> IResult<&[u8], String> {
        map_res(alphanumeric1, |s: &[u8]| String::from_utf8(s.to_vec()))(input)
    }
    fn match_open_paren(input: &[u8]) -> IResult<&[u8], char> {
        char('(')(input)
    }
    fn match_close_paren(input: &[u8]) -> IResult<&[u8], char> {
        char(')')(input)
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_empty_table() {
        let test_string = b"create table foo ()";

        let (_, result) = TopLevel::parse_create_table(test_string).unwrap();

        assert_eq!("foo", result.name);
    }
}
