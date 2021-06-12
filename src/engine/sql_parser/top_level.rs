//! Top Level of the sql parsing engine
use super::super::super::constants::DeserializeTypes;
use super::super::objects::{Attribute, Table};
use nom::bytes::complete::take_until;
use nom::bytes::complete::{is_a, is_not};
use nom::bytes::complete::{tag, tag_no_case, take_while1};
use nom::character::complete::{alphanumeric1, char, multispace0, multispace1};
use nom::character::is_alphanumeric;
use nom::combinator::map_res;
use nom::error::{make_error, ErrorKind};
use nom::multi::separated_list1;
use nom::IResult;
use nom::ParseTo;
use std::str::FromStr;
use uuid::Uuid;

pub struct TopLevel;

impl TopLevel {
    pub fn parse_create_table(input: &[u8]) -> IResult<&[u8], Table> {
        let (input, _) = TopLevel::match_create(input)?;
        let (input, _) = TopLevel::take_whitespace(input)?;
        let (input, _) = TopLevel::match_table(input)?;
        let (input, _) = TopLevel::take_whitespace(input)?;
        let (input, name) = TopLevel::match_name(input)?;
        let (input, _) = TopLevel::maybe_take_whitespace(input)?;
        let (input, _) = TopLevel::match_open_paren(input)?;

        let table = Table::new(name, vec![]);

        let (input, columns) = TopLevel::parse_table_columns(input, table.clone())?;

        let (input, _) = TopLevel::maybe_take_whitespace(input)?;
        let (input, _) = TopLevel::match_close_paren(input)?;

        //Recreate table now that we have everything, the uuid will change but whatever
        let new_table = Table::new(table.name, columns);

        Ok((input, new_table))
    }

    fn parse_table_columns(input: &[u8], table: Table) -> IResult<&[u8], Vec<Attribute>> {
        let (input, raw_columns) = TopLevel::match_columns(input)?;

        let mut columns = vec![];
        for c in raw_columns {
            let (_, new_column) = TopLevel::parse_table_column(c, table.clone())?;
            columns.push(new_column);
        }

        Ok((input, columns))
    }

    fn parse_table_column(input: &[u8], table: Table) -> IResult<&[u8], Attribute> {
        let (input, _) = TopLevel::maybe_take_whitespace(input)?;
        let (input, name) = TopLevel::match_name(input)?;
        let (input, _) = TopLevel::take_whitespace(input)?;
        let (input, sql_type) = TopLevel::match_sql_type(input)?;

        let (input, _) = TopLevel::maybe_take_whitespace(input)?;
        Ok((input, Attribute::new(table.id, name, sql_type)))
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
    fn match_name(input: &[u8]) -> IResult<&[u8], String> {
        map_res(alphanumeric1, |s: &[u8]| String::from_utf8(s.to_vec()))(input)
    }

    fn match_open_paren(input: &[u8]) -> IResult<&[u8], char> {
        char('(')(input)
    }
    fn match_close_paren(input: &[u8]) -> IResult<&[u8], char> {
        char(')')(input)
    }

    fn match_columns(input: &[u8]) -> IResult<&[u8], Vec<&[u8]>> {
        separated_list1(tag(","), is_not(","))(input)
    }

    fn match_sql_type(input: &[u8]) -> IResult<&[u8], DeserializeTypes> {
        let (_, raw_type) =
            map_res(alphanumeric1, |s: &[u8]| String::from_utf8(s.to_vec()))(input)?;

        //Making a nom error is awful unless you read this: https://github.com/Geal/nom/issues/1257
        let sql_type = DeserializeTypes::from_str(&raw_type).map_err(|_| {
            nom::Err::Error(nom::error::Error::new(
                "Not a known type".as_bytes(),
                nom::error::ErrorKind::ParseTo,
            ))
        })?;
        Ok((input, sql_type))
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_match_column() {
        let test_string = vec!["bar text", "bar text,bar text"];

        let (_, result) = TopLevel::match_columns(test_string[0].as_bytes()).unwrap();

        assert_eq!(vec!(test_string[0].as_bytes()), result);

        let (_, result) = TopLevel::match_columns(test_string[1].as_bytes()).unwrap();

        assert_eq!(
            vec!(test_string[0].as_bytes(), test_string[0].as_bytes()),
            result
        );
    }

    #[test]
    fn test_simple_table() {
        let test_string = b"create table foo (bar text)";

        let (_, result) = TopLevel::parse_create_table(test_string).unwrap();

        assert_eq!("foo", result.name);
    }
}
