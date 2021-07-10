//! Format here: https://www.postgresql.org/docs/current/sql-insert.html
//! This is only implementing a basic insert, fancy will come later

use crate::engine::objects::ParseTree;

use super::super::objects::RawInsertCommand;
use super::common::{
    match_close_paren, match_comma, match_open_paren, maybe_take_whitespace, parse_expression,
    parse_sql_identifier, take_whitespace,
};
use nom::bytes::complete::tag_no_case;
use nom::character::complete::alphanumeric1;
use nom::combinator::{cut, opt};
use nom::error::{ContextError, ParseError};
use nom::multi::separated_list0;
use nom::sequence::tuple;
use nom::IResult;

pub(super) fn parse_insert<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, ParseTree, E> {
    let (input, (_, (_, table_name, _, provided_columns, _, provided_values))) = tuple((
        match_insert_into,
        cut(tuple((
            take_whitespace,
            parse_sql_identifier,
            maybe_take_whitespace,
            opt(parse_column_names),
            match_values,
            parse_values,
        ))),
    ))(input)?;

    let raw_ins = RawInsertCommand {
        table_name: table_name.to_string(),
        provided_columns,
        provided_values,
    };

    Ok((input, ParseTree::Insert(raw_ins)))
}

fn match_insert_into<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, (_, _, _)) =
        tuple((tag_no_case("insert"), take_whitespace, tag_no_case("into")))(input)?;
    Ok((input, ()))
}

//TODO candidate for moving into common
fn match_column_name<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, String, E> {
    let (input, (_, name, _)) =
        tuple((maybe_take_whitespace, alphanumeric1, maybe_take_whitespace))(input)?;
    Ok((input, name.to_string()))
}

fn parse_column_names<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, Vec<String>, E> {
    let (input, (_, names, _)) = tuple((
        match_open_paren,
        separated_list0(match_comma, match_column_name),
        match_close_paren,
    ))(input)?;
    Ok((input, names))
}

fn match_values<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, (_, _, _, _)) = tuple((
        maybe_take_whitespace,
        tag_no_case("values"),
        maybe_take_whitespace,
        match_open_paren,
    ))(input)?;
    Ok((input, ()))
}

fn parse_values<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, Vec<String>, E> {
    let (input, (values, _)) = tuple((
        separated_list0(match_comma, parse_expression),
        match_close_paren,
    ))(input)?;
    Ok((input, values))
}

#[cfg(test)]
mod tests {
    use nom::error::VerboseError;

    use super::*;

    #[test]
    fn test_simple_insert() -> Result<(), Box<dyn std::error::Error>> {
        let test = "insert into foo (first, second,third) values('stuff and things', 2)";

        let (output, value) = parse_insert::<VerboseError<&str>>(test)?;

        let value = match value {
            ParseTree::Insert(i) => i,
            _ => panic!("Wrong type"),
        };
        assert_eq!(output.len(), 0);

        let expected = RawInsertCommand {
            table_name: "foo".to_string(),
            provided_columns: Some(vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string(),
            ]),
            provided_values: vec!["stuff and things".to_string(), "2".to_string()],
        };
        assert_eq!(expected, value);

        Ok(())
    }
}
