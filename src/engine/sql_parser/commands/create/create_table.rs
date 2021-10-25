//! Format here: https://www.postgresql.org/docs/current/sql-createtable.html
//! This is only implementing a basic create table, fancy will come later

use crate::engine::objects::{ParseTree, RawColumn};

use super::super::super::super::objects::RawCreateTableCommand;
use super::super::super::common::{
    match_close_paren, match_comma, match_open_paren, maybe_take_whitespace, parse_sql_identifier,
    take_whitespace,
};
use super::match_create;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{cut, opt};
use nom::error::{ContextError, ParseError};
use nom::multi::separated_list1;
use nom::sequence::tuple;
use nom::IResult;

pub fn parse_create_table<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, ParseTree, E> {
    let (input, (_, _, (_, table_name, _, _, provided_columns, _))) = tuple((
        match_create,
        match_table,
        cut(tuple((
            take_whitespace,
            parse_sql_identifier,
            maybe_take_whitespace,
            match_open_paren,
            match_columns,
            match_close_paren,
        ))),
    ))(input)?;

    Ok((
        input,
        ParseTree::CreateTable(RawCreateTableCommand {
            table_name: table_name.to_string(),
            provided_columns,
        }),
    ))
}

fn match_table<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, _) = tag_no_case("table")(input)?;
    Ok((input, ()))
}

fn match_columns<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, Vec<RawColumn>, E> {
    separated_list1(match_comma, match_column_attribute)(input)
}

fn match_column_attribute<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, RawColumn, E> {
    let (input, (_, name, _, sql_type, _, is_null, _, is_primary_key, _)) = tuple((
        maybe_take_whitespace,
        parse_sql_identifier,
        take_whitespace,
        parse_sql_identifier,
        maybe_take_whitespace,
        is_null,
        maybe_take_whitespace,
        is_primary_key,
        maybe_take_whitespace,
    ))(input)?;
    Ok((
        input,
        RawColumn {
            name: name.to_string(),
            sql_type: sql_type.to_string(),
            null: is_null,
            primary_key: is_primary_key,
        },
    ))
}

fn is_null<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, bool, E> {
    let (input, not_null) = opt(match_not_null)(input)?;
    if not_null.is_some() {
        return Ok((input, false));
    }
    let (input, null) = opt(match_null)(input)?;
    if null.is_some() {
        return Ok((input, true));
    }
    Ok((input, true))
}

fn is_primary_key<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, bool, E> {
    let (input, primary_key) = opt(match_primary_key)(input)?;
    match primary_key {
        Some(()) => Ok((input, true)),
        None => Ok((input, false)),
    }
}

fn match_not_null<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, (_, _, _)) =
        tuple((tag_no_case("not"), take_whitespace, tag_no_case("null")))(input)?;
    Ok((input, ()))
}

fn match_null<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, _) = tag_no_case("null")(input)?;
    Ok((input, ()))
}

fn match_primary_key<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, (_, _, _)) =
        tuple((tag_no_case("primary"), take_whitespace, tag_no_case("key")))(input)?;
    Ok((input, ()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::error::{convert_error, VerboseError};

    #[test]
    fn test_simple_table() -> Result<(), Box<dyn std::error::Error>> {
        let test_string = "create table foo (bar text primary key, baz text not null)";

        let (_, result) = parse_create_table::<VerboseError<&str>>(test_string)?;

        let result = match result {
            ParseTree::CreateTable(c) => c,
            _ => panic!("Wrong type"),
        };

        assert_eq!("foo", result.table_name);

        let columns = vec![
            RawColumn {
                name: "bar".to_string(),
                sql_type: "text".to_string(),
                null: true,
                primary_key: true,
            },
            RawColumn {
                name: "baz".to_string(),
                sql_type: "text".to_string(),
                null: false,
                primary_key: false,
            },
        ];
        assert_eq!(columns, result.provided_columns);
        Ok(())
    }

    #[test]
    fn test_nullable_columns() -> Result<(), Box<dyn std::error::Error>> {
        let test_string = "create table foo (bar text, test text null)";
        let res = parse_create_table::<VerboseError<&str>>(test_string);

        let (_, _result) = match res {
            Ok(o) => o,
            Err(nom::Err::Failure(e)) | Err(nom::Err::Error(e)) => {
                println!("{0}", convert_error(test_string, e));
                panic!();
            }
            _ => {
                panic!("Whatever")
            }
        };
        Ok(())
    }
}
