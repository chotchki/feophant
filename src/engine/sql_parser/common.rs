use nom::branch::alt;
use nom::bytes::complete::{escaped_transform, is_a, tag, tag_no_case};
use nom::character::complete::{alphanumeric1, digit1, multispace0, multispace1, none_of};
use nom::combinator::{cut, map_parser, recognize};
use nom::error::{ContextError, ParseError};
use nom::multi::{many0, separated_list0, separated_list1};
use nom::sequence::{delimited, tuple};
use nom::IResult;

use crate::engine::objects::ParseExpression;

pub(super) fn parse_sql_identifier<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, E> {
    is_a("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.")(input)
}

// This parser is designed to capture valid postgres expressions and values
// Examples:
// * 'foo'
// * 'foo bar'
// * 1
// Fancier expressions will be evolved in over time
// Will consume all input so be careful!
pub(super) fn parse_expression<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, ParseExpression, E> {
    cut(alt((parse_sql_string, parse_sql_integer, parse_sql_null)))(input)
}

fn parse_sql_string<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, ParseExpression, E> {
    //Code from here: https://stackoverflow.com/a/58520871
    let seq = recognize(separated_list1(tag("''"), many0(none_of("'"))));
    let unquote = escaped_transform(none_of("'"), '\'', tag("'"));
    let (input, (_, sql_value, _)) = tuple((
        maybe_take_whitespace,
        delimited(tag("'"), map_parser(seq, unquote), tag("'")),
        maybe_take_whitespace,
    ))(input)?;

    Ok((input, ParseExpression::String(sql_value)))
}

fn parse_sql_integer<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, ParseExpression, E> {
    let (input, (_, num, _)) =
        tuple((maybe_take_whitespace, digit1, maybe_take_whitespace))(input)?;
    Ok((input, ParseExpression::String(num.to_string())))
}

fn parse_sql_null<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, ParseExpression, E> {
    let (input, (_, _, _)) = tuple((
        maybe_take_whitespace,
        tag_no_case("null"),
        maybe_take_whitespace,
    ))(input)?;
    Ok((input, ParseExpression::Null()))
}

pub(super) fn parse_column_names<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, Vec<String>, E> {
    let (input, (_, names, _)) = tuple((
        match_open_paren,
        separated_list0(match_comma, match_column_name),
        match_close_paren,
    ))(input)?;
    Ok((input, names))
}

pub(super) fn match_column_name<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, String, E> {
    let (input, (_, name, _)) =
        tuple((maybe_take_whitespace, alphanumeric1, maybe_take_whitespace))(input)?;
    Ok((input, name.to_string()))
}

pub(super) fn maybe_take_whitespace<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, E> {
    multispace0(input)
}
pub(super) fn take_whitespace<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, E> {
    multispace1(input)
}

pub(super) fn match_open_paren<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, E> {
    tag("(")(input)
}
pub(super) fn match_close_paren<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, E> {
    tag(")")(input)
}

pub(super) fn match_comma<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, E> {
    tag(",")(input)
}

#[cfg(test)]
mod tests {
    use nom::error::VerboseError;

    use super::*;

    #[test]
    fn test_sql_identifier() {
        let test = "bar.foo";

        let res = parse_sql_identifier::<VerboseError<&str>>(test);
        assert!(res.is_ok());
        let (output, value) = res.unwrap();
        assert_eq!(output.len(), 0);
        assert_eq!(test, value);
    }

    #[test]
    fn test_parse_sql_string() {
        let test = "'one''two'";
        let expected = ParseExpression::String("one'two".to_string());

        let res = parse_sql_string::<VerboseError<&str>>(test);
        let res = match res {
            Ok(o) => o,
            Err(e) => {
                println!("{} {:?}", e, e);
                panic!("Ah crap");
            }
        };
        //assert!(res.is_ok());
        let (output, value) = res;
        assert_eq!(output.len(), 0);
        assert_eq!(expected, value);
    }
}
