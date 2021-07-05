use nom::branch::alt;
use nom::bytes::complete::{is_a, tag, take_until};
use nom::character::complete::{digit1, multispace0, multispace1};
use nom::combinator::{cut, map};
use nom::error::{ContextError, ParseError};
use nom::sequence::tuple;
use nom::IResult;

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
) -> IResult<&'a str, String, E> {
    map(
        cut(alt((parse_sql_string, parse_sql_integer))),
        |s: &str| s.to_string(),
    )(input)
}

fn parse_sql_string<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, E> {
    let (input, (_, _, value, _, _)) = tuple((
        maybe_take_whitespace,
        tag("'"),
        take_until("'"),
        tag("'"),
        maybe_take_whitespace,
    ))(input)?;
    Ok((input, value))
}

fn parse_sql_integer<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, E> {
    let (input, (_, num, _)) =
        tuple((maybe_take_whitespace, digit1, maybe_take_whitespace))(input)?;
    Ok((input, num))
}

//pub(super) fn convert_to_string<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
//    input: &'a str,
//) -> IResult<&'a str, String, E> {
//    map(all_consuming, |s: &str| s.to_string())(input)
//}

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
}
