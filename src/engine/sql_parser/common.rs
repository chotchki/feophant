use nom::branch::alt;
use nom::bytes::complete::{escaped, escaped_transform, is_a, is_not, tag, take_until};
use nom::character::complete::{alphanumeric1, digit1, multispace0, multispace1, none_of};
use nom::combinator::{cut, map, map_parser, recognize, value};
use nom::error::{ContextError, ParseError};
use nom::multi::{many0, separated_list0, separated_list1};
use nom::sequence::{delimited, tuple};
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
    cut(alt((parse_sql_string, parse_sql_integer)))(input)
}

fn parse_sql_string<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, String, E> {
    //Code from here: https://stackoverflow.com/a/58520871
    let seq = recognize(separated_list1(tag("''"), many0(none_of("'"))));
    let unquote = escaped_transform(none_of("'"), '\'', tag("'"));
    delimited(tag("'"), map_parser(seq, unquote), tag("'"))(input)
}

fn parse_sql_integer<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, String, E> {
    let (input, (_, num, _)) =
        tuple((maybe_take_whitespace, digit1, maybe_take_whitespace))(input)?;
    Ok((input, num.to_string()))
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
        let expected = "one'two";

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
