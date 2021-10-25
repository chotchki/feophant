use std::iter::FromIterator;

use crate::engine::objects::ParseExpression;
use nom::{
    branch::alt,
    bytes::complete::{
        escaped_transform, is_not, tag, take_till, take_until,
        take_until_parser_matches_and_consume,
    },
    character::complete::{anychar, line_ending, none_of, space0},
    combinator::{map_parser, not, peek, recognize, rest, value},
    error::{ContextError, ParseError},
    multi::{fold_many1, many0, many1, many_till, separated_list1},
    sequence::{delimited, terminated, tuple},
    IResult,
};

/// SQL string constants have wacky syntax, they are surrounded by single quotes
///
/// Two single quotes escape into a single quote
///
/// Two single quotes with a newline + any other whitespace merge into a single string
pub fn parse_sql_string<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, ParseExpression, E> {
    let (input, _) = tag("'")(input)?;
    let (input, buffer) = take_until_parser_matches_and_consume(sql_string_end_tag)(input)?;

    let (_, buffer) = escaped_transform(is_not("'"), '\'', value("'", tag("'")))(buffer)?;
    /*let (input, _) = tag("'")(input)?;
    let mut buffer = String::new();

    loop {
        let (input, chunk) = take_until("'")(input)?;
        buffer += chunk;

        if is_end_tag::<'a, E>(input) {
            return Ok(("", ParseExpression::String(buffer)));
        } else {
            buffer += "'";
        }
    }
    */

    //let raw_sql_string = String::from_iter(raw_sql_string);
    //let raw_sql_str = raw_sql_string.as_str();

    //We have a sql string but now need to clean up the escapes
    //let (input, sql_quote_escape) =
    //    escaped_transform(none_of("'"), '\'', tag("'"))(raw_sql_string)?;

    //let seq = recognize(separated_list1(tag("''"), many0(none_of("'"))));
    //let unquote = escaped_transform(none_of("'"), '\'', tag("'"));
    //let (input, sql_value) =
    //   tuple((delimited(tag("'"), map_parser(seq, unquote), tag("'")),))(input)?;
    Ok((input, ParseExpression::String(buffer)))
}

fn sql_string_end_tag<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, _) = tag("'")(input)?;
    let (_, _) = peek(not(tag("'")))(input)?;
    let (_, _) = peek(not(tuple((whitespace_with_newline, tag("'")))))(input)?;

    Ok((input, ()))
}

fn whitespace_with_newline<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, String, E> {
    let (input, (a, b, c)) = tuple((space0, many1(line_ending), space0))(input)?;
    Ok((input, a.to_string() + &b.join("") + c))
}

#[cfg(test)]
mod tests {
    use nom::error::VerboseError;

    use super::*;

    #[test]
    fn test_parse_sql_string() -> Result<(), Box<dyn std::error::Error>> {
        let test = "'one two'";
        let expected = ParseExpression::String("one two".to_string());

        let (remaining, parsed) = parse_sql_string::<VerboseError<&str>>(test)?;

        assert_eq!(remaining, "");
        assert_eq!(expected, parsed);
        Ok(())
    }

    #[test]
    fn test_parse_sql_string_quote() -> Result<(), Box<dyn std::error::Error>> {
        let test = "'one''two'";
        let expected = ParseExpression::String("one'two".to_string());

        let (remaining, parsed) = parse_sql_string::<VerboseError<&str>>(test)?;

        assert_eq!(remaining, "");
        assert_eq!(expected, parsed);
        Ok(())
    }

    #[test]
    fn test_parse_sql_string_quote_newline() -> Result<(), Box<dyn std::error::Error>> {
        let test = "'one' \n 'two'";
        let expected = ParseExpression::String("onetwo".to_string());

        let (remaining, parsed) = parse_sql_string::<VerboseError<&str>>(test)?;

        assert_eq!(remaining, "");
        assert_eq!(expected, parsed);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_parse_bad_sql_string() {
        let test = "'one two";
        parse_sql_string::<VerboseError<&str>>(test).unwrap();
    }

    #[test]
    fn test_parse_end_tags() -> Result<(), Box<dyn std::error::Error>> {
        let test = "'";
        let (remaining, _) = sql_string_end_tag::<VerboseError<&str>>(test)?;
        assert_eq!(remaining, "");

        let test = "' \n '";
        let res = sql_string_end_tag::<VerboseError<&str>>(test);
        assert!(res.is_err());

        Ok(())
    }
}
