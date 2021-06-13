use nom::branch::alt;
use nom::bytes::complete::{tag, take, take_until, take_while};
use nom::character::complete::{alphanumeric1, digit1, multispace0, multispace1};
use nom::character::is_digit;
use nom::combinator::map_res;
use nom::IResult;

pub(super) fn parse_sql_identifier(input: &str) -> IResult<&str, &str> {
    alphanumeric1(input)
    //map_res(alphanumeric1, |s: &str| String::from_utf8(s.to_vec()))(input)
}

// This parser is designed to capture valid postgres expressions and values
// Examples:
// * 'foo'
// * 'foo bar'
// * 1
// Fancier expressions will be evolved in over time
pub(super) fn parse_expression(input: &str) -> IResult<&str, String> {
    let (input, expression) = alt((parse_sql_string, parse_sql_integer))(input)?;
    let (_, expression) = convert_to_string(expression)?;
    Ok((input, expression))
}

fn parse_sql_string(input: &str) -> IResult<&str, &str> {
    let (input, _) = maybe_take_whitespace(input)?;
    let (input, _) = tag("'")(input)?;
    let (input, value) = take_until("'")(input)?;
    let (input, _) = tag("'")(input)?;
    let (input, _) = maybe_take_whitespace(input)?;
    Ok((input, value))
}

fn parse_sql_integer(input: &str) -> IResult<&str, &str> {
    let (input, _) = maybe_take_whitespace(input)?;
    let (input, num) = digit1(input)?;
    let (input, _) = maybe_take_whitespace(input)?;
    Ok((input, num))
}

//Will consume all input so be careful!
pub(super) fn convert_to_string(input: &str) -> IResult<&str, String> {
    Ok(("", input.to_string()))
    //map_res(take(input.len()), |s: &str| String::from_utf8(s.to_vec()))(input)
}

pub(super) fn maybe_take_whitespace(input: &str) -> IResult<&str, &str> {
    multispace0(input)
}
pub(super) fn take_whitespace(input: &str) -> IResult<&str, &str> {
    multispace1(input)
}

pub(super) fn match_open_paren(input: &str) -> IResult<&str, &str> {
    tag("(")(input)
}
pub(super) fn match_close_paren(input: &str) -> IResult<&str, &str> {
    tag(")")(input)
}

pub(super) fn match_comma(input: &str) -> IResult<&str, &str> {
    tag(",")(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_identifier() {
        let test = "bar.foo";

        let res = parse_sql_identifier(test);
        assert!(res.is_ok());
        let (output, value) = res.unwrap();
        assert_eq!(output.len(), 0);
        assert_eq!(test, value);
    }
}
