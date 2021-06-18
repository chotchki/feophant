//! Format here: https://www.postgresql.org/docs/current/sql-insert.html
//! This is only implementing a basic insert, fancy will come later

use super::super::objects::RawInsertCommand;
use super::common::{
    match_close_paren, match_comma, match_open_paren, maybe_take_whitespace, parse_expression,
    parse_sql_identifier, take_whitespace,
};
use nom::bytes::complete::tag_no_case;
use nom::character::complete::alphanumeric1;
use nom::combinator::opt;
use nom::multi::separated_list0;
use nom::IResult;

pub(super) fn parse_insert(input: &str) -> IResult<&str, RawInsertCommand> {
    let (input, _) = match_insert_into(input)?;
    let (input, _) = take_whitespace(input)?;
    let (input, table_name) = parse_sql_identifier(input)?;
    let (input, _) = maybe_take_whitespace(input)?;

    let (input, provided_columns) = opt(parse_column_names)(input)?;

    let (input, _) = match_values(input)?;

    let (input, provided_values) = parse_values(input)?;

    let raw_ins = RawInsertCommand {
        table_name: table_name.to_string(),
        provided_columns,
        provided_values,
    };

    Ok((input, raw_ins))
}

fn match_insert_into(input: &str) -> IResult<&str, &str> {
    let (input, _) = tag_no_case("insert")(input)?;
    let (input, _) = take_whitespace(input)?;
    tag_no_case("into")(input)
}

//TODO candidate for moving into common
fn match_column_name(input: &str) -> IResult<&str, String> {
    let (input, _) = maybe_take_whitespace(input)?;
    let (input, name) = alphanumeric1(input)?;
    let (input, _) = maybe_take_whitespace(input)?;
    Ok((input, name.to_string()))
}

fn parse_column_names(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = match_open_paren(input)?;
    let (input, names) = separated_list0(match_comma, match_column_name)(input)?;
    let (input, _) = match_close_paren(input)?;
    Ok((input, names))
}

fn match_values(input: &str) -> IResult<&str, &str> {
    let (input, _) = maybe_take_whitespace(input)?;
    let (input, _) = tag_no_case("values")(input)?;
    let (input, _) = maybe_take_whitespace(input)?;
    match_open_paren(input)
}

fn parse_values(input: &str) -> IResult<&str, Vec<String>> {
    let (input, values) = separated_list0(match_comma, parse_expression)(input)?;
    let (input, _) = match_close_paren(input)?;
    Ok((input, values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_insert() {
        let test = "insert into foo (hah,he) values('stuff and things', 2)";

        let res = parse_insert(test);
        assert!(res.is_ok());

        let (output, value) = res.unwrap();
        assert_eq!(output.len(), 0);

        let expected = RawInsertCommand {
            table_name: "foo".to_string(),
            provided_columns: Some(vec!["hah".to_string(), "he".to_string()]),
            provided_values: vec!["stuff and things".to_string(), "2".to_string()],
        };
        assert_eq!(expected, value);
    }
}
