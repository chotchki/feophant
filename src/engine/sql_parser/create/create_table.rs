//! Format here: https://www.postgresql.org/docs/current/sql-createtable.html
//! This is only implementing a basic create table, fancy will come later

use super::super::super::objects::RawCreateTableCommand;
use super::super::common::{
    match_close_paren, match_comma, match_open_paren, maybe_take_whitespace, parse_sql_identifier,
    take_whitespace,
};
use nom::bytes::complete::tag_no_case;
use nom::multi::separated_list1;
use nom::IResult;

pub fn parse_create_table(input: &str) -> IResult<&str, RawCreateTableCommand> {
    let (input, _) = match_table(input)?;
    let (input, _) = take_whitespace(input)?;
    let (input, table_name) = parse_sql_identifier(input)?;
    let (input, _) = maybe_take_whitespace(input)?;

    let (input, _) = match_open_paren(input)?;
    let (input, provided_columns) = separated_list1(match_comma, match_column_and_type)(input)?;
    let (input, _) = match_close_paren(input)?;

    Ok((
        input,
        RawCreateTableCommand {
            table_name: table_name.to_string(),
            provided_columns,
        },
    ))
}

fn match_table(input: &str) -> IResult<&str, &str> {
    tag_no_case("table")(input)
}

fn match_column_and_type(input: &str) -> IResult<&str, (String, String)> {
    let (input, _) = maybe_take_whitespace(input)?;
    let (input, name) = parse_sql_identifier(input)?;
    let (input, _) = take_whitespace(input)?;
    let (input, sql_type) = parse_sql_identifier(input)?;
    let (input, _) = maybe_take_whitespace(input)?;
    Ok((input, (name.to_string(), sql_type.to_string())))
}

//This only supports builtin types at the moment, I will need to adjust to user supplied in the future
// fn match_sql_type(input: &str) -> IResult<&str, DeserializeTypes> {
//     let (_, raw_type) = map_res(alphanumeric1, |s: &str| String::from_utf8(s.to_vec()))(input)?;

//     //Making a nom error is awful unless you read this: https://github.com/Geal/nom/issues/1257
//     let sql_type = DeserializeTypes::from_str(&raw_type).map_err(|_| {
//         nom::Err::Error(nom::error::Error::new(
//             "Not a known type".as_bytes(),
//             nom::error::ErrorKind::ParseTo,
//         ))
//     })?;
//     Ok((input, sql_type))
// }

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_simple_table() {
        let test_string = "table foo (bar text)";

        let (_, result) = parse_create_table(test_string).unwrap();

        assert_eq!("foo", result.table_name);
        assert_eq!(
            vec!(("bar".to_string(), "text".to_string())),
            result.provided_columns
        );
    }
}
