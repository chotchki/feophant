use nom::{
    bytes::complete::tag_no_case,
    combinator::cut,
    error::{ContextError, ParseError},
    multi::separated_list0,
    sequence::tuple,
    IResult,
};

use crate::engine::objects::{ParseTree, RawSelectCommand};

use super::common::{
    match_column_name, match_comma, maybe_take_whitespace, parse_sql_identifier, take_whitespace,
};

pub(super) fn parse_select<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, ParseTree, E> {
    let (input, (_, (columns, _, _, table))) = tuple((
        match_select,
        cut(tuple((
            separated_list0(match_comma, match_column_name),
            maybe_take_whitespace,
            match_from,
            parse_sql_identifier,
        ))),
    ))(input)?;

    let raw_sel = RawSelectCommand {
        table: table.to_string(),
        columns,
    };

    Ok((input, ParseTree::Select(raw_sel)))
}

pub(super) fn match_select<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, (_, _)) = tuple((tag_no_case("select"), take_whitespace))(input)?;
    Ok((input, ()))
}

pub(super) fn match_from<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, (_, _)) = tuple((tag_no_case("from"), take_whitespace))(input)?;
    Ok((input, ()))
}

#[cfg(test)]
mod tests {
    use nom::error::VerboseError;

    use crate::engine::objects::RawSelectCommand;

    use super::*;

    #[test]
    fn test_select_parser() -> Result<(), Box<dyn std::error::Error>> {
        let test = "select foo, bar from baz";

        let (output, value) = parse_select::<VerboseError<&str>>(test)?;

        let value = match value {
            ParseTree::Select(s) => s,
            _ => panic!("Wrong type"),
        };
        assert_eq!(output.len(), 0);

        let expected = RawSelectCommand {
            table: "baz".to_string(),
            columns: vec!["foo".to_string(), "bar".to_string()],
        };
        assert_eq!(expected, value);

        Ok(())
    }
}
