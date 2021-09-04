use nom::{
    bytes::complete::tag_no_case,
    error::{make_error, ContextError, ErrorKind, ParseError},
    IResult,
};

use super::ConstraintMapper;

pub fn parse_constraint<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, ConstraintMapper, E> {
    let (input, matched) = tag_no_case("PrimaryKey")(input)?;

    let constraint_type = match matched {
        "PrimaryKey" => ConstraintMapper::PrimaryKey,
        _ => {
            return Err(nom::Err::Failure(make_error(input, ErrorKind::Fix)));
        }
    };
    Ok((input, constraint_type))
}
