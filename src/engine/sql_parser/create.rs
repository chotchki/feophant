use super::common::take_whitespace;
use nom::bytes::complete::tag_no_case;
use nom::error::{ContextError, ParseError};
use nom::IResult;

mod create_table;
pub(super) use create_table::parse_create_table;
use nom::sequence::tuple;

pub(super) fn match_create<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, (), E> {
    let (input, (_, _)) = tuple((tag_no_case("create"), take_whitespace))(input)?;
    Ok((input, ()))
}
