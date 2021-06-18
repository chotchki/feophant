use super::common::take_whitespace;
use nom::bytes::complete::tag_no_case;
use nom::IResult;

mod create_table;
pub(super) use create_table::parse_create_table;

pub(super) fn match_create(input: &str) -> IResult<&str, &str> {
    let (input, _) = tag_no_case("create")(input)?;
    take_whitespace(input)
}
