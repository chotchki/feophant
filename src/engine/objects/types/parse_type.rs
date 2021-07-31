use std::sync::Arc;

use nom::{
    branch::alt,
    bytes::complete::tag_no_case,
    error::{make_error, ContextError, ErrorKind, ParseError},
    IResult,
};

use super::BaseSqlTypesMapper;

pub fn parse_type<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, BaseSqlTypesMapper, E> {
    let (input, matched) = alt((
        tag_no_case("bool"),
        tag_no_case("integer"),
        tag_no_case("text"),
        tag_no_case("uuid"),
        tag_no_case("array(bool)"),
        tag_no_case("array(integer)"),
        tag_no_case("array(text)"),
        tag_no_case("array(uuid)"),
    ))(input)?;

    let sql_type = match matched {
        "bool" => BaseSqlTypesMapper::Bool,
        "integer" => BaseSqlTypesMapper::Integer,
        "text" => BaseSqlTypesMapper::Text,
        "uuid" => BaseSqlTypesMapper::Uuid,
        "array(bool)" => BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Bool)),
        "array(integer)" => BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Integer)),
        "array(text)" => BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Text)),
        "array(uuid)" => BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Uuid)),
        _ => {
            return Err(nom::Err::Failure(make_error(input, ErrorKind::Fix)));
        }
    };
    Ok((input, sql_type))
}

#[cfg(test)]
mod tests {
    use nom::error::VerboseError;
    use nom::Finish;

    use super::*;

    #[test]
    fn test_mapping() -> Result<(), Box<dyn std::error::Error>> {
        let (_, res) = parse_type::<VerboseError<&str>>("bool").finish()?;
        assert_eq!(res, BaseSqlTypesMapper::Bool);
        let (_, res) = parse_type::<VerboseError<&str>>("integer").finish()?;
        assert_eq!(res, BaseSqlTypesMapper::Integer);
        let (_, res) = parse_type::<VerboseError<&str>>("text").finish()?;
        assert_eq!(res, BaseSqlTypesMapper::Text);
        let (_, res) = parse_type::<VerboseError<&str>>("uuid").finish()?;
        assert_eq!(res, BaseSqlTypesMapper::Uuid);

        let (_, res) = parse_type::<VerboseError<&str>>("array(bool)").finish()?;
        assert_eq!(
            res,
            BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Bool))
        );
        let (_, res) = parse_type::<VerboseError<&str>>("array(integer)").finish()?;
        assert_eq!(
            res,
            BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Integer))
        );
        let (_, res) = parse_type::<VerboseError<&str>>("array(text)").finish()?;
        assert_eq!(
            res,
            BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Text))
        );
        let (_, res) = parse_type::<VerboseError<&str>>("array(uuid)").finish()?;
        assert_eq!(
            res,
            BaseSqlTypesMapper::Array(Arc::new(BaseSqlTypesMapper::Uuid))
        );
        Ok(())
    }
}
