//! Top Level of the sql parsing engine

mod common;
mod create;
mod insert;

use super::objects::ParseTree;
use create::parse_create_table;
use insert::parse_insert;
use nom::branch::alt;
use nom::combinator::{all_consuming, complete, cut};
use nom::error::{convert_error, ContextError, ParseError, VerboseError};
use nom::IResult;
use nom::{Err, Finish};
use thiserror::Error;

pub struct SqlParser {}

impl SqlParser {
    pub fn parse(input: &str) -> Result<ParseTree, SqlParserError> {
        match SqlParser::nom_parse::<VerboseError<&str>>(input).finish() {
            Ok((_, cmd)) => Ok(cmd),
            Err(e) => Err(SqlParserError::ParseError(convert_error(input, e))),
        }
    }

    fn nom_parse<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
        input: &'a str,
    ) -> IResult<&'a str, ParseTree, E> {
        complete(all_consuming(alt((parse_create_table, parse_insert))))(input)
    }
}

#[derive(Debug, Error)]
pub enum SqlParserError {
    #[error("SQL Parse Error {0}")]
    ParseError(String),
    #[error("Got an incomplete on {0} which shouldn't be possible")]
    Incomplete(String),
}
