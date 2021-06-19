//! Top Level of the sql parsing engine

mod common;
mod create;
mod insert;

use super::objects::ParseTree;
use create::{match_create, parse_create_table};
use insert::parse_insert;
use nom::combinator::complete;
use nom::IResult;
use thiserror::Error;

pub fn parse(input: &str) -> Result<ParseTree, SqlParseError> {
    match complete(nom_parse)(input) {
        Ok((_, cmd)) => Ok(cmd),
        Err(_) => Err(SqlParseError::ParseError()),
    }
}

fn nom_parse(input: &str) -> IResult<&str, ParseTree> {
    if match_create(input).is_ok() {
        let (input, _) = match_create(input)?;

        match parse_create_table(input) {
            Ok((i, cmd)) => return Ok((i, ParseTree::CreateTable(cmd))),
            Err(_) => {}
        }
    }

    match parse_insert(input) {
        Ok((i, cmd)) => return Ok((i, ParseTree::Insert(cmd))),
        Err(_) => {}
    }

    //Fail since we have no idea what we got
    Err(nom::Err::Error(nom::error::Error::new(
        "Unable to parse",
        nom::error::ErrorKind::Complete,
    )))
}

#[derive(Debug, Error)]
pub enum SqlParseError {
    #[error("There was an error parsing, rework will be needed to make this user friendly")]
    ParseError(),
}
