//! Top Level of the sql parsing engine
use super::create::{match_create, parse_create_table, RawCreateTableCommand};
use super::insert::{parse_insert, RawInsertCommand};
use nom::combinator::complete;
use nom::IResult;
use thiserror::Error;

pub enum RawSqlCommand {
    CreateTable(RawCreateTableCommand),
    Insert(RawInsertCommand),
}

pub fn parse(input: &str) -> Result<RawSqlCommand, SqlParseError> {
    match complete(nom_parse)(input) {
        Ok((_, cmd)) => Ok(cmd),
        Err(_) => Err(SqlParseError::ParseError()),
    }
}

fn nom_parse(input: &str) -> IResult<&str, RawSqlCommand> {
    if match_create(input).is_ok() {
        let (input, _) = match_create(input)?;

        match parse_create_table(input) {
            Ok((i, cmd)) => return Ok((i, RawSqlCommand::CreateTable(cmd))),
            Err(_) => {}
        }
    }

    match parse_insert(input) {
        Ok((i, cmd)) => return Ok((i, RawSqlCommand::Insert(cmd))),
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
