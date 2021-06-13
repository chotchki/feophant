//! Top Level of the sql parsing engine
use super::create::{match_create, parse_create_table, RawCreateTableCommand};
use super::insert::RawInsertCommand;
use nom::combinator::complete;
use nom::IResult;
use thiserror::Error;

pub enum SqlCommand {
    CreateTable(RawCreateTableCommand),
    Insert(RawInsertCommand),
}

pub fn parse(input: &str) -> Result<SqlCommand, SqlParseError> {
    match complete(nom_parse)(input) {
        Ok((_, cmd)) => Ok(cmd),
        Err(_) => Err(SqlParseError::ParseError()),
    }
}

fn nom_parse(input: &str) -> IResult<&str, SqlCommand> {
    if match_create(input).is_ok() {
        let (input, _) = match_create(input)?;

        let result = parse_create_table(input);
        if result.is_ok() {
            return Ok((input, SqlCommand::CreateTable(result.unwrap().1)));
        }
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
