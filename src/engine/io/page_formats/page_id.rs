//! A struct to uniquely identify a page in all operations. This replaces adding additional arguments everywhere.

use nom::{
    bytes::complete::tag_no_case,
    error::{convert_error, make_error, ContextError, ErrorKind, ParseError, VerboseError},
    Finish, IResult,
};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct PageId {
    pub resource_key: Uuid,
    pub page_type: PageType,
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum PageType {
    Data,
    //FreeSpaceMap,
    //VisibilityMap
}

impl PageType {
    pub fn parse_type<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
        input: &'a str,
    ) -> IResult<&'a str, PageType, E> {
        let (input, matched) = tag_no_case("data")(input)?;

        let page_type = match matched {
            "data" => PageType::Data,
            _ => {
                return Err(nom::Err::Failure(make_error(input, ErrorKind::Fix)));
            }
        };
        Ok((input, page_type))
    }
}

impl Display for PageType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PageType::Data => {
                write!(f, "data")
            }
        }
    }
}

impl FromStr for PageType {
    type Err = PageTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Self::parse_type::<VerboseError<&str>>(s).finish() {
            Ok((_, page_type)) => Ok(page_type),
            Err(e) => Err(PageTypeError::ParseError(convert_error(s, e))),
        }
    }
}

#[derive(Debug, Error)]
pub enum PageTypeError {
    #[error("Page Type Parse Error {0}")]
    ParseError(String),
}
