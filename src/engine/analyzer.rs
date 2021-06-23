use super::objects::{CommandType, ParseTree, QueryTree, RawCreateTableCommand};
use std::sync::Arc;
use thiserror::Error;

pub struct Analyzer {}

impl Analyzer {
    pub fn analyze(parse_tree: Arc<ParseTree>) -> Result<QueryTree, AnalyzerError> {
        match parse_tree {
            _ => return Err(AnalyzerError::NotImplemented()),
        }
    }
}

#[derive(Debug, Error)]
pub enum AnalyzerError {
    #[error("Not implemented")]
    NotImplemented(),
    #[error("Unknown")]
    Unknown(),
}
