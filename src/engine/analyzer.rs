use super::objects::{ParseTree, QueryTree};
use thiserror::Error;

pub struct Analyzer {}

impl Analyzer {
    pub fn analyze(parse_tree: ParseTree) -> Result<QueryTree, AnalyzerError> {
        Err(AnalyzerError::Unknown())
    }
}

#[derive(Debug, Error)]
pub enum AnalyzerError {
    #[error("Unknown")]
    Unknown(),
}
