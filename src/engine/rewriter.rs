//! The rewrite processor take a parsed query and makes it into a set of commands that can be sequentially executed.
use super::objects::QueryTree;
use thiserror::Error;

pub struct Rewriter {}

impl Rewriter {
    pub fn rewrite(query_tree: QueryTree) -> Result<QueryTree, RewriterError> {
        Err(RewriterError::Unknown())
    }
}

#[derive(Debug, Error)]
pub enum RewriterError {
    #[error("Unknown")]
    Unknown(),
}
