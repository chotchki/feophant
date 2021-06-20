use thiserror::Error;

pub mod analyzer;
pub use analyzer::Analyzer;
pub use analyzer::AnalyzerError;

pub mod executor;
pub use executor::Executor;
pub use executor::ExecutorError;

pub mod io;
pub mod objects;

pub mod planner;
pub use planner::Planner;
pub use planner::PlannerError;

pub mod rewriter;
pub use rewriter::Rewriter;
pub use rewriter::RewriterError;

pub mod sql_parser;
pub use sql_parser::SqlParser;
pub use sql_parser::SqlParserError;

pub mod transactions;

pub struct Engine {}

impl Engine {
    pub fn process_query(query: String) -> Result<(), EngineError> {
        //Parse it - I need to figure out if I should do statement splitting here
        let parse_tree = SqlParser::parse(&query)?;

        //Analyze it
        let query_tree = Analyzer::analyze(parse_tree)?;

        //Rewrite it
        let rewrite_tree = Rewriter::rewrite(query_tree)?;

        //Plan it
        let planned_stmt = Planner::plan(rewrite_tree)?;

        Executor::execute(planned_stmt)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    AnalyzerError(#[from] AnalyzerError),
    #[error(transparent)]
    ExecutorError(#[from] ExecutorError),
    #[error(transparent)]
    QueryNotUtf8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    RewriterError(#[from] RewriterError),
    #[error(transparent)]
    ParseError(#[from] SqlParserError),
    #[error(transparent)]
    PlannerError(#[from] PlannerError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn create_insert_select() {
        let create_test = "create table foo (bar text)".to_string();
        let insert_test = "insert into foo value('test text')".to_string();
        let select_test = "select bar from foo".to_string();

        assert!(Engine::process_query(create_test).is_ok());
        assert!(Engine::process_query(insert_test).is_ok());
        assert!(Engine::process_query(select_test).is_ok());
    }
}
