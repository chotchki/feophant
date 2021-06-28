pub mod analyzer;
pub use analyzer::Analyzer;
pub use analyzer::AnalyzerError;

pub mod executor;
pub use executor::Executor;
pub use executor::ExecutorError;

pub mod io;
use io::RowManager;
pub mod objects;
use objects::ParseTree;

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
use transactions::TransactionId;

use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;

pub struct Engine {
    executor: Executor,
}

impl Engine {
    pub fn new(row_manager: RowManager) -> Engine {
        Engine {
            executor: Executor::new(row_manager),
        }
    }

    pub async fn process_query(
        &mut self,
        tran_id: TransactionId,
        query: String,
    ) -> Result<(), EngineError> {
        //Parse it - I need to figure out if I should do statement splitting here
        let parse_tree = SqlParser::parse(&query)?;

        if Engine::should_bypass_planning(parse_tree.clone()) {
            return Ok(self.executor.execute_utility(tran_id, parse_tree).await?);
        }

        //Analyze it
        let query_tree = Analyzer::analyze(parse_tree)?;

        //Rewrite it
        let rewrite_tree = Rewriter::rewrite(query_tree)?;

        //Plan it
        let planned_stmt = Planner::plan(rewrite_tree)?;

        Executor::execute(planned_stmt)?;
        Ok(())
    }

    fn should_bypass_planning(parse_tree: Arc<ParseTree>) -> bool {
        match parse_tree.deref() {
            ParseTree::CreateTable(_) => true,
            _ => false,
        }
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
    use super::io::IOManager;
    use super::transactions::TransactionManager;
    use super::*;
    use tokio::sync::RwLock;

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    fn create_insert_select() {
        let create_test = "create table foo (bar text)".to_string();
        let insert_test = "insert into foo values('test text')".to_string();
        let select_test = "select bar from foo".to_string();

        let mut transaction_manager = TransactionManager::new();
        let row_manager = RowManager::new(Arc::new(RwLock::new(IOManager::new())));
        let mut engine = Engine::new(row_manager);

        let tran = aw!(transaction_manager.start_trans()).unwrap();
        assert_eq!(aw!(engine.process_query(tran, create_test)).unwrap(), ());
        aw!(transaction_manager.commit_trans(tran)).unwrap();

        //assert_eq!(aw!(engine.process_query(tran, insert_test)).unwrap(), ());
        //assert!(aw!(engine.process_query(tran, select_test)).is_ok());
    }
}
