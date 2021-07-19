pub mod analyzer;
pub use analyzer::Analyzer;
pub use analyzer::AnalyzerError;

pub mod executor;
pub use executor::Executor;
pub use executor::ExecutorError;

pub mod io;
use futures::pin_mut;
use io::{IOManager, RowManager, VisibleRowManager};
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
use transactions::{TransactionId, TransactionManager};

use self::objects::QueryResult;
use crate::engine::objects::TargetEntry;
use std::ops::Deref;
use thiserror::Error;
use tokio_stream::StreamExt;

#[derive(Clone, Debug)]
pub struct Engine {
    analyzer: Analyzer,
    executor: Executor,
}

impl Engine {
    pub fn new(io_manager: IOManager, tran_manager: TransactionManager) -> Engine {
        let vis_row_man = VisibleRowManager::new(RowManager::new(io_manager), tran_manager);
        Engine {
            analyzer: Analyzer::new(vis_row_man.clone()),
            executor: Executor::new(vis_row_man),
        }
    }

    pub async fn process_query(
        &mut self,
        tran_id: TransactionId,
        query: String,
    ) -> Result<QueryResult, EngineError> {
        //Parse it - I need to figure out if I should do statement splitting here
        let parse_tree = SqlParser::parse(&query)?;

        if Engine::should_bypass_planning(&parse_tree) {
            let output_rows = self.executor.execute_utility(tran_id, parse_tree).await?;
            return Ok(QueryResult {
                columns: vec![],
                rows: output_rows,
            });
        }

        //Analyze it
        let query_tree = self.analyzer.analyze(tran_id, parse_tree).await?;

        //Rewrite it - noop for right now
        let rewrite_tree = Rewriter::rewrite(query_tree.clone())?;

        //Plan it
        let planned_stmt = Planner::plan(rewrite_tree)?;

        //Execute it, single shot for now
        let mut result = vec![];
        let execute_stream = self.executor.clone().execute(tran_id, planned_stmt);
        pin_mut!(execute_stream);

        while let Some(value) = execute_stream.next().await {
            result.push(value?);
        }

        let output_columns = query_tree
            .targets
            .into_iter()
            .map(|t| match t {
                TargetEntry::Parameter(p) => p.name,
            })
            .collect();

        return Ok(QueryResult {
            columns: output_columns,
            rows: result,
        });
    }

    fn should_bypass_planning(parse_tree: &ParseTree) -> bool {
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

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    fn create_insert_select() -> Result<(), Box<dyn std::error::Error>> {
        let create_test = "create table foo (bar text)".to_string();
        let insert_test = "insert into foo values('test text')".to_string();
        let select_test = "select bar from foo".to_string();

        let mut transaction_manager = TransactionManager::new();
        let mut engine = Engine::new(IOManager::new(), transaction_manager.clone());

        let tran = aw!(transaction_manager.start_trans())?;
        aw!(engine.process_query(tran, create_test))?;
        aw!(transaction_manager.commit_trans(tran))?;

        aw!(engine.process_query(tran, insert_test))?;
        aw!(engine.process_query(tran, select_test))?;

        Ok(())
    }
}
