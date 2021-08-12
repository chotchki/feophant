pub mod analyzer;
pub use analyzer::Analyzer;
pub use analyzer::AnalyzerError;

pub mod executor;
pub use executor::Executor;
pub use executor::ExecutorError;

pub mod io;
use futures::pin_mut;
use io::{RowManager, VisibleRowManager};
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

use self::io::ConstraintManager;
use self::io::FileManager;
use self::objects::QueryResult;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;
use tokio_stream::StreamExt;

#[derive(Clone, Debug)]
pub struct Engine {
    analyzer: Analyzer,
    executor: Executor,
}

impl Engine {
    pub fn new(file_manager: Arc<FileManager>, tran_manager: TransactionManager) -> Engine {
        let vis_row_man = VisibleRowManager::new(RowManager::new(file_manager), tran_manager);
        let con_man = ConstraintManager::new(vis_row_man.clone());
        Engine {
            analyzer: Analyzer::new(vis_row_man),
            executor: Executor::new(con_man),
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

        let output_columns = query_tree.targets.iter().map(|t| t.0.clone()).collect();

        Ok(QueryResult {
            columns: output_columns,
            rows: result,
        })
    }

    fn should_bypass_planning(parse_tree: &ParseTree) -> bool {
        matches!(parse_tree.deref(), ParseTree::CreateTable(_))
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
    use tempfile::TempDir;

    use super::transactions::TransactionManager;
    use super::*;

    #[tokio::test]
    async fn create_insert_select() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let create_test = "create table foo (bar text)".to_string();
        let insert_test = "insert into foo values('test text')".to_string();
        let select_test = "select bar from foo".to_string();

        let mut transaction_manager = TransactionManager::new();
        let mut engine = Engine::new(
            Arc::new(FileManager::new(tmp_dir)?),
            transaction_manager.clone(),
        );

        let tran = transaction_manager.start_trans().await?;
        engine.process_query(tran, create_test).await?;
        transaction_manager.commit_trans(tran).await?;

        engine.process_query(tran, insert_test).await?;
        engine.process_query(tran, select_test).await?;

        Ok(())
    }
}
