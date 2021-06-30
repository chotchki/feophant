//! The analyzer should check that tables and columns exist before allowing a query to proceed.
//! More features will come I'm sure
mod definition_lookup;
use definition_lookup::{DefinitionLookup, DefinitionLookupError};

use super::io::VisibleRowManager;
use super::objects::{CommandType, ParseTree, QueryTree, RawCreateTableCommand};
use super::transactions::TransactionId;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct Analyzer {
    dl: DefinitionLookup,
}

impl Analyzer {
    pub fn new(vis_row_man: VisibleRowManager) -> Analyzer {
        Analyzer {
            dl: DefinitionLookup::new(vis_row_man),
        }
    }

    pub async fn analyze(
        &self,
        tran_id: TransactionId,
        parse_tree: ParseTree,
    ) -> Result<QueryTree, AnalyzerError> {
        match parse_tree {
            ParseTree::Insert(i) => {
                let definition = self.dl.get_definition(tran_id, i.table_name).await?;

                return Err(AnalyzerError::NotImplemented());
            }
            _ => return Err(AnalyzerError::NotImplemented()),
        }
    }
}

#[derive(Debug, Error)]
pub enum AnalyzerError {
    #[error(transparent)]
    DefinitionLookupError(#[from] DefinitionLookupError),
    #[error("Not implemented")]
    NotImplemented(),
}
