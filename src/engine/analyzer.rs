//! The analyzer should check that tables and columns exist before allowing a query to proceed.
//! More features will come I'm sure
mod definition_lookup;
use definition_lookup::{DefinitionLookup, DefinitionLookupError};

use super::io::VisibleRowManager;
use super::objects::{
    CommandType, ParseTree, QueryTree, RangeRelation, RangeRelationTable, RawCreateTableCommand,
    RawInsertCommand,
};
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
                return self.insert_processing(tran_id, i).await;
            }
            _ => return Err(AnalyzerError::NotImplemented()),
        }
    }

    async fn insert_processing(
        &self,
        tran_id: TransactionId,
        raw_insert: RawInsertCommand,
    ) -> Result<QueryTree, AnalyzerError> {
        let definition = self
            .dl
            .get_definition(tran_id, raw_insert.table_name)
            .await?;

        //Now let's make sure insert has the right columns
        match raw_insert.provided_columns {
            Some(pc) => {
                let mut pc_sorted: Vec<String> = pc.clone();
                pc_sorted.sort();
                let mut def_sorted: Vec<String> = definition
                    .attributes
                    .clone()
                    .into_iter()
                    .map(|a| a.name)
                    .collect();
                def_sorted.sort();

                if pc_sorted != def_sorted {
                    return Err(AnalyzerError::ColumnVsColumnMismatch(pc_sorted, def_sorted));
                }
                if raw_insert.provided_values.len() != definition.attributes.len() {
                    return Err(AnalyzerError::ValueVsColumnMismatch(
                        raw_insert.provided_values.len(),
                        definition.attributes.len(),
                    ));
                }
            }
            None => {
                if raw_insert.provided_values.len() != definition.attributes.len() {
                    return Err(AnalyzerError::ValueVsColumnMismatch(
                        raw_insert.provided_values.len(),
                        definition.attributes.len(),
                    ));
                }
            }
        }

        let rr = RangeRelation::Table(RangeRelationTable {
            table: definition,
            alias: None,
        });

        //We should be good to build the query tree if we got here
        Ok(QueryTree {
            command_type: CommandType::Insert,
            range_tables: vec![Arc::new(rr)],
        })
    }
}

#[derive(Debug, Error)]
pub enum AnalyzerError {
    #[error(transparent)]
    DefinitionLookupError(#[from] DefinitionLookupError),
    #[error("Provided columns {0:?} does not match the underlying table columns {1:?}")]
    ColumnVsColumnMismatch(Vec<String>, Vec<String>),
    #[error("Provided value count {0} does not match the underlying table column count {1}")]
    ValueVsColumnMismatch(usize, usize),
    #[error("Not implemented")]
    NotImplemented(),
}
