//! The analyzer should check that tables and columns exist before allowing a query to proceed.
//! More features will come I'm sure
mod definition_lookup;
use definition_lookup::{DefinitionLookup, DefinitionLookupError};

use crate::constants::Nullable;
use crate::engine::objects::{JoinType, SqlTuple};

use super::io::VisibleRowManager;
use super::objects::types::{BaseSqlTypes, BaseSqlTypesError, SqlTypeDefinition};
use super::objects::{
    Attribute, CommandType, ParseExpression, ParseTree, QueryTree, RangeRelation,
    RangeRelationTable, RawInsertCommand, RawSelectCommand, Table,
};
use super::transactions::TransactionId;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone)]
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
            ParseTree::Insert(i) => self.insert_processing(tran_id, i).await,
            ParseTree::Select(i) => self.select_processing(tran_id, i).await,
            _ => Err(AnalyzerError::NotImplemented()),
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

        let (output_type, val_cols) = Analyzer::validate_columns(
            definition.clone(),
            raw_insert.provided_columns,
            raw_insert.provided_values,
        )?;

        let anon_tbl = RangeRelation::AnonymousTable(Arc::new(vec![val_cols]));
        let target_tbl = RangeRelation::Table(RangeRelationTable {
            alias: None,
            table: definition,
        });

        //We should be good to build the query tree if we got here
        Ok(QueryTree {
            command_type: CommandType::Insert,
            //Insert columns will be the target
            targets: Arc::new(output_type),
            range_tables: vec![target_tbl.clone(), anon_tbl.clone()],
            joins: vec![(JoinType::Inner, target_tbl, anon_tbl)],
        })
    }

    async fn select_processing(
        &self,
        tran_id: TransactionId,
        raw_select: RawSelectCommand,
    ) -> Result<QueryTree, AnalyzerError> {
        let definition = self.dl.get_definition(tran_id, raw_select.table).await?;

        //Need to valid the columns asked for exist
        let mut targets = vec![];
        'outer: for rcol in raw_select.columns {
            for c in definition.attributes.as_slice() {
                if rcol == c.name {
                    targets.push((c.name.clone(), c.sql_type.clone()));
                    continue 'outer;
                }
            }
            return Err(AnalyzerError::UnknownColumn(rcol));
        }

        //We should be good to build the query tree if we got here
        Ok(QueryTree {
            command_type: CommandType::Select,
            targets: Arc::new(SqlTypeDefinition(targets)),
            range_tables: vec![RangeRelation::Table(RangeRelationTable {
                table: definition,
                alias: None,
            })],
            joins: vec![],
        })
    }

    /// This function will sort the columns and values and convert them
    fn validate_columns(
        table: Arc<Table>,
        provided_columns: Option<Vec<String>>,
        provided_values: Vec<ParseExpression>,
    ) -> Result<(SqlTypeDefinition, SqlTuple), AnalyzerError> {
        let columns = match provided_columns {
            Some(pc) => {
                //Can't assume we got the columns in order so we'll have to reorder to match the table
                let mut provided_pair: HashMap<String, ParseExpression> =
                    pc.into_iter().zip(provided_values).collect();
                let mut result = vec![];
                for a in table.attributes.clone() {
                    match provided_pair.get(&a.name) {
                        Some(ppv) => {
                            result.push((a.clone(), Some(ppv.clone())));
                            provided_pair.remove(&a.name);
                        }
                        None => match a.nullable {
                            Nullable::NotNull => return Err(AnalyzerError::MissingColumn(a)),
                            Nullable::Null => result.push((a, None)),
                        },
                    }
                }

                if !provided_pair.is_empty() {
                    return Err(AnalyzerError::UnknownColumns(
                        provided_pair.keys().cloned().collect(),
                    ));
                }

                result
            }
            None => {
                //Assume we are in order of the table columns
                table
                    .attributes
                    .clone()
                    .into_iter()
                    .zip(provided_values)
                    .map(|(a, s)| (a, Some(s)))
                    .collect()
            }
        };

        Analyzer::convert_into_types(columns)
    }

    fn convert_into_types(
        provided: Vec<(Attribute, Option<ParseExpression>)>,
    ) -> Result<(SqlTypeDefinition, SqlTuple), AnalyzerError> {
        let mut tbl_cols = vec![];
        let mut val_cols = vec![];
        for (a, s) in provided {
            match s {
                Some(s2) => match s2 {
                    ParseExpression::String(s3) => {
                        tbl_cols.push((a.name, a.sql_type.clone()));
                        val_cols.push(Some(BaseSqlTypes::parse(a.sql_type, &s3)?));
                    }
                    ParseExpression::Null() => {
                        tbl_cols.push((a.name, a.sql_type));
                        val_cols.push(None);
                    }
                },
                None => {
                    tbl_cols.push((a.name, a.sql_type));
                    val_cols.push(None);
                }
            }
        }
        Ok((SqlTypeDefinition(tbl_cols), SqlTuple(val_cols)))
    }
}

#[derive(Debug, Error)]
pub enum AnalyzerError {
    #[error(transparent)]
    DefinitionLookupError(#[from] DefinitionLookupError),
    #[error(transparent)]
    BaseSqlTypesError(#[from] BaseSqlTypesError),
    #[error("Provided columns {0:?} does not match the underlying table columns {1:?}")]
    ColumnVsColumnMismatch(Vec<String>, Vec<String>),
    #[error("Provided value count {0} does not match the underlying table column count {1}")]
    ValueVsColumnMismatch(usize, usize),
    #[error("Missing required column {0}")]
    MissingColumn(Attribute),
    #[error("Unknown column received {0}")]
    UnknownColumn(String),
    #[error("Unknown columns received {0:?}")]
    UnknownColumns(Vec<String>),
    #[error("Not implemented")]
    NotImplemented(),
}
