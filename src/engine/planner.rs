//! The planner takes a parsed query and makes it into a set of commands that can be sequentially executed.
use super::objects::{
    CommandType, JoinType, ModifyTablePlan, Plan, PlannedCommon, PlannedStatement, QueryTree,
    RangeRelation,
};
use crate::engine::objects::FullTableScan;
use std::sync::Arc;
use thiserror::Error;

pub struct Planner {}

impl Planner {
    pub fn plan(query_tree: QueryTree) -> Result<PlannedStatement, PlannerError> {
        match query_tree.command_type {
            CommandType::Insert => Planner::plan_insert(query_tree),
            CommandType::Select => Planner::plan_select(query_tree),
            _ => Err(PlannerError::NotImplemented()),
        }
    }

    fn plan_insert(query_tree: QueryTree) -> Result<PlannedStatement, PlannerError> {
        //So we know we want to insert, now the question is into what.
        //I'm going to start with a simple insert and let it evolve.

        //So we should be able to find a join for our table target
        let join = query_tree
            .joins
            .first()
            .ok_or_else(|| PlannerError::TooManyJoins(query_tree.joins.len()))?;

        match join {
            (JoinType::Inner, RangeRelation::Table(t), RangeRelation::AnonymousTable(at)) => {
                Ok(PlannedStatement {
                    common: PlannedCommon {},
                    plan: Arc::new(Plan::ModifyTable(ModifyTablePlan {
                        table: t.table.clone(),
                        source: Arc::new(Plan::StaticData(at.clone())),
                    })),
                })
            }
            (_, _, _) => Err(PlannerError::NotImplemented()),
        }
    }

    fn plan_select(query_tree: QueryTree) -> Result<PlannedStatement, PlannerError> {
        //TODO I'm ignoring joins at the moment

        //let mut targets = vec![];
        //for t in query_tree.targets {
        //    match t {
        //        TargetEntry::Parameter(p) => targets.push(p),
        //    }
        //}

        let mut unjoined = vec![];
        for rr in query_tree.range_tables {
            match rr {
                RangeRelation::Table(rrt) => {
                    unjoined.push(Arc::new(Plan::FullTableScan(FullTableScan {
                        src_table: rrt.table,
                        target_type: query_tree.targets.clone(), //TODO I know not every table needs every column
                    })));
                }
                RangeRelation::AnonymousTable(anon_tbl) => {
                    unjoined.push(Arc::new(Plan::StaticData(anon_tbl.clone())));
                }
            }
        }

        if unjoined.is_empty() {
            Err(PlannerError::NoDataProvided())
        } else if unjoined.len() == 1 {
            Ok(PlannedStatement {
                common: PlannedCommon {},
                plan: unjoined[0].clone(),
            })
        } else {
            //let cart_joins = return Ok(PlannedStatement {
            //    common: PlannedCommon {},
            //    plan: Arc::new(Plan::CrossProduct(CrossProduct {
            //        columns: targets,
            //        source: unjoined,
            //    })),
            //});
            Err(PlannerError::NotImplemented())
        }
    }
}

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("No data provided")]
    NoDataProvided(),
    #[error("Too Many Joins {0}")]
    TooManyJoins(usize),
    #[error("Not Implemented")]
    NotImplemented(),
}
