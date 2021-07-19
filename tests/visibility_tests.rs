use feophantlib::{
    constants::{BuiltinSqlTypes, DeserializeTypes, Nullable},
    engine::{
        io::{row_formats::RowData, IOManager, RowManager, VisibleRowManager},
        objects::{Attribute, SqlTuple, Table},
        transactions::TransactionManager,
    },
};
use futures::stream::StreamExt;
use log::{debug, info};
use simplelog::{ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode};
use std::sync::Arc;
use tokio::sync::RwLock;
mod common;

fn get_row(input: String) -> Arc<SqlTuple> {
    Arc::new(SqlTuple(vec![
        Some(BuiltinSqlTypes::Text(input)),
        None,
        Some(BuiltinSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
    ]))
}
fn get_table() -> Arc<Table> {
    Arc::new(Table::new(
        "test_table".to_string(),
        vec![
            Attribute::new(
                uuid::Uuid::new_v4(),
                "header".to_string(),
                DeserializeTypes::Text,
                Nullable::NotNull,
            ),
            Attribute::new(
                uuid::Uuid::new_v4(),
                "id".to_string(),
                DeserializeTypes::Uuid,
                Nullable::Null,
            ),
            Attribute::new(
                uuid::Uuid::new_v4(),
                "header3".to_string(),
                DeserializeTypes::Text,
                Nullable::NotNull,
            ),
        ],
    ))
}

#[test]
fn test_row_manager_visibility() -> Result<(), Box<dyn std::error::Error>> {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Warn,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])?;

    let table = get_table();
    let mut tm = TransactionManager::new();
    let pm = IOManager::new();
    let rm = RowManager::new(pm);
    let vm = VisibleRowManager::new(rm.clone(), tm.clone());
    let row = get_row("test".to_string());

    info!("Insert a row that should be seen.");
    let tran_id = aw!(tm.start_trans())?;
    let row_pointer = aw!(rm.clone().insert_row(tran_id, table.clone(), row.clone()))?;
    let res: Vec<RowData> = aw!(vm
        .clone()
        .get_stream(tran_id, table.clone())
        .map(Result::unwrap)
        .collect());
    assert_eq!(res[0].user_data, row);

    info!("It should not be seen in the future.");
    let tran_id_2 = aw!(tm.start_trans())?;
    let res: Vec<RowData> = aw!(vm
        .clone()
        .get_stream(tran_id_2, table.clone())
        .map(Result::unwrap)
        .collect());
    assert!(res.is_empty());

    aw!(tm.commit_trans(tran_id))?;
    aw!(tm.commit_trans(tran_id_2))?;

    info!("It should be seen when deleted but still in the past");
    let tran_id_3 = aw!(tm.start_trans())?;
    debug!("On transaction {:?}, viewing as {:?}", tran_id_3, tran_id);
    aw!(rm.clone().delete_row(tran_id_3, table.clone(), row_pointer))?;
    aw!(tm.commit_trans(tran_id_3))?;
    let res: Vec<RowData> = aw!(vm
        .clone()
        .get_stream(tran_id, table.clone())
        .map(Result::unwrap)
        .collect());
    assert_eq!(res[0].user_data, row);

    info!("It should be gone in the present");
    let res: Vec<RowData> = aw!(vm
        .clone()
        .get_stream(tran_id_3, table.clone())
        .map(Result::unwrap)
        .collect());
    assert!(res.is_empty());

    Ok(())
}
