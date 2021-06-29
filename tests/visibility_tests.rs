use feophantlib::{
    constants::{BuiltinSqlTypes, DeserializeTypes},
    engine::{
        io::{row_formats::RowData, IOManager, RowManager, VisibleRowManager},
        objects::{Attribute, Table},
        transactions::TransactionManager,
    },
};
use futures::stream::StreamExt;
use log::{debug, info};
use simplelog::{ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode};
use std::sync::Arc;
use tokio::sync::RwLock;

macro_rules! aw {
    ($e:expr) => {
        tokio_test::block_on($e)
    };
}

fn get_row(input: String) -> Vec<Option<BuiltinSqlTypes>> {
    vec![
                Some(BuiltinSqlTypes::Text(input)),
                None,
                Some(BuiltinSqlTypes::Text("blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah".to_string())),
            ]
}
fn get_table() -> Arc<Table> {
    Arc::new(Table::new(
        "test_table".to_string(),
        vec![
            Attribute::new(
                uuid::Uuid::new_v4(),
                "header".to_string(),
                DeserializeTypes::Text,
            ),
            Attribute::new(
                uuid::Uuid::new_v4(),
                "id".to_string(),
                DeserializeTypes::Uuid,
            ),
            Attribute::new(
                uuid::Uuid::new_v4(),
                "header3".to_string(),
                DeserializeTypes::Text,
            ),
        ],
    ))
}

#[test]
fn test_row_manager_visibility() {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Warn,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])
    .unwrap();

    let table = get_table();
    let mut tm = TransactionManager::new();
    let pm = Arc::new(RwLock::new(IOManager::new()));
    let rm = RowManager::new(pm);
    let vm = VisibleRowManager::new(rm.clone(), tm.clone());
    let row = get_row("test".to_string());

    info!("Insert a row that should be seen.");
    let tran_id = aw!(tm.start_trans()).unwrap();
    let row_pointer = aw!(rm.clone().insert_row(tran_id, table.clone(), row.clone())).unwrap();
    let res: Vec<RowData> = aw!(vm
        .clone()
        .get_stream(tran_id, table.clone())
        .map(Result::unwrap)
        .collect());
    assert_eq!(res[0].user_data, row);

    info!("It should not be seen in the future.");
    let tran_id_2 = aw!(tm.start_trans()).unwrap();
    let res: Vec<RowData> = aw!(vm
        .clone()
        .get_stream(tran_id_2, table.clone())
        .map(Result::unwrap)
        .collect());
    assert!(res.is_empty());

    aw!(tm.commit_trans(tran_id)).unwrap();
    aw!(tm.commit_trans(tran_id_2)).unwrap();

    info!("It should be seen when deleted but still in the past");
    let tran_id_3 = aw!(tm.start_trans()).unwrap();
    debug!("On transaction {:?}, viewing as {:?}", tran_id_3, tran_id);
    aw!(rm.clone().delete_row(tran_id_3, table.clone(), row_pointer)).unwrap();
    aw!(tm.commit_trans(tran_id_3)).unwrap();
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
}
