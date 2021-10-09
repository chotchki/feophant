use feophantlib::engine::{
    get_row, get_table,
    io::{
        block_layer::{file_manager2::FileManager2, free_space_manager::FreeSpaceManager},
        row_formats::RowData,
        RowManager, VisibleRowManager,
    },
    transactions::TransactionManager,
};
use futures::stream::StreamExt;
use log::{debug, info};
use simplelog::{ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode};
use std::sync::Arc;
use tempfile::TempDir;
mod common;

#[tokio::test]
async fn test_row_manager_visibility() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let tmp_dir = tmp.path().as_os_str().to_os_string();

    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Warn,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])?;

    let table = get_table();
    let fm = Arc::new(FileManager2::new(tmp_dir.clone())?);
    let fsm = FreeSpaceManager::new(fm.clone());
    let rm = RowManager::new(fm.clone(), fsm);
    let mut tm = TransactionManager::new();
    let vm = VisibleRowManager::new(rm.clone(), tm.clone());
    let row = get_row("test".to_string());

    info!("Insert a row that should be seen.");
    let tran_id = tm.start_trans().await?;
    let row_pointer = rm.clone().insert_row(tran_id, &table, row.clone()).await?;
    let res: Vec<RowData> = vm
        .clone()
        .get_stream(tran_id, &table)
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(res[0].user_data, row);

    info!("It should not be seen in the future.");
    let tran_id_2 = tm.start_trans().await?;
    let res: Vec<RowData> = vm
        .clone()
        .get_stream(tran_id_2, &table)
        .map(Result::unwrap)
        .collect()
        .await;
    assert!(res.is_empty());

    tm.commit_trans(tran_id).await?;
    tm.commit_trans(tran_id_2).await?;

    info!("It should be seen when deleted but still in the past");
    let tran_id_3 = tm.start_trans().await?;
    debug!("On transaction {:?}, viewing as {:?}", tran_id_3, tran_id);
    rm.clone()
        .delete_row(tran_id_3, &table, row_pointer)
        .await?;
    tm.commit_trans(tran_id_3).await?;
    let res: Vec<RowData> = vm
        .clone()
        .get_stream(tran_id, &table)
        .map(Result::unwrap)
        .collect()
        .await;
    assert_eq!(res[0].user_data, row);

    info!("It should be gone in the present");
    let res: Vec<RowData> = vm
        .clone()
        .get_stream(tran_id_3, &table)
        .map(Result::unwrap)
        .collect()
        .await;
    assert!(res.is_empty());

    Ok(())
}
