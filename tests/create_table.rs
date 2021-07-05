use feophantlib::{
    constants::{BuiltinSqlTypes, DeserializeTypes},
    engine::{
        io::{row_formats::RowData, IOManager, RowManager, VisibleRowManager},
        objects::{Attribute, Table},
        transactions::TransactionManager,
        Engine, EngineError,
    },
};
use std::sync::Arc;
use tokio::sync::RwLock;

macro_rules! aw {
    ($e:expr) => {
        tokio_test::block_on($e)
    };
}

#[test]
fn create_table_with_nullable() -> Result<(), Box<dyn std::error::Error>> {
    let create_test =
        "create table foo (bar text, baz text not null, another text null)".to_string();

    let mut transaction_manager = TransactionManager::new();
    let mut engine = Engine::new(
        Arc::new(RwLock::new(IOManager::new())),
        transaction_manager.clone(),
    );

    let tran = aw!(transaction_manager.start_trans())?;
    match aw!(engine.process_query(tran, create_test)) {
        Ok(_) => {}
        Err(e) => {
            println!("{0}", e);
            panic!();
        }
    };
    aw!(transaction_manager.commit_trans(tran))?;

    Ok(())
}
