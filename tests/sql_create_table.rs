mod common;

#[test]
fn create_table_with_nullable() -> Result<(), Box<dyn std::error::Error>> {
    let create_test =
        "create table foo (bar text, baz text not null, another text null)".to_string();

    let (mut tm, mut engine) = common::create_engine();

    let tran = aw!(tm.start_trans())?;
    aw!(engine.process_query(tran, create_test))?;
    aw!(tm.commit_trans(tran))?;

    Ok(())
}
