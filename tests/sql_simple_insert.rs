mod common;

#[test]
fn simple_insert() -> Result<(), Box<dyn std::error::Error>> {
    let (mut tm, mut engine) = common::create_engine();

    let create_test =
        "create table foo (bar text, baz text not null, another text null)".to_string();

    let tran = aw!(tm.start_trans())?;
    aw!(engine.process_query(tran, create_test))?;
    aw!(tm.commit_trans(tran))?;

    let insert_test =
        "insert into foo (another, baz, bar) values('one', 'two', 'three')".to_string();
    let tran = aw!(tm.start_trans())?;
    aw!(engine.process_query(tran, insert_test))?;
    aw!(tm.commit_trans(tran))?;

    Ok(())
}
