use tokio_postgres::SimpleQueryMessage;

mod common;

#[tokio::test]
async fn simple_select() -> Result<(), Box<dyn std::error::Error>> {
    let (request_shutdown, client) = common::_create_server_and_client().await?;
    client
        .batch_execute("create table foo (bar text, baz text not null, another text null)")
        .await?;

    client
        .batch_execute("insert into foo (another, baz, bar) values(null, 'two', 'three')")
        .await?;

    let rows = client
        .simple_query("select baz, bar, another from foo;")
        .await?;

    // And then check that we got back the same string we sent over.
    assert_eq!(rows.len(), 2);
    match &rows[0] {
        SimpleQueryMessage::Row(s) => {
            assert_eq!(s.get(0).unwrap(), "two");
            assert_eq!(s.get(1).unwrap(), "three");
            assert_eq!(s.get(2), None);
        }
        _ => assert!(false),
    }

    common::_request_shutdown(request_shutdown).await
}
