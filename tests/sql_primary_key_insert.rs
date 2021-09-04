use tokio_postgres::SimpleQueryMessage;

mod common;

#[tokio::test]
async fn primary_key_insert() -> Result<(), Box<dyn std::error::Error>> {
    let (request_shutdown, client) = common::_create_server_and_client().await?;
    client
        .batch_execute(
            "create table foo (
            bar text not null primary key, 
            baz text not null, 
            another text null)",
        )
        .await?;

    client
        .batch_execute("insert into foo (bar, baz, another) values('one', 'two', 'three')")
        .await?;

    let rows = client
        .simple_query("select bar, baz, another from foo;")
        .await?;

    let row_count = rows.into_iter().fold(0, |acc, x| -> usize {
        if let SimpleQueryMessage::Row(_) = x {
            acc + 1
        } else {
            acc
        }
    });

    assert_eq!(row_count, 1);

    let result = client
        .batch_execute("insert into foo (bar, baz, another) values('one', 'two', 'three')")
        .await;
    assert!(result.is_err());

    common::_request_shutdown(request_shutdown).await
}
