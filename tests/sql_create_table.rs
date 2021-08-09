mod common;

#[tokio::test]
async fn create_table_with_nullable() -> Result<(), Box<dyn std::error::Error>> {
    let (request_shutdown, client) = common::_create_server_and_client().await?;
    client
        .batch_execute(
            "create table foo (
            bar text, 
            baz text not null, 
            another text null
        )",
        )
        .await?;

    common::_request_shutdown(request_shutdown).await
}
