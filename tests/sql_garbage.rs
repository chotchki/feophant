mod common;

///This test will input random garbage as a sql error and should error out
#[tokio::test]
async fn garbage() -> Result<(), Box<dyn std::error::Error>> {
    let (request_shutdown, client) = common::_create_server_and_client().await?;
    let res = client.batch_execute("Lorem ipsum dolor sit amet").await;
    assert!(res.is_err());

    common::_request_shutdown(request_shutdown).await
}
