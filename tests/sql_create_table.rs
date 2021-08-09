use core::time;
use std::thread;

use feophantlib::feophant::FeOphant;
use simplelog::{ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode};
use tempfile::TempDir;
use tokio::{
    runtime::Runtime,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot::{self, Sender},
    },
};
use tokio_postgres::NoTls;

mod common;

#[tokio::test]
async fn create_table_with_nullable() -> Result<(), Box<dyn std::error::Error>> {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Debug,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])?;

    let tmp = TempDir::new()?;

    let (request_shutdown, receive_shutdown): (
        UnboundedSender<Sender<()>>,
        UnboundedReceiver<Sender<()>>,
    ) = mpsc::unbounded_channel();

    let feo = FeOphant::new(tmp.into_path().into_os_string(), 50000).await?;
    let port = feo.port;

    tokio::spawn(async move {
        feo.start(receive_shutdown).await;
    });

    let connect_str = format!("host=127.0.0.1 user=postgres port={0}", port);
    let (client, connection) = tokio_postgres::connect(&connect_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    client
        .batch_execute(
            "create table foo (
            bar text, 
            baz text not null, 
            another text null
        )",
        )
        .await?;

    let (res_shutdown, rev_shutdown) = oneshot::channel();
    request_shutdown.send(res_shutdown)?;

    rev_shutdown.await?;

    Ok(())
}
