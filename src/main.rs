#[macro_use]
extern crate log;

extern crate simplelog;
use feophantlib::codec::{NetworkFrame, PgCodec};
use feophantlib::engine::io::{IOManager, RowManager};
use feophantlib::engine::transactions::TransactionManager;
use feophantlib::processor::ClientProcessor;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use simplelog::{ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Debug,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])
    .unwrap();

    info!("Welcome to FeOphant!");

    //Start the services first
    let io_manager = Arc::new(RwLock::new(IOManager::new()));
    let transaction_manager = TransactionManager::new();
    let row_manager = RowManager::new(io_manager, transaction_manager.clone());

    //Bind to a fixed port
    let port: u32 = 50000;
    let listener = TcpListener::bind(format!("{}{}", "127.0.0.1:", port))
        .await
        .unwrap();

    info!("Up and listening on port {}", port);

    loop {
        let (stream, client_addr) = listener.accept().await.unwrap();

        info!("Got a connection from {}", client_addr);

        let rm = row_manager.clone();
        let tm = transaction_manager.clone();
        tokio::spawn(async move {
            let codec = PgCodec {};
            let (mut sink, mut input) = Framed::new(stream, codec).split();

            let mut process = ClientProcessor::new(rm, tm);
            while let Some(Ok(event)) = input.next().await {
                let responses: Vec<NetworkFrame> = match process.process(event).await {
                    Ok(responses) => responses,
                    Err(e) => {
                        warn!("Had a processing error {}", e);
                        break;
                    }
                };

                for response in responses {
                    match sink.send(response).await {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Unable to send response {}", e);
                            break;
                        }
                    }
                }
            }
        });
    }
}
