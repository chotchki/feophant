//Vendor Imports
#[macro_use] 
extern crate log;
extern crate simplelog;
use futures::stream::StreamExt;
use simplelog::{CombinedLogger, TermLogger, LevelFilter, Config, TerminalMode, ColorChoice};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use hex_literal::hex;

const SSL_PAYLOAD: [u8; 4] = hex!("12 34 56 78");

//Application Imports
mod codec;
use codec::PgCodec;

#[tokio::main]
async fn main() {
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Debug, Config::default(), TerminalMode::Mixed, ColorChoice::Auto)
        ]
    ).unwrap();

    info!("Welcome to the Rusty Elephant!");

    //Bind to a fixed port
    let port:u32 = 50000;
    let listener = TcpListener::bind(format!("{}{}", "127.0.0.1:", port)).await.unwrap();

    info!("Up and listening on port {}", port);

    loop {
        let (stream, client_addr) = listener.accept().await.unwrap();

        info!("Got a connection from {}", client_addr);
        
        tokio::spawn(async move {
            let codec = PgCodec{};
            let (mut sink, mut input) = Framed::new(stream, codec).split();

            while let Some(Ok(event)) = input.next().await {
                println!("Event {:?}", event);
              }
        });
    }
}