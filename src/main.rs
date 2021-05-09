//Vendor Imports
#[macro_use] 
extern crate log;
extern crate simplelog;
use simplelog::{CombinedLogger, TermLogger, LevelFilter, Config, TerminalMode, ColorChoice};
use tokio::net::{TcpListener, TcpStream};


//Application Imports
mod protocol;
use protocol::ConnectionHandler;

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
        let (socket, client_addr) = listener.accept().await.unwrap();

        info!("Got a connection from {}", client_addr);
        
        //This is the inbound commands
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut handler = ConnectionHandler::new(socket, tx);
        tokio::spawn(async move {
            handler.process().await;
        });
    }
}

//async fn process(socket: TcpStream) {
    

    // while let Some(frame) = &connection.read_frame().await.unwrap() {
    //     //println!("GOT {:?}", frame);
    //     //println!("length of payload {}", frame.payload.len());

    //     //Channel to get the response to send back to the client
    //     let(tx, rx) = oneshot::channel::<Frame>();
        
    //     //Next we have a task parse the payload
    //     let async_conn = connection.clone();
    //     let handle = tokio::spawn(async move {
    //         process_frame(&async_conn, frame, tx);
    //     });


    //     //Next we take action on the payload


    //     //Finally this is where we wait for a response to come back from the pipelines
    //     match rx.await {
    //         Ok(resp) => {
    //             match connection.write_frame(&resp).await {
    //                 Ok(_) => (),
    //                 Err(e) => {
    //                     println!("Had an error writing response, closing connection {}", e);
    //                     return;
    //                 }
    //             }
    //         }
    //         Err(e) => {
    //             println!("Had an error getting a response, closing connection {}", e);
    //             return;
    //         }
    //     }
    // }
//}