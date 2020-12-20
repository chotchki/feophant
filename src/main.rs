use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
mod protocol;
use protocol::connection::Connection;
use protocol::frame::Frame;

#[tokio::main]
async fn main() {
    //Bind to a fixed port
    let mut listener = TcpListener::bind("127.0.0.1:50000").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(socket: TcpStream) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT {:?}", frame);

        //Channel to get the response to send back to the client
        let(tx, rx) = oneshot::channel::<Frame>();
        
        //Next we have a task parse the payload
        //Next we take action on the payload


        //Finally this is where we wait for a response to come back from the pipelines
        match rx.await {
            Ok(resp) => {
                match connection.write_frame(&resp).await {
                    Ok(_) => (),
                    Err(e) => {
                        println!("Had an error writing response, closing connection {}", e);
                        return;
                    }
                }
            }
            Err(e) => {
                println!("Had an error getting a response, closing connection {}", e);
                return;
            }
        }
    }
}