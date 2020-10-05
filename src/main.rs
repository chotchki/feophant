use tokio::net::{TcpListener, TcpStream};
mod protocol;

#[tokio::main]
async fn main() {
    //Bind to a fixed port
    let listener = TcpListener::bind("127.0.0.1:50000").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(socket: TcpStream) {

}