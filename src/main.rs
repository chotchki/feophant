use std::net::{SocketAddr, TcpListener, TcpStream};
use threadpool::Builder;
mod protocol;
use protocol::parser_error::ParserErrors;




fn handle_connection(stream: TcpStream) -> Result<(), ParserErrors> {
    println!("Recieved connection from {}", stream.peer_addr()?.ip());

    let mut parser = protocol::parser::Parser::new(Box::new(stream));
    let client_request = parser.read_request()?.unwrap();

    println!("Got type {}", client_request.message_type);
    Ok(())
}

fn main() -> std::io::Result<()> {
    let pool = Builder::new()
    .thread_name("Listener".into())
    .build();

    println!("Threadpool started with {} threads", pool.max_count());

    //Bind to a random port
    let addrs = [
        SocketAddr::from(([0, 0, 0, 0], 50000)),
    ];
    let listener = TcpListener::bind(&addrs[..])?;

    println!("Listening on {} port", listener.local_addr()?.port());

    for stream in listener.incoming(){
        pool.execute(|| {
            let cstream = stream.unwrap();
            match handle_connection(cstream) {
                Ok(_) => println!("Client session ended"),
                Err(_) => println!("Listener thread had an error")
            }
        });
    }

    println!("Hello, world!");
    Ok(())
}