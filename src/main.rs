use std::net::{SocketAddr, TcpListener, TcpStream};
use threadpool::Builder;
mod protocol;




fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    println!("Recieved connection from {}", stream.peer_addr()?.ip());

    let parser = protocol::parser::Parser::new(stream);






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
                Err(e) => println!("Listener thread had an error {}", e)
            }
        });
    }

    println!("Hello, world!");
    Ok(())
}