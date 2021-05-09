//! The protocol module covers the network level traffic. It will frame a message into a byte array for parsing.

pub mod connection;

//mod connection_handler;
//pub use connection_handler::ConnectionHandler;

pub mod frame;
pub mod process_frame;