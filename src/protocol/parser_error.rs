use std::io;
use std::num;

//See here for error types: https://doc.rust-lang.org/stable/rust-by-example/error/multiple_error_types/define_error_type.html

#[derive(Debug)]
pub enum ParserErrors {
    IoError(io::Error),
    ClientData(ClientDataError),
    USizeTooSmall(num::TryFromIntError)
}

impl From<io::Error> for ParserErrors {
    fn from(error: io::Error) -> Self {
        ParserErrors::IoError(error)
    }
}

impl From<ClientDataError> for ParserErrors {
    fn from(error: ClientDataError) -> Self {
        ParserErrors::ClientData(error)
    }
}
impl From<std::num::TryFromIntError> for ParserErrors {
    fn from(error: num::TryFromIntError) -> Self {
        ParserErrors::USizeTooSmall(error)
    }
}

#[derive(Debug)]
pub struct ClientDataError {
    message: String
}

impl ClientDataError {
    pub fn new(mesg: String) -> ClientDataError{
        ClientDataError {
            message: mesg
        }
    }
}