use std::convert::TryFrom;
use std::io::BufReader;
use std::io::Read;
use std::net::TcpStream;
use super::parser_error::ParserError;
use super::client_request::ClientRequest;

pub struct Parser {
    buff_reader: BufReader<TcpStream>,
    is_first: bool
}

impl Parser {
    pub fn new(stream: TcpStream) -> Parser {
        Parser {
            buff_reader: BufReader::new(stream),
            is_first: false
        }
    }

    pub fn read_request(&self) -> Result<Option<ClientRequest>, ParserError> {
        let req_type: u8 = 0;

        //The type is not transmitted for the first request
        if self.is_first { 
            req_type = 0;
        } else {
            let mut buffer = [0; 1];
            self.buff_reader.read(&mut buffer)?;

            req_type = buffer[0];

            self.is_first = false;
        }

        //Get the size of the payload
        let mut buffer = [0; 4];
        let bytes_read = self.buff_reader.read(&mut buffer)?;

        if bytes_read != 4 {
            return Err(format!("Read just {} bytes", bytes_read));
        }

        //Convert the payload, there is a chance the payload could be too big to fit in memory
        //HACK Figure out how to handle malicous clients from crashing the server
        //It looks like fallible collection support is needed from here: https://github.com/rust-lang/rust/issues/48043
        let payload_size: u32 = u32::from_le_bytes(buffer);
        let payload_usize = usize::try_from(payload_size)?;

        let payload = Vec::with_capacity(payload_size);


        let client_request = ClientRequest {
            message_type: req_type,
            length: 
        };

        Ok(None())
    }
}

