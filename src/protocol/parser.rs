use std::convert::TryFrom;
use std::io::BufReader;
use std::io::Read;
use super::parser_error::ParserErrors;
use super::parser_error::ClientDataError;
use super::client_request::ClientRequest;

pub struct Parser {
    buff_reader: BufReader<Box<dyn Read>>,
    is_first: bool
}

impl Parser {
    pub fn new(stream: Box<dyn Read>) -> Parser {
        Parser {
            buff_reader: BufReader::new(stream),
            is_first: true
        }
    }

    pub fn read_request(&mut self) -> Result<Option<ClientRequest>, ParserErrors> {
        let req_type: u8;

        //The type is not transmitted for the first request
        if self.is_first { 
            req_type = 0;
            self.is_first = false;
        } else {
            let mut buffer = [0; 1];
            self.buff_reader.read(&mut buffer)?;

            req_type = buffer[0];   
        }

        //Get the size of the payload
        let mut buffer = [0; 4];
        let bytes_read = self.buff_reader.read(&mut buffer)?;

        if bytes_read != 4 {
            return Err(ParserErrors::ClientData(ClientDataError::new(format!("Read just {} bytes", bytes_read))));
        }

        //Convert the payload, there is a chance the payload could be too big to fit in memory
        //HACK Figure out how to handle malicous clients from crashing the server
        //It looks like fallible collection support is needed from here: https://github.com/rust-lang/rust/issues/48043
        let payload_size: u32 = u32::from_be_bytes(buffer);
        let payload_usize = usize::try_from(payload_size)?;

        let mut payload = Vec::with_capacity(payload_usize);

        let payload_bytes_read = self.buff_reader.read(&mut payload)?;

        if payload_bytes_read != payload.len() {
            return Err(ParserErrors::ClientData(ClientDataError::new(format!("Read just {} bytes of {} asked for", payload_bytes_read, payload_usize))));
        }

        let client_request = ClientRequest {
            message_type: req_type,
            payload: payload
        };

        Ok(Some(client_request))
    }
}

