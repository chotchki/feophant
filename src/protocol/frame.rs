use bytes::{Buf,Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::io::ErrorKind;

//Following this tutorial: https://tokio.rs/tokio/tutorial/framing
//Mirroring this code: https://github.com/tokio-rs/mini-redis/blob/master/src/frame.rs

#[derive(Clone, Debug)]
pub struct Frame {
    pub message_type: u8,
    pub length: u32,
    pub payload: Bytes
}

#[derive(Debug)]
pub enum Error {
    IncompleteLength,
    IncompleteType,
    IncompletePayload,
    Other(std::io::Error)
}

impl Frame {
    pub fn new(message_type: u8, length: u32, payload: Bytes) -> Frame {
        Frame{
            message_type: message_type,
            length: length,
            payload: payload
        }
    }

    //Protocol documented here: https://www.postgresql.org/docs/13/protocol-message-formats.html
    pub fn check(src: &mut Cursor<&[u8]>, in_startup: bool) -> Result<(), Error>{
        if !in_startup {
            let _msg_type = get_type(src)?;

            //TODO Check for other valid msg types here
        }

        let length = get_length(src)?;
        let remaining_len: usize = (length - 4).try_into().unwrap();

        if src.remaining() < remaining_len.try_into().unwrap() {
            return Err(Error::IncompletePayload);
        }

        Ok(())
    }

    //Must be called post check to make sure we have a message
    pub fn parse(src: &mut Cursor<&[u8]>, in_startup: bool) -> Result<Frame, Error> {
        let mut msg_type: u8 = 0;
        if !in_startup {
            msg_type = get_type(src)?;

            //TODO Check for other valid msg types here
        }

        let length: u32 = get_length(src)?;
        let remaining_len: usize = (length - 4).try_into().unwrap();

        let data = Bytes::copy_from_slice(&src.chunk()[..remaining_len]);
        src.advance(remaining_len);

        Ok(Frame{
            message_type: msg_type,
            length: length,
            payload: data
        })
    }
}

pub fn get_type(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::IncompleteType);
    }

    Ok(src.get_u8())
}

pub fn get_length(src: &mut Cursor<&[u8]>) -> Result<u32, Error> {
    if src.remaining() < 4 {
        return Err(Error::IncompleteLength);
    }

    Ok(src.get_u32())
}

impl From<Error> for std::io::Error {
    fn from(src: Error) -> std::io::Error {
        std::io::Error::new(ErrorKind::Other, src)
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::IncompleteLength => "stream was missing the u32 length".fmt(fmt),
            Error::IncompleteType => "stream was missing the type byte".fmt(fmt),
            Error::IncompletePayload => "stream was missing the complete payload".fmt(fmt),
            _other => "had an I/O error".fmt(fmt)
        }
    }
}