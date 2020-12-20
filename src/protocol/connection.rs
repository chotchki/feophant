use bytes::{Buf,BytesMut};
use std::io::{Cursor, Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use super::frame::Frame;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    in_startup: bool
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4096),
            in_startup: true
        }
    }

    pub fn startup_done(&mut self){
        self.in_startup = false;
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>, Error> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Error::new(ErrorKind::ConnectionReset, "Connection reset by peer."));
                }
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), Error> {
        if frame.message_type == 0 {
            //This is a special type for SSL responses, no type or length will be written
            self.stream.write_all(&frame.payload).await?;
        } else {
            self.stream.write_u8(frame.message_type).await?;
            self.stream.write_u32(frame.length).await?;
            self.stream.write_all(&frame.payload).await?;
        }

        self.stream.flush().await?;

        Ok(())
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>, Error>{
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf, self.in_startup){
            Ok(_) => {
                let len = buf.position() as usize;

                buf.set_position(0);

                let frame = Frame::parse(&mut buf, self.in_startup)?;

                self.buffer.advance(len);

                Ok(Some(frame))
            }

            Err(super::frame::Error::IncompleteLength) => Ok(None),
            Err(super::frame::Error::IncompleteType) => Ok(None),
            Err(super::frame::Error::IncompletePayload) => Ok(None),
            Err(e) => Err(e.into())
        }
    }
}
