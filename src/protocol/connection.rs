use bytes::{Buf,BytesMut};
use std::io::{Cursor, Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::sync::oneshot::error::TryRecvError;
use super::frame::Frame;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    startup_receiver: oneshot::Receiver<bool>,
    startup_sender: oneshot::Sender<bool>,
    in_startup: bool
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        let(tx, rx) = oneshot::channel();
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4096),
            startup_receiver: rx,
            startup_sender: tx,
            in_startup: true
        }
    }

    pub fn startup_done(self){
        self.startup_sender.send(true);
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>, Error> {
        loop {
            //Check if the startup channel fired
            if self.in_startup == false {
                self.in_startup = self.startup_receiver.try_recv().is_ok();
            }

            //Try to parse a frame
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
