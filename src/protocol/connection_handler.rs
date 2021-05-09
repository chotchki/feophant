// It's this struct's job to take messages and stream them over the network
// It will also do basic frame parsing and pass the inbound traffic back up the stack

//Vendor
use bytes::BytesMut;
use tokio;
use tokio::io::{AsyncReadExt,AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use thiserror::Error;

//Application
use super::NetworkFrame;

pub struct ConnectionHandler {
    stream: TcpStream,
    up_buffer: BytesMut,
    up_send: Sender<NetworkFrame>,
    down_recv: Receiver<NetworkFrame>,
    pub down_send: Sender<NetworkFrame>
}

impl ConnectionHandler {
    pub fn new(stream: TcpStream, up_send: tokio::sync::mpsc::Sender<NetworkFrame>) -> ConnectionHandler {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        ConnectionHandler {
            stream: stream,
            up_buffer: BytesMut::with_capacity(4096),
            up_send: up_send,
            down_recv: rx,
            down_send: tx
        }
    }

    pub async fn process(&mut self){
        loop {
            let mut channel_should_close = false;

            tokio::select!{
                bytes_read = self.stream.read(&mut self.up_buffer) => {
                    match bytes_read {
                        Ok(0) => {
                            channel_should_close = true;
                        },
                        Ok(n) => {
                            match self.parse_frame() {
                                Ok(frame) => {
                                    self.up_send.send(frame);
                                },
                                Err(_) => {}
                            }
                        },
                        Err(e) => {
                            warn!("Had an I/O error {}", e);
                            channel_should_close = true;
                        }
                    }
                }
                frame_to_send = self.down_recv.recv() => {
                    match frame_to_send {
                        Some(f) => {
                            //I think this await is still wrong but I think I'll have to come back
                            //for a proper pin implementation on this
                            self.stream.write_all(&f.to_bytes()[..]).await;
                        },
                        None => {}
                    }
                    
                }
            };

            if channel_should_close {
                return;
            }
        }
    }

    pub fn parse_frame(&self) -> Result<NetworkFrame,HandlerError> {
        Err(HandlerError::Empty)
    }
}

#[derive(Error, Debug)]
pub enum HandlerError {
    #[error("No data")]
    Empty
}