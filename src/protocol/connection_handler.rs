// It's this struct's job to take messages and stream them over the network
// It will also do basic frame parsing and pass the inbound traffic back up the stack

//Vendor
use bytes::BytesMut;
use futures::future::{Future,TryFutureExt};
use futures::io::Read;
use std::io;
use std::sync::Arc;
use tokio;
use tokio::io::Interest;
use tokio::io::{AsyncReadExt,AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
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

    pub async fn process(mut self) -> Result<(), io::Error>{
        loop {
            let ready = self.stream.ready(Interest::READABLE | Interest::WRITABLE).await?;

            if ready.is_readable() {
                match self.stream.try_read(&mut self.up_buffer) {
                    Ok(n) => {
                               
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        continue;
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
             }

            if ready.is_writable() {
                match self.down_recv.recv() => {
                     frame_to_send {
                        Some(f) => {
                            //I think this await is still wrong but I think I'll have to come back
                            //for a proper pin implementation on this
                            self.stream.write_all(&f.to_bytes()[..]).await;
                        },
                        None => {}
                    }
                    
                }


                // Try to write data, this may still fail with `WouldBlock`
                // if the readiness event is a false positive.
                match self.stream.try_write() {
                Ok(n) => {
                    println!("write {} bytes", n);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

            /*
            tokio::select!{
                bytes_read = self.stream.read(&mut self.up_buffer) => {
                    match bytes_read {
                        Ok(0) => {
                            info!("Client disconnected");
                            channel_should_close = true;
                        },
                        Ok(_) => {
                            self.parse_frame().and_then(move |frame| {up_send_clone.send(frame).map_err(|e| HandlerError::CannotAddToQueue)});
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
            };*/

           /*  tokio::select!{
                bytes_read = self.stream.read(&mut self.up_buffer) => {
                    match bytes_read {
                        Ok(0) => {
                            info!("Client disconnected");
                            channel_should_close = true;
                        },
                        Ok(_) => {
                            match self.parse_frame() {
                                Ok(frame) => {
                                    self.up_send.send(frame).await;
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
            }; */
        }
    }

    fn parse_frame(mut self) -> Result<NetworkFrame, io::Error> {
        Ok()
    }
}

#[derive(Error, Debug)]
pub enum HandlerError {
    #[error("No data")]
    Empty,
    #[error("Unable to add to queue")]
    CannotAddToQueue
}