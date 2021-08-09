use crate::{
    codec::{NetworkFrame, PgCodec},
    engine::{io::IOManager, transactions::TransactionManager, Engine},
    processor::ClientProcessor,
};
use futures::{SinkExt, StreamExt};
use std::{
    env::ArgsOs,
    ffi::{OsStr, OsString},
};
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{self, UnboundedSender},
        oneshot::{self, error::RecvError, Sender},
    },
    task::JoinHandle,
};
use tokio_util::codec::Framed;

const DEFAULT_PORT: u32 = 50000;

pub struct FeOphant {
    request_shutdown: UnboundedSender<Sender<()>>,
    listener_task: JoinHandle<()>,
}

impl FeOphant {
    pub async fn new(data_dir: OsString) -> Result<FeOphant, FeOphantError> {
        let io_manager = IOManager::new();
        let transaction_manager = TransactionManager::new();
        let engine = Engine::new(io_manager, transaction_manager.clone());
        let (request_shutdown, mut receive_shutdown) = mpsc::unbounded_channel();

        let port = DEFAULT_PORT;

        let listener = TcpListener::bind(format!("{}{}", "127.0.0.1:", port)).await?;
        info!("Up and listening on port {}", port);

        let handle = tokio::spawn(async move {
            let listen = listener;

            let mut shutdown_sender: Option<Sender<()>> = None;

            loop {
                tokio::select! {
                    biased;
                    shut_sender = receive_shutdown.recv() => {
                        if let Some(sender) = shut_sender {
                            shutdown_sender = Some(sender);
                            info!("Got shutdown request");
                            break;
                        }
                    }
                    listen_res = listen.accept() => {
                        if let Ok((stream, client_addr)) = listen_res {
                            info!("Got a connection from {}", client_addr);
                            let tm = transaction_manager.clone();
                            let eng = engine.clone();
                            tokio::spawn(async move {
                                let codec = PgCodec {};
                                let (mut sink, mut input) = Framed::new(stream, codec).split();

                                let mut process = ClientProcessor::new(eng, tm);
                                while let Some(Ok(event)) = input.next().await {
                                    let responses: Vec<NetworkFrame> = match process.process(event).await {
                                        Ok(responses) => responses,
                                        Err(e) => {
                                            warn!("Had a processing error {}", e);
                                            break;
                                        }
                                    };

                                    for response in responses {
                                        match sink.send(response).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                warn!("Unable to send response {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                            });
                        }
                    }
                };
            }

            match shutdown_sender {
                Some(s) => {
                    s.send(())
                        .unwrap_or_else(|_| warn!("Unable to signal shutdown."));
                }
                None => {
                    error!("Exitting before shutting down all the sockets!");
                }
            }
        });

        Ok(FeOphant {
            request_shutdown,
            listener_task: handle,
        })
    }

    pub async fn shutdown(&self) -> Result<(), FeOphantError> {
        let (res_shutdown, rev_shutdown) = oneshot::channel();
        self.request_shutdown.clone().send(res_shutdown)?;

        Ok(rev_shutdown.await?)
    }
}

#[derive(Debug, Error)]
pub enum FeOphantError {
    #[error("FeOphant already started.")]
    AlreadyStarted(),
    #[error("Can't start the FeOphant twice")]
    CantStartTwice(),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    RecvError(#[from] RecvError),
    #[error(transparent)]
    ShutdownSendError(#[from] SendError<Sender<()>>),
}
