use crate::{
    codec::{NetworkFrame, PgCodec},
    engine::{
        io::{FileManager, FileManagerError},
        transactions::TransactionManager,
        Engine,
    },
    processor::ClientProcessor,
};
use futures::{SinkExt, StreamExt};
use std::{ffi::OsString, sync::Arc};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::oneshot::{error::RecvError, Sender},
};
use tokio::{
    pin,
    sync::mpsc::{error::SendError, UnboundedReceiver},
};
use tokio_util::codec::Framed;

pub struct FeOphant {
    pub port: u16,
    listener: TcpListener,
    transaction_manager: TransactionManager,
    engine: Engine,
    file_manager: Arc<FileManager>,
}

impl FeOphant {
    pub async fn new(data_dir: OsString, port: u16) -> Result<FeOphant, FeOphantError> {
        let file_manager = Arc::new(FileManager::new(data_dir)?);
        let transaction_manager = TransactionManager::new();
        let engine = Engine::new(file_manager.clone(), transaction_manager.clone());

        let listener = TcpListener::bind(format!("{}{}", "127.0.0.1:", port)).await?;
        let port = listener.local_addr()?.port();
        debug!("Bound to port {0}, but not processing yet.", port);

        Ok(FeOphant {
            port,
            listener,
            transaction_manager,
            engine,
            file_manager,
        })
    }

    /// Starts up the actual server, should be started as its own task
    /// Send on the shutdown_recv to shut it down.
    pub async fn start(&self, shutdown_recv: UnboundedReceiver<Sender<()>>) {
        let mut shutdown_sender: Option<Sender<()>> = None;
        info!("Up and listening on port {}", self.port);

        let listen = &self.listener;
        pin!(shutdown_recv);
        pin!(listen);

        loop {
            tokio::select! {
                biased;
                shut_sender = shutdown_recv.recv() => {
                    if let Some(sender) = shut_sender {
                        shutdown_sender = Some(sender);
                        info!("Got shutdown request");
                        break;
                    }
                }
                listen_res = listen.accept() => {
                    if let Ok((stream, client_addr)) = listen_res {
                        info!("Got a connection from {}", client_addr);
                        let tm = self.transaction_manager.clone();
                        let eng = self.engine.clone();
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
                    } else if let Err(e) = listen_res {
                        error!("Got error receiving a connection. {0}", e);
                    }
                }
            };
        }

        //Clean up
        match self.file_manager.shutdown().await {
            Ok(_) => info!("File Manager shutdown!"),
            Err(e) => error!("Had an error shutting down I/O {0}", e),
        }

        match shutdown_sender {
            Some(s) => {
                debug!("Attempting to signal shutdown.");
                s.send(())
                    .unwrap_or_else(|_| warn!("Unable to signal shutdown."));
            }
            None => {
                error!("Exitting before shutting down all the sockets!");
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum FeOphantError {
    #[error("FeOphant already started.")]
    AlreadyStarted(),
    #[error("Can't start the FeOphant twice")]
    CantStartTwice(),
    #[error(transparent)]
    FileManagerError(#[from] FileManagerError),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    RecvError(#[from] RecvError),
    #[error(transparent)]
    ShutdownSendError(#[from] SendError<Sender<()>>),
}
