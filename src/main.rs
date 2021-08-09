#![forbid(unsafe_code)]

#[macro_use]
extern crate log;

extern crate simplelog;
use feophantlib::feophant::FeOphant;
use simplelog::{ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode};
use std::env;
use thiserror::Error;
use tokio::{
    signal,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot::{self, Sender},
    },
};

const DEFAULT_PORT: u16 = 50000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Debug,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])?;

    info!("Welcome to FeOphant!");

    let data_dir = env::args_os().nth(1).ok_or_else(MainError::NoDataDir)?;
    let (request_shutdown, receive_shutdown): (
        UnboundedSender<Sender<()>>,
        UnboundedReceiver<Sender<()>>,
    ) = mpsc::unbounded_channel();

    let feo = FeOphant::new(data_dir, DEFAULT_PORT).await?;

    tokio::spawn(async move {
        feo.start(receive_shutdown).await;
    });

    signal::ctrl_c().await?;

    let (res_shutdown, rev_shutdown) = oneshot::channel();
    request_shutdown.send(res_shutdown)?;

    Ok(rev_shutdown.await?)
}
#[derive(Debug, Error)]
pub enum MainError {
    #[error("You MUST provide a writeable directory so FeOphant can store its data.")]
    NoDataDir(),
}
