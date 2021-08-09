#![forbid(unsafe_code)]

#[macro_use]
extern crate log;

extern crate simplelog;
use feophantlib::feophant::FeOphant;
use simplelog::{ColorChoice, CombinedLogger, Config, LevelFilter, TermLogger, TerminalMode};
use std::env;
use thiserror::Error;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Debug,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])
    .unwrap();

    info!("Welcome to FeOphant!");

    let data_dir = env::args_os().nth(1).ok_or_else(MainError::NoDataDir)?;
    let feo = FeOphant::new(data_dir).await?;

    signal::ctrl_c().await?;

    feo.shutdown().await?;

    Ok(())
}
#[derive(Debug, Error)]
pub enum MainError {
    #[error("You MUST provide a writeable directory so FeOphant can store its data.")]
    NoDataDir(),
}
