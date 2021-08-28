use feophantlib::feophant::FeOphant;
use log::LevelFilter;
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    oneshot::Sender,
};
use tokio_postgres::{Client, NoTls};

pub async fn _create_server_and_client(
) -> Result<(UnboundedSender<Sender<()>>, Client), Box<dyn std::error::Error>> {
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Debug,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])?;

    let tmp = TempDir::new()?;

    let (request_shutdown, receive_shutdown): (
        UnboundedSender<Sender<()>>,
        UnboundedReceiver<Sender<()>>,
    ) = mpsc::unbounded_channel();

    let feo = FeOphant::new(tmp.into_path().into_os_string(), 0).await?;
    let port = feo.port;

    tokio::spawn(async move {
        feo.start(receive_shutdown).await;
    });

    let connect_str = format!("host=127.0.0.1 user=postgres port={0}", port);
    let (client, connection) = tokio_postgres::connect(&connect_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok((request_shutdown, client))
}

pub async fn _request_shutdown(
    request_shutdown: UnboundedSender<Sender<()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (res_shutdown, rev_shutdown) = oneshot::channel();
    request_shutdown.send(res_shutdown)?;

    Ok(rev_shutdown.await?)
}
