use bytes::Bytes;
use thiserror::Error;

use super::super::engine::transactions::{TransactionManager, TransactionManagerError};
use super::super::engine::{Engine, EngineError};
use super::ssl_and_gssapi_parser;
use super::startup_parser;
use crate::codec::{NetworkFrame, NetworkFrameError};
use crate::constants::{PgErrorCodes, PgErrorLevels};

pub struct ClientProcessor {
    engine: Engine,
    transaction_manager: TransactionManager,
}

impl ClientProcessor {
    pub fn new(engine: Engine, transaction_manager: TransactionManager) -> ClientProcessor {
        ClientProcessor {
            engine,
            transaction_manager,
        }
    }

    pub async fn process(
        &mut self,
        frame: NetworkFrame,
    ) -> Result<Vec<NetworkFrame>, ClientProcessorError> {
        let payload_buff: &[u8] = &frame.payload;

        //Startup stuff
        if frame.message_type == 0 && ssl_and_gssapi_parser::is_ssl_request(payload_buff) {
            debug!("Got a SSL Request, no security here... yet");
            return Ok(vec![NetworkFrame::new(0, Bytes::from_static(b"N"))]);
        } else if frame.message_type == 0 && ssl_and_gssapi_parser::is_gssapi_request(payload_buff)
        {
            debug!("Got a GSSAPI Request, no security here... yet");
            return Ok(vec![NetworkFrame::new(0, Bytes::from_static(b"N"))]);
        } else if frame.message_type == 0 {
            debug!("Got a startup message!");
            let message = startup_parser::parse_startup(payload_buff)
                .map_err(|_| ClientProcessorError::BadStartup())?;

            //TODO: Upon getting a startup message we should be checking for a database and user
            //We should also check for configured authentication methods... maybe later!
            //   we're just going to let them in so we can get further on message parsing.
            info!("Just going to let {:?} in", message.get("user"));
            return Ok(vec![
                NetworkFrame::authentication_ok(),
                NetworkFrame::ready_for_query(),
            ]);
        }

        //Support basic query
        if frame.message_type == b'Q' {
            debug!("Got query {:?}", payload_buff);

            //Convert to utf8
            let query_str = String::from_utf8(payload_buff.to_vec())?;

            let txid = self.transaction_manager.start_trans().await?;

            //TODO this should not just return Err, I should transform to an error response instead
            let query_res = self.engine.process_query(txid, query_str).await?;

            let mut frames = vec![];
            if query_res.columns.len() > 0 {
                frames.push(NetworkFrame::row_description(query_res.columns)?);
            }
            let results_rows = query_res.rows.len();
            if results_rows > 0 {
                frames.append(&mut NetworkFrame::data_rows(query_res.rows)?);
            }

            frames.push(NetworkFrame::command_complete(format!(
                "SELECT {}",
                results_rows
            )));

            frames.push(NetworkFrame::ready_for_query());

            return Ok(frames);
        }

        warn!(
            "Got a message we don't understand yet {}",
            frame.message_type
        );
        Ok(vec![NetworkFrame::error_response(
            PgErrorLevels::Error,
            PgErrorCodes::SystemError,
            "Got an unimplemented message".to_string(),
        )])
    }
}

#[derive(Error, Debug)]
pub enum ClientProcessorError {
    #[error("Malformed Startup Packet")]
    BadStartup(),
    #[error(transparent)]
    EngineError(#[from] EngineError),
    #[error(transparent)]
    NetworkFrameError(#[from] NetworkFrameError),
    #[error(transparent)]
    QueryNotUtf8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    TransactionManagerError(#[from] TransactionManagerError),
}
