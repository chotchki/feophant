use bytes::Bytes;
use std::sync::Arc;
use thiserror::Error;

use super::super::engine::io::PageManager;
use super::super::engine::TransactionGenerator;
use super::ssl_and_gssapi_parser;
use super::startup_parser;
use crate::codec::{authentication_ok, error_response, ready_for_query, NetworkFrame};
use crate::constants::{PgErrorCodes, PgErrorLevels};

pub struct ClientProcessor {
    page_manager: Arc<PageManager>,
    transaction_generator: Arc<TransactionGenerator>,
}

impl ClientProcessor {
    pub fn new(
        page_manager: Arc<PageManager>,
        transaction_generator: Arc<TransactionGenerator>,
    ) -> ClientProcessor {
        ClientProcessor {
            page_manager: page_manager,
            transaction_generator: transaction_generator,
        }
    }

    pub fn process(&self, frame: NetworkFrame) -> Result<Vec<NetworkFrame>, ClientProcessorError> {
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
            return Ok(vec![authentication_ok(), ready_for_query()]);
        }

        //Support basic query
        if frame.message_type == b'Q' {
            debug!("Got query {:?}", payload_buff);

            //first query is "create table foo(bar u32);"
            //Parse to the following commands
            //Get XID
            //Call to TransGen
            //Does table already exist? -> Error
            //Scan pg_class for table name
            //Look up definition of pg_class (hardcoded)
            //command::getDefinition(name) -> Result<PgTable, Err>
            //Use that info to parse a page of data for rows
            //Check each row to match on table name
            //return row if found
            //Add entry for table
            //Look up definition of pg_class (hardcoded)
            //Prepare new row entry
            //Scan for a page with the empty space for the row
            //Rewrite the page with the row
            //Replace page with new row
            //Add entry for column + type
            //Do the same thing for the table type with pg_attribute

            //let commands:vec[Commands] = Parse the query
        }

        warn!(
            "Got a message we don't understand yet {}",
            frame.message_type
        );
        Ok(vec![error_response(
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
    //#[error("Unknown Message")]
    //Unknown(),
}
