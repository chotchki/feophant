use bytes::Bytes;
use bytes::{BufMut, BytesMut};
use std::convert::TryFrom;
use std::num::TryFromIntError;
use thiserror::Error;

use crate::constants::{PgErrorCodes, PgErrorLevels};
use crate::engine::objects::SqlTuple;

#[derive(Clone, Debug)]
pub struct NetworkFrame {
    pub message_type: u8,
    pub payload: Bytes,
}

impl NetworkFrame {
    pub fn new(message_type: u8, payload: Bytes) -> NetworkFrame {
        NetworkFrame {
            message_type,
            payload,
        }
    }
    pub fn authentication_ok() -> NetworkFrame {
        NetworkFrame::new(b'R', Bytes::from_static(b"\0\0\0\0"))
    }

    pub fn command_complete(command_tag: String) -> NetworkFrame {
        let mut buffer = BytesMut::new();

        buffer.put(command_tag.as_bytes());
        buffer.put_u8(b'\0');

        NetworkFrame::new(b'C', buffer.freeze())
    }

    pub fn data_rows(rows: Vec<SqlTuple>) -> Result<Vec<NetworkFrame>, NetworkFrameError> {
        let mut frames = vec![];

        for row in rows {
            let mut buffer = BytesMut::new();

            let column_count = u16::try_from(row.0.len())?;
            buffer.put_u16(column_count);

            for field in row.0.into_iter() {
                match field {
                    Some(f) => {
                        let f_str = format!("{}", f);
                        let f_bytes = f_str.as_bytes();
                        let f_len = i32::try_from(f_bytes.len())?;
                        buffer.put_i32(f_len);
                        buffer.put(f_bytes);
                    }
                    None => {
                        buffer.put_i32(-1);
                    }
                }
            }

            frames.push(NetworkFrame::new(b'D', buffer.freeze()));
        }

        Ok(frames)
    }

    //Note this claims that the server is ALWAYS ready, even if its not
    pub fn ready_for_query() -> NetworkFrame {
        NetworkFrame::new(b'Z', Bytes::from_static(b"I"))
    }

    pub fn row_description(column_names: Vec<String>) -> Result<NetworkFrame, NetworkFrameError> {
        let mut buffer = BytesMut::new();

        let field_count = u16::try_from(column_names.len())?;
        buffer.put_u16(field_count);

        for column in column_names {
            buffer.put(column.as_bytes());
            buffer.put_u8(b'\0');

            //The next following fields are going to be dummied out unless testing shows I need them.
            //https://www.postgresql.org/docs/current/protocol-message-formats.html
            buffer.put_u32(0); //Table OID
            buffer.put_u16(0); //Table Column
            buffer.put_u32(0); //Type OID
            buffer.put_i16(0); //Type length
            buffer.put_i32(0); //Type specifier
            buffer.put_i16(0); //Format code, we're doing text for everything
        }

        Ok(NetworkFrame::new(b'T', buffer.freeze()))
    }

    //Valid severities can be found here: https://www.postgresql.org/docs/current/protocol-error-fields.html
    //Valid error codes can be found here: https://www.postgresql.org/docs/current/errcodes-appendix.html
    pub fn error_response(
        severity: PgErrorLevels,
        code: PgErrorCodes,
        message: String,
    ) -> NetworkFrame {
        let mut buffer = BytesMut::new();
        buffer.put_u8(b'S'); //Severity
        buffer.put(severity.value());
        buffer.put_u8(b'\0');
        buffer.put_u8(b'M'); //Code
        buffer.put(message.as_bytes());
        buffer.put_u8(b'\0');
        buffer.put_u8(b'C'); //Code
        buffer.put(code.value());
        buffer.put_u8(b'\0');
        buffer.put_u8(b'\0');

        NetworkFrame::new(
            b'N', //Testing notifications
            buffer.freeze(),
        )
    }
}

#[derive(Error, Debug)]
pub enum NetworkFrameError {
    #[error(transparent)]
    TooManyFields(#[from] TryFromIntError),
}
