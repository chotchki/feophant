use crate::engine::io::page_formats::PageOffset;
use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot::Sender;

use super::file_executor::FileExecutorError;

#[derive(Debug)]
pub enum RequestType {
    Add((Bytes, Sender<Result<PageOffset, FileExecutorError>>)),
    Read(
        (
            PageOffset,
            Sender<Result<Option<BytesMut>, FileExecutorError>>,
        ),
    ),
    Update((PageOffset, Bytes, Sender<Result<(), FileExecutorError>>)),
}
