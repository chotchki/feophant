use crate::engine::io::page_formats::PageOffset;
use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot::Sender;

use super::file_executor::FileExecutorError;

#[derive(Debug)]
pub enum RequestType {
    GetOffset(Sender<Result<PageOffset, FileExecutorError>>),
    Add((PageOffset, Bytes, Sender<Result<(), FileExecutorError>>)),
    Read(
        (
            PageOffset,
            Sender<Result<Option<BytesMut>, FileExecutorError>>,
        ),
    ),
    Update((PageOffset, Bytes, Sender<Result<(), FileExecutorError>>)),
}
