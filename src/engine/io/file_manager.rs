//! This is a different approach than I had done before. This file manager runs its own loop based on a spawned task
//! since the prior approach was too lock heavy and I couldn't figure out an approach that didn't starve resources.
use super::page_formats::{PageId, PageOffset, UInt12, UInt12Error};
use bytes::{Bytes, BytesMut};
use std::convert::TryFrom;
use std::ffi::OsString;
use std::num::TryFromIntError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::{self, Sender};

//Inner Types
mod file_executor;
use file_executor::FileExecutor;
use file_executor::FileExecutorError;
mod file_operations;
mod request_type;
use request_type::RequestType;
mod resource_formatter;
pub use resource_formatter::ResourceFormatter;

#[derive(Clone, Debug)]
pub struct FileManager {
    request_queue: UnboundedSender<(PageId, RequestType)>,
    request_shutdown: UnboundedSender<Sender<()>>,
}

impl FileManager {
    pub fn new(raw_path: OsString) -> Result<FileManager, FileManagerError> {
        let (request_queue, receive_queue) = mpsc::unbounded_channel();
        let (request_shutdown, receive_shutdown) = mpsc::unbounded_channel();

        let mut file_executor = FileExecutor::new(raw_path, receive_queue, receive_shutdown)?;

        tokio::spawn(async move {
            file_executor.start().await;
        });

        Ok(FileManager {
            request_queue,
            request_shutdown,
        })
    }

    pub async fn shutdown(&self) -> Result<(), FileManagerError> {
        let (res_shutdown, rev_shutdown) = oneshot::channel();
        self.request_shutdown.clone().send(res_shutdown)?;

        Ok(rev_shutdown.await?)
    }

    pub async fn get_offset(&self, page_id: &PageId) -> Result<PageOffset, FileManagerError> {
        let (res_request, res_receiver) = oneshot::channel();

        self.request_queue
            .send((*page_id, RequestType::GetOffset(res_request)))?;

        Ok(res_receiver.await??)
    }

    pub async fn add_page(
        &self,
        page_id: &PageId,
        offset: &PageOffset,
        page: Bytes,
    ) -> Result<(), FileManagerError> {
        let size = UInt12::try_from(page.len() - 1)?;
        if size != UInt12::max() {
            return Err(FileManagerError::InvalidPageSize(page.len()));
        }

        let (res_request, res_receiver) = oneshot::channel();

        self.request_queue
            .send((*page_id, RequestType::Add((*offset, page, res_request))))?;

        Ok(res_receiver.await??)
    }

    pub async fn get_page(
        &self,
        page_id: &PageId,
        offset: &PageOffset,
    ) -> Result<Option<BytesMut>, FileManagerError> {
        let (res_request, res_receiver) = oneshot::channel();

        self.request_queue
            .send((*page_id, RequestType::Read((*offset, res_request))))?;

        Ok(res_receiver.await??)
    }

    pub async fn update_page(
        &self,
        page_id: &PageId,
        offset: &PageOffset,
        page: Bytes,
    ) -> Result<(), FileManagerError> {
        let size = UInt12::try_from(page.len() - 1)?;
        if size != UInt12::max() {
            return Err(FileManagerError::InvalidPageSize(page.len()));
        }

        let (res_request, res_receiver) = oneshot::channel();

        self.request_queue
            .send((*page_id, RequestType::Update((*offset, page, res_request))))?;

        Ok(res_receiver.await??)
    }
}

impl Drop for FileManager {
    fn drop(&mut self) {
        if self.request_queue.is_closed() {
            return;
        }
        error!("File Manager wasn't shutdown cleanly! This is a bug, please report!");
    }
}

#[derive(Debug, Error)]
pub enum FileManagerError {
    #[error(transparent)]
    FileExecutorError(#[from] FileExecutorError),
    #[error("Read {0} bytes instead of the required {1}")]
    IncompleteRead(usize, u16),
    #[error("Invalid Page size of {0}")]
    InvalidPageSize(usize),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("Need a directory to store the data. Got ({0}) may be stripped of non Unicode chars.")]
    NeedDirectory(String),
    #[error("The backend processor is not running.")]
    NotRunning(),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error(transparent)]
    RecvError(#[from] RecvError),
    #[error(transparent)]
    SendError(#[from] SendError<(PageId, RequestType)>),
    #[error(transparent)]
    ShutdownSendError(#[from] SendError<Sender<()>>),
    #[error(transparent)]
    UInt12Error(#[from] UInt12Error),
    #[error("Unexpected Add Response")]
    UnexpectedAdd(),
    #[error("Unexpected Read Response")]
    UnexpectedRead(),
    #[error("Unexpected Update Response")]
    UnexpectedUpdate(),
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::{constants::PAGE_SIZE, engine::io::page_formats::PageType};

    use super::*;

    fn get_test_page(fill: u8) -> Bytes {
        let mut test_page = BytesMut::with_capacity(PAGE_SIZE as usize);
        let free_space = vec![fill; PAGE_SIZE as usize];
        test_page.extend_from_slice(&free_space);
        test_page.freeze()
    }

    #[tokio::test]
    async fn test_roundtrips() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path();

        let fm = FileManager::new(tmp_dir.as_os_str().to_os_string())?;

        let page_id = PageId {
            resource_key: Uuid::new_v4(),
            page_type: PageType::Data,
        };

        let test_page = get_test_page(1);
        let test_po = fm.get_offset(&page_id).await?;
        fm.add_page(&page_id, &test_po, test_page.clone()).await?;

        assert_eq!(test_po, PageOffset(0));

        let test_page_get = fm.get_page(&page_id, &test_po).await?.unwrap();

        assert_eq!(test_page, test_page_get);

        let test_page2 = get_test_page(2);
        fm.update_page(&page_id, &test_po, test_page2.clone())
            .await?;

        let test_page_get2 = fm.get_page(&page_id, &test_po).await?.unwrap();

        assert_eq!(test_page2, test_page_get2);

        fm.shutdown().await.unwrap();

        let fm2 = FileManager::new(tmp_dir.as_os_str().to_os_string())?;
        let test_page3 = get_test_page(3);
        let test_po3 = fm2.get_offset(&page_id).await?;
        fm2.add_page(&page_id, &test_po3, test_page3.clone())
            .await?;
        assert!(test_po3 > test_po);

        let test_page_get2 = fm2.get_page(&page_id, &test_po).await?.unwrap();

        assert_eq!(test_page2, test_page_get2);

        Ok(())
    }
}
