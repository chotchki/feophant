use bytes::{Buf, Bytes, BytesMut};
use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::{
    ffi::OsString,
    io::SeekFrom,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tokio::fs;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::{self, Sender};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use uuid::Uuid;

use crate::constants::PAGE_SIZE;

use super::page_formats::{PageOffset, UInt12, UInt12Error};

/*
This is a different approach than I had done before. This file manager runs its own loop based on a spawned task
since the prior approach was too lock heavy and I couldn't figure out an approach that didn't starve resources.

*/

/// Linux seems to limit to 1024, macos 256, windows 512 but I'm staying low until
/// a benchmark proves I need to change it.
const MAX_FILE_HANDLE_COUNT: usize = 128;
const PAGES_PER_FILE: usize = 256; //Max file size is 1GB <Result<Bytes, FileManagerError>>

struct FileManager {
    data_dir: PathBuf,
    request_queue: UnboundedSender<(
        PathBuf,
        usize,
        RequestType,
        Sender<Result<ResponseType, FileManagerError>>,
    )>,
    recieve_queue: UnboundedReceiver<(
        PathBuf,
        usize,
        RequestType,
        Sender<Result<ResponseType, FileManagerError>>,
    )>,
}

#[derive(Debug, Clone)]
enum RequestType {
    Add(Bytes),
    Read(()),
    Update(Bytes),
}

#[derive(Debug, Clone)]
enum ResponseType {
    Add(PageOffset),
    Read(Bytes),
    Update(()),
}

impl FileManager {
    pub fn new(raw_path: OsString) -> Result<FileManager, FileManagerError> {
        let data_dir = Path::new(&raw_path).to_path_buf();

        if !data_dir.is_dir() {
            return Err(FileManagerError::NeedDirectory(
                data_dir.to_string_lossy().to_string(),
            ));
        }

        let (request_queue, recieve_queue) = mpsc::unbounded_channel();

        Ok(FileManager {
            data_dir: Path::new(&raw_path).to_path_buf(),
            request_queue,
            recieve_queue,
        })
    }

    pub async fn add_page(
        &self,
        resource_key: &Uuid,
        page: Bytes,
    ) -> Result<PageOffset, IOManagerError> {
        let size = UInt12::try_from(page.len() - 1)?;
        if size != UInt12::max() {
            return Err(FileManagerError::InvalidPageSize(page.len()));
        }

        let path = self.construct_path(resource_key, offset).await?;
        let offset_in_file = offset.0 % PAGES_PER_FILE * PAGE_SIZE as usize;
    }

    pub async fn get_page(
        &self,
        resource_key: &Uuid,
        offset: &PageOffset,
    ) -> Result<Bytes, FileManagerError> {
        let path = self.construct_path(resource_key, offset).await?;
        let offset_in_file = offset.0 % PAGES_PER_FILE * PAGE_SIZE as usize;

        let (error_request, error_reciever) = oneshot::channel();

        self.request_queue
            .send((path, offset_in_file, RequestType::Read(()), error_request))?;

        match error_reciever.await?? {
            ResponseType::Add(_) => Err(FileManagerError::UnexpectedAdd()),
            ResponseType::Read(b) => Ok(b),
            ResponseType::Update(_) => Err(FileManagerError::UnexpectedUpdate()),
        }
    }

    pub async fn update_page(
        &self,
        resource_key: &Uuid,
        page: Bytes,
        offset: &PageOffset,
    ) -> Result<(), FileManagerError> {
        let size = UInt12::try_from(page.len() - 1)?;
        if size != UInt12::max() {
            return Err(FileManagerError::InvalidPageSize(page.len()));
        }

        let path = self.construct_path(resource_key, offset).await?;
        let offset_in_file = offset.0 % PAGES_PER_FILE * PAGE_SIZE as usize;

        let (error_request, error_reciever) = oneshot::channel();

        self.request_queue.send((
            path,
            offset_in_file,
            RequestType::Update(page),
            error_request,
        ))?;

        match error_reciever.await?? {
            ResponseType::Add(_) => Err(FileManagerError::UnexpectedAdd()),
            ResponseType::Read(_) => Err(FileManagerError::UnexpectedRead()),
            ResponseType::Update(_) => Ok(()),
        }
    }

    //Todo should work on file handles
    async fn read_file_chunk(path: &Path, position: usize) -> Result<Bytes, FileManagerError> {
        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        let position_u64 = u64::try_from(position)?;

        let mut file = File::open(path).await?;
        file.seek(SeekFrom::Start(position_u64)).await?;

        while buffer.len() != PAGE_SIZE as usize {
            let readamt = file.read_buf(&mut buffer).await?;
            if readamt == 0 as usize {
                return Err(FileManagerError::IncompleteRead(readamt, PAGE_SIZE));
            }
        }

        Ok(buffer.freeze())
    }

    //Todo should work on file handles
    async fn write_file_chunk(
        path: &Path,
        position: usize,
        buffer: &mut impl Buf,
    ) -> Result<(), FileManagerError> {
        let position_u64 = u64::try_from(position)?;

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .await?;
        let mut file = File::open(path).await?;
        file.seek(SeekFrom::Start(position_u64)).await?;

        file.write_all_buf(buffer).await?;

        Ok(())
    }

    /// Construct what the path should be, roughly at the moment we are doing the following format
    /// (2 chars of Uuid)/(Uuid).(count based on offset)
    async fn construct_path(
        &self,
        resource_key: &Uuid,
        offset: &PageOffset,
    ) -> Result<PathBuf, FileManagerError> {
        let resource = resource_key.to_simple().to_string();
        let subfolder = format!("{:?}", &resource.as_bytes()[..2]); //TODO find a better way to do this

        let filename_num = offset.0 / PAGES_PER_FILE;
        let filename = format!("{0}.{1}", resource, filename_num);

        let mut path = self.data_dir.clone();
        path.push(subfolder);

        match fs::create_dir(path.clone()).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(FileManagerError::IOError(e)),
        }

        path.push(filename);

        Ok(path)
    }
}

#[derive(Debug, Error)]
pub enum FileManagerError {
    #[error("Read {0} bytes instead of the required {1}")]
    IncompleteRead(usize, u16),
    #[error("Invalid Page size of {0}")]
    InvalidPageSize(usize),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("Need a directory to store the data. Got ({0}) may be stripped of non Unicode chars.")]
    NeedDirectory(String),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error(transparent)]
    RecvError(#[from] RecvError),
    #[error(transparent)]
    SendError(
        #[from]
        SendError<(
            PathBuf,
            usize,
            RequestType,
            Sender<Result<ResponseType, FileManagerError>>,
        )>,
    ),
    #[error(transparent)]
    UInt12Error(#[from] UInt12Error),
    #[error("Unexpected Add Response")]
    UnexpectedAdd(),
    #[error("Unexpected Read Response")]
    UnexpectedRead(),
    #[error("Unexpected Update Response")]
    UnexpectedUpdate(),
}
