//! The intent of this struct is to provide a managed interface to read and write pages from the file system.
//!
//! Application level locking will be done at a higher level, but I will manage the number of file handles so
//! I don't exhaust the various kernel limits.
/*
what does this need to do?

Interface same as IO Manager
    pub async fn get_page(ID,PageOffset) -> Page
    pub async fn add_page(ID, Page)
    pub async fn update_page(ID, PageOffset, Page)

Internally I want to map Uuid + PageOffset to a set of 1GB files like postgres.
    So if get is called I should do the following
        Try to open the file (filename.count.dat), count should be calculated off offset
        Seek to the file offset
        Read the file chunk
        Return to the caller
    If add is called
        I need to figure out where the next file should be and its offset
        Try to open the file (filename.count.dat), count should be calculated off offset
        Seek to the file offset / resize the file
        Write the chunk
        Return to the caller

What constraints should I have?
    I should only open a single file handle per file since otherwise I risk corruption.
    I should only open a limited number of file handles so that I don't exhaust limits.

This makes me think I should have a:
    Arc<Mutex<HashMap<filename, Vec<Notify>>
        Everyone gets the mutex, checks if someone has the filename:
            Some => Add an entry in the Vec to notify when the file is availible.
                    Unlock the mutex
            None => Add to the hashmap the file
                    Unlock the mutex

    Once its your turn, try to get a permit from the semaphore
        Once gotten, do your I/O

    Once I/O is done go back to the HashMap, lock the mutex.
        check if anyone else is waiting
            Some => shift off the entry notify them
                Unlock the mutex
            None => Delete the key from the Hash Map

*/

use crate::constants::PAGE_SIZE;

use super::page_formats::PageOffset;
use bytes::{Buf, Bytes, BytesMut};
use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::{
    collections::{HashMap, VecDeque},
    ffi::OsString,
    fs,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{AcquireError, Mutex, Notify, Semaphore},
};
use uuid::Uuid;

/// Linux seems to limit to 1024, macos 256, windows 512 but I'm staying low until
/// a benchmark proves I need to change it.
const MAX_FILE_HANDLE_COUNT: usize = 128;
const PAGES_PER_FILE: usize = 256; //Max file size is 1GB

pub struct FileManager {
    data_dir: PathBuf,
    filename_check: Arc<Mutex<HashMap<PathBuf, VecDeque<Arc<Notify>>>>>,
    filehandle_limit: Semaphore,
}

impl FileManager {
    pub fn new(raw_path: OsString) -> Result<FileManager, FileManagerError> {
        let data_dir = Path::new(&raw_path).to_path_buf();

        if !data_dir.is_dir() {
            return Err(FileManagerError::NeedDirectory(
                data_dir.to_string_lossy().to_string(),
            ));
        }

        Ok(FileManager {
            data_dir: Path::new(&raw_path).to_path_buf(),
            filename_check: Arc::new(Mutex::new(HashMap::new())),
            filehandle_limit: Semaphore::new(MAX_FILE_HANDLE_COUNT),
        })
    }

    pub async fn get_page(
        &self,
        resource_key: &Uuid,
        offset: &PageOffset,
    ) -> Result<Bytes, FileManagerError> {
        let path = self.construct_path(resource_key, offset)?;
        let offset_in_file = offset.0 % PAGES_PER_FILE * PAGE_SIZE as usize;

        self.add_file_entry(&path).await;

        //Reminder, past this point we MUST cleanup the file entry, no ? use

        match self.filehandle_limit.acquire().await {
            Ok(_) => {
                let res = Self::read_file_chunk(&path, offset_in_file).await;
                self.release_file_entry(&path).await;
                match res {
                    Ok(o) => {
                        return Ok(o);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                self.release_file_entry(&path).await;
                return Err(FileManagerError::AcquireError(e));
            }
        }
    }

    async fn add_file_entry(&self, path: &PathBuf) {
        let mut f_check = self.filename_check.lock().await;
        match f_check.get_mut(path) {
            Some(v_n) => {
                //Now we need to wait for the other writer to be done
                let notify = Arc::new(Notify::new());
                v_n.push_back(notify.clone());
                drop(f_check);
                notify.notified().await;
            }
            None => {
                f_check.insert(path.clone(), VecDeque::new());
            }
        }
    }

    async fn release_file_entry(&self, path: &PathBuf) {
        let mut f_check = self.filename_check.lock().await;
        match f_check.get_mut(path) {
            Some(v_n) => match v_n.pop_front() {
                Some(s) => {
                    s.notify_one();
                }
                None => {
                    f_check.remove(path);
                }
            },
            None => {
                //Shouldn't get here but doesn't matter
            }
        }
    }

    async fn read_file_chunk(path: &Path, position: usize) -> Result<Bytes, FileManagerError> {
        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        let position_u64 = u64::try_from(position)?;

        let mut file = File::open(path).await?;
        file.seek(SeekFrom::Start(position_u64)).await?;

        file.read_exact(&mut buffer);

        Ok(buffer.freeze())
    }

    async fn write_file_chunk(
        path: &Path,
        position: usize,
        buffer: impl Buf,
    ) -> Result<(), FileManagerError> {
        let position_u64 = u64::try_from(position)?;

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .await?;
        let mut file = File::open(path).await?;
        file.seek(SeekFrom::Start(position_u64)).await?;

        file.write_all(&buffer).await?;

        Ok(())
    }

    /// Construct what the path should be, roughly at the moment we are doing
    /// (2 chars of Uuid)/(Uuid).(count based on offset)
    fn construct_path(
        &self,
        resource_key: &Uuid,
        offset: &PageOffset,
    ) -> Result<PathBuf, FileManagerError> {
        let resource = resource_key
            .to_simple()
            .encode_lower(&mut Uuid::encode_buffer());
        let subfolder = &resource[..2];

        let filename_num = offset.0 / PAGES_PER_FILE;
        let filename = format!("{0}.{1}", resource, filename_num);

        let mut path = self.data_dir.clone();
        path.push(subfolder);

        match fs::create_dir(path) {
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
    #[error(transparent)]
    AcquireError(#[from] AcquireError),
    #[error("Need a directory to store the data. Got ({0}) may be stripped of non Unicode chars.")]
    NeedDirectory(String),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
}
