/// Inner type that implements the actual I/O operations so the outter type can
/// handle queue management.
use super::{request_type::RequestType, response_type::ResponseType};
use crate::constants::PAGE_SIZE;
use crate::engine::io::page_formats::PageOffset;
use bytes::{Bytes, BytesMut};
use futures::AsyncBufReadExt;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ffi::OsStr;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;
use std::{
    ffi::OsString,
    io::SeekFrom,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tokio::fs;
use tokio::{
    fs::{read_dir, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc::UnboundedReceiver, oneshot::Sender},
};
use uuid::Uuid;

/// Linux seems to limit to 1024, macos 256, windows 512 but I'm staying low until
/// a benchmark proves I need to change it.
const MAX_FILE_HANDLE_COUNT: usize = 128;
/// Number of characters per folder
const PREFIX_LEN: usize = 2;
/// Empty page buffer
const EMPTY_BUFFER: [u8; 16] = [0u8; 16];

#[derive(Debug)]
pub struct FileExecutor {
    data_dir: PathBuf,
    receive_queue: UnboundedReceiver<(
        Uuid,
        RequestType,
        Sender<Result<ResponseType, FileExecutorError>>,
    )>,
    receive_shutdown: UnboundedReceiver<Sender<()>>,
}

impl FileExecutor {
    pub fn new(
        raw_path: OsString,
        receive_queue: UnboundedReceiver<(
            Uuid,
            RequestType,
            Sender<Result<ResponseType, FileExecutorError>>,
        )>,
        receive_shutdown: UnboundedReceiver<Sender<()>>,
    ) -> Result<FileExecutor, FileExecutorError> {
        let data_dir = Path::new(&raw_path).to_path_buf();

        if !data_dir.is_dir() {
            return Err(FileExecutorError::NeedDirectory(
                data_dir.to_string_lossy().to_string(),
            ));
        }

        Ok(FileExecutor {
            data_dir,
            receive_queue,
            receive_shutdown,
        })
    }

    pub async fn start(&mut self) {
        let mut resource_lookup: HashMap<Uuid, PageOffset> = HashMap::new();
        let mut shutdown_sender = None;
        loop {
            tokio::select! {
                biased;
                shut_sender = self.receive_shutdown.recv() => {
                    if let Some(sender) = shut_sender {
                        shutdown_sender = Some(sender);
                        self.receive_queue.close();
                        info!("Got shutdown request");
                    } else {}
                }
                maybe_recv = self.receive_queue.recv() => {
                    if let Some((resource_key, req_type, response_channel)) = maybe_recv {
                            let result = self
                            .handle_request(&mut resource_lookup, &resource_key, &req_type)
                            .await;
                        response_channel
                            .send(result)
                            .unwrap_or_else(|_| error!("Unable to response back."));
                    } else {
                        break;
                    }
                }
            }
        }
        match shutdown_sender {
            Some(s) => {
                s.send(())
                    .unwrap_or_else(|_| warn!("Unable to signal shutdown."));
            }
            None => {
                error!("Exitting before processing all I/O!");
            }
        }
    }

    async fn handle_request(
        &self,
        resource_lookup: &mut HashMap<Uuid, PageOffset>,
        resource_key: &Uuid,
        req_type: &RequestType,
    ) -> Result<ResponseType, FileExecutorError> {
        //Find the resource key's latest offset so we can iterate on it for adds
        let next_po = match resource_lookup.get(&resource_key) {
            Some(po) => po.clone(),
            None => {
                let po = self.find_next_offset(&resource_key).await?;
                resource_lookup.insert(resource_key.clone(), po);
                po
            }
        };

        /* Goals here:
            Process each request.
            If requests access the SAME file, they cannot happen at the same time.
                PageOffset can tell me that now.
            Requests to the same file should reuse file handles.
            Requests to add should be ordered per uuid.
        */

        match req_type {
            RequestType::Add(buffer) => {
                let mut buffer = buffer.clone();

                resource_lookup.insert(resource_key.clone(), next_po.next());
                let file_path = self.make_file_path(&resource_key, &next_po).await?;

                //Need a file handle
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(file_path)
                    .await?;

                file.set_len(u64::try_from(next_po.get_file_chunk_size())?)
                    .await?;
                file.seek(SeekFrom::Start(u64::try_from(
                    next_po.get_file_chunk_size(),
                )?))
                .await?;

                file.write_all_buf(&mut buffer).await?;

                return Ok(ResponseType::Add(next_po));
            }
            RequestType::Read(po) => {
                let file_path = self.make_file_path(&resource_key, &po).await?;
                let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);

                let mut file = File::open(file_path).await?;

                file.seek(SeekFrom::Start(u64::try_from(po.get_file_chunk_size())?))
                    .await?;

                while buffer.len() != PAGE_SIZE as usize {
                    let readamt = file.read_buf(&mut buffer).await?;
                    if readamt == 0 as usize {
                        return Err(FileExecutorError::IncompleteRead(readamt, PAGE_SIZE));
                    }
                }
                Ok(ResponseType::Read(buffer.freeze()))
            }
            RequestType::Update((po, buffer)) => {
                let mut buffer = buffer.clone();
                let file_path = self.make_file_path(&resource_key, &po).await?;

                //Need a file handle
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(file_path)
                    .await?;

                file.seek(SeekFrom::Start(u64::try_from(po.get_file_chunk_size())?))
                    .await?;

                file.write_all_buf(&mut buffer).await?;
                return Ok(ResponseType::Update(()));
            }
        }
    }

    async fn make_file_path(
        &self,
        resource_key: &Uuid,
        offset: &PageOffset,
    ) -> Result<PathBuf, FileExecutorError> {
        let mut sub_path = self.make_sub_path(&resource_key).await?;
        let target_filename = Self::format_uuid(&resource_key);
        let target_extension = offset.get_file_number();
        let name = format!("{0}.{1}", target_filename, target_extension);
        sub_path.push(name);
        Ok(sub_path)
    }

    async fn find_next_offset(&self, resource_key: &Uuid) -> Result<PageOffset, FileExecutorError> {
        let (path, count) = match self.search_for_max_file(resource_key).await? {
            Some((p, c)) => (p, c),
            None => {
                return Ok(PageOffset(0));
            }
        };

        let mut file = File::open(path.clone()).await?;
        let file_meta = file.metadata().await?;
        let file_len = file_meta.len();

        if file_len % PAGE_SIZE as u64 != 0 {
            return Err(FileExecutorError::IncorrectPageSize(file_len, path));
        }

        // If this fails you are probably on a 32bit platform and
        // have changed the PAGE_SIZE constant. I would reduce PAGE_SIZE.
        let file_len = usize::try_from(file_len)?;

        //Now we need to scan backwards in the file to make sure we find the last non-zero page.
        let mut in_file_len = file_len.saturating_sub(PAGE_SIZE as usize);
        while in_file_len != 0 {
            let in_file_len_u64 = u64::try_from(in_file_len)?;
            file.seek(SeekFrom::Start(in_file_len_u64)).await?;

            //Each page should start with a non-zero number within the first 16 bytes, if it has data
            let mut buffer = BytesMut::with_capacity(EMPTY_BUFFER.len());
            file.read_buf(&mut buffer).await?;
            let buffer = buffer.freeze();
            if buffer == Bytes::from_static(&EMPTY_BUFFER) {
                //Okay we keep going
                in_file_len = file_len.saturating_sub(PAGE_SIZE as usize);
                continue;
            } else {
                //We can calucate our page offset now
                let po = PageOffset::calculate_page_offset(count, in_file_len);
                return Ok(po);
            }
        }

        //Okay so the file is empty
        let po = PageOffset::calculate_page_offset(count, in_file_len);
        Ok(po)
    }

    /// This will search for the highest numbered file for the Uuid
    async fn search_for_max_file(
        &self,
        resource_key: &Uuid,
    ) -> Result<Option<(PathBuf, usize)>, FileExecutorError> {
        let sub_path = self.make_sub_path(&resource_key).await?;
        let target_filename = Self::format_uuid(&resource_key);

        let mut max_file_count = 0;
        let mut max_file_path = None;

        let mut files = read_dir(sub_path).await?;
        while let Some(entry) = files.next_entry().await? {
            let path = entry.path();
            let file_stem = match path.file_stem() {
                Some(s) => Self::format_os_string(s),
                None => {
                    continue;
                }
            };
            let file_ext = match path.extension() {
                Some(s) => Self::format_os_string(s),
                None => {
                    continue;
                }
            };
            if !file_stem.eq(&target_filename) {
                continue;
            }
            let file_count = match file_ext.parse::<usize>() {
                Ok(s) => s,
                Err(_) => {
                    continue;
                }
            };

            if file_count > max_file_count {
                max_file_count = file_count;
                max_file_path = Some(path);
            }
        }

        match max_file_path {
            Some(s) => Ok(Some((s, max_file_count))),
            None => Ok(None),
        }
    }

    //Makes the prefix folder so we don't fill up folders. Will consider more nesting eventually
    async fn make_sub_path(&self, resource_key: &Uuid) -> Result<PathBuf, FileExecutorError> {
        let subfolder = Self::get_uuid_prefix(&resource_key);

        let mut path = PathBuf::new();
        path.push(self.data_dir.to_path_buf());
        path.push(subfolder);

        match fs::create_dir(path.as_path()).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(FileExecutorError::IOError(e)),
        }
        Ok(path)
    }

    fn format_uuid(input: &Uuid) -> String {
        let mut buf = [b'0'; 32];
        input.to_simple().encode_lower(&mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    }

    fn format_os_string(input: &OsStr) -> String {
        input.to_ascii_lowercase().to_string_lossy().into_owned()
    }

    fn get_uuid_prefix(input: &Uuid) -> String {
        let mut buf = [b'0'; 32];
        input.to_simple().encode_lower(&mut buf);
        String::from_utf8_lossy(&buf[..PREFIX_LEN]).into_owned()
    }
}

#[derive(Debug, Error)]
pub enum FileExecutorError {
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("Read {0} bytes instead of the required {1}")]
    IncompleteRead(usize, u16),
    #[error("Incorrect page size of {0} on file {1} found. System cannot function")]
    IncorrectPageSize(u64, PathBuf),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("Need a directory to store the data. Got ({0}) may be stripped of non Unicode chars.")]
    NeedDirectory(String),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
}

#[cfg(test)]
mod tests {
    use hex_literal::hex;

    use super::*;

    #[test]
    fn test_uuid_formating() -> Result<(), Box<dyn std::error::Error>> {
        let hex = "ee89957f3e9f482c836dda6c349ac632";
        let test = Uuid::from_bytes(hex!("ee89957f3e9f482c836dda6c349ac632"));
        assert_eq!(hex, FileExecutor::format_uuid(&test));

        assert_eq!("ee", FileExecutor::get_uuid_prefix(&test));

        Ok(())
    }
}
