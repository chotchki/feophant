use super::file_operations::{FileOperations, FileOperationsError};
/// Inner type that implements the actual I/O operations so the outter type can
/// handle queue management.
use super::request_type::RequestType;
use crate::constants::PAGE_SIZE;
use crate::engine::io::file_manager::ResourceFormatter;
use crate::engine::io::page_formats::{PageId, PageOffset};
use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use lru::LruCache;
use std::collections::{HashMap, VecDeque};
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
use tokio::sync::mpsc;
use tokio::{
    fs::{read_dir, File},
    io::{AsyncReadExt, AsyncSeekExt},
    sync::{mpsc::UnboundedReceiver, oneshot::Sender},
};

/// Linux seems to limit to 1024, macos 256, windows 512 but I'm staying low until
/// a benchmark proves I need to change it.
const MAX_FILE_HANDLE_COUNT: usize = 128;
/// Empty page buffer
const EMPTY_BUFFER: [u8; 16] = [0u8; 16];

#[derive(Debug)]
pub struct FileExecutor {
    data_dir: PathBuf,
    receive_queue: UnboundedReceiver<(PageId, RequestType)>,
    receive_shutdown: UnboundedReceiver<Sender<()>>,
    resource_lookup: HashMap<PageId, PageOffset>,
}

impl FileExecutor {
    pub fn new(
        raw_path: OsString,
        receive_queue: UnboundedReceiver<(PageId, RequestType)>,
        receive_shutdown: UnboundedReceiver<Sender<()>>,
    ) -> Result<FileExecutor, FileExecutorError> {
        let data_dir = Path::new(&raw_path).to_path_buf();

        if !data_dir.is_dir() {
            return Err(FileExecutorError::NeedDirectory(
                data_dir.to_string_lossy().to_string(),
            ));
        }

        let resource_lookup = HashMap::new();

        Ok(FileExecutor {
            data_dir,
            receive_queue,
            receive_shutdown,
            resource_lookup,
        })
    }

    pub async fn start(&mut self) {
        /*
        Check if we have max jobs in flight, if so select on jobs+shutdown.
            else jobs+requests+shutdown

        1. Get request
        2. push onto fifo queue (bounded to max handles)
        3. Scan fifo queue, per item.
        4. For an item, take its uuid.
            4a. an add should be resolved to a page offset
        5. See if the uuid has other in flight operations.
            5a. this should check if the end file will be the same.
            5b. Same file equals skipping.
        6. If not, check if we have a file handle in the lru cache.
            6a. spawn a job with it if we do
            6b. other open one, store it and spawn a job
        7. The job should return the result to the calling process and the file handle + file its for back on the job queue

        , see if uuid has anything

        */

        let mut file_handles_open: usize = 0;

        // This cache is used to indicate when a file operation is in flight on a handle, there are two options:
        // * entry: some(file) -> Idle File Handle that can be used
        // * entry: None -> File handle in use but not returned
        let mut file_handle_cache: LruCache<(PageId, usize), Option<File>> =
            LruCache::new(MAX_FILE_HANDLE_COUNT);

        // This channel is used to restore or drop entries on the file handle cache, there are two options:
        // * Some(file) -> Idle handle to be stored
        // * None -> Failure/Error means there is not a handle and the entry should be dropped
        let (send_completed, mut receive_completed) = mpsc::unbounded_channel();

        // Queue used as a holding ground until a handle is availible for it to execute. Used in a FIFO fashion
        let mut request_queue: VecDeque<(PageId, RequestType)> = VecDeque::new();

        let mut shutdown_sender = None;

        //TODO All these match blocks suck but I don't see a way around them.
        //I'm hoping clippy yells at me once I get it compiling
        loop {
            tokio::select! {
                biased;
                shut_sender = self.receive_shutdown.recv() => {
                    if let Some(sender) = shut_sender {
                        shutdown_sender = Some(sender);
                        self.receive_queue.close();
                        debug!("File Executor: Got shutdown request");
                    } else {}
                }
                recv_completed = receive_completed.recv() => {
                    if let Some((resource_key, file_number, file_handle)) = recv_completed {
                        //If we didn't get a handle back, the file is no longer in use, delete the key
                        match file_handle {
                            Some(f) => {file_handle_cache.put((resource_key,file_number), Some(f));}
                            None => {
                                file_handle_cache.pop(&(resource_key,file_number));
                                file_handles_open = file_handles_open.saturating_sub(1);
                            }
                        }
                    } else {}
                }
                maybe_recv = self.receive_queue.recv(), if request_queue.len() < MAX_FILE_HANDLE_COUNT => {
                    if let Some((page_id, req_type)) = maybe_recv {
                        request_queue.push_back((page_id, req_type));
                    } else {
                        break;
                    }
                }
            }

            if file_handles_open < MAX_FILE_HANDLE_COUNT && !request_queue.is_empty() {
                let mut new_request_queue = VecDeque::with_capacity(request_queue.len());
                for (page_id, req_type) in request_queue.into_iter() {
                    match req_type {
                        RequestType::Add((po, a, response)) => {
                            match file_handle_cache.pop(&(page_id, po.get_file_number())) {
                                Some(maybe_file) => match maybe_file {
                                    Some(file) => {
                                        file_handle_cache
                                            .put((page_id, po.get_file_number()), None);
                                        let file_handle_ret = send_completed.clone();
                                        tokio::spawn(async move {
                                            let response_f = response;

                                            match FileOperations::add_chunk(file, &po, a).await {
                                                Ok(o) => {
                                                    let _ = file_handle_ret.send((
                                                        page_id,
                                                        po.get_file_number(),
                                                        Some(o),
                                                    ));
                                                    let _ = response_f.send(Ok(()));
                                                }
                                                Err(e) => {
                                                    let _ = file_handle_ret.send((
                                                        page_id,
                                                        po.get_file_number(),
                                                        None,
                                                    ));
                                                    let _ = response_f.send(Err(
                                                        FileExecutorError::FileOperationsError(e),
                                                    ));
                                                }
                                            }
                                        });
                                    }
                                    None => {
                                        //Request in flight, skip for now, but have to reinsert into cache
                                        file_handle_cache
                                            .put((page_id, po.get_file_number()), None);

                                        new_request_queue.push_back((
                                            page_id,
                                            RequestType::Add((po, a, response)),
                                        ));
                                        continue;
                                    }
                                },
                                None => {
                                    file_handle_cache.put((page_id, po.get_file_number()), None);
                                    file_handles_open = file_handles_open.saturating_add(1);
                                    let data_dir = self.data_dir.clone();
                                    let file_handle_ret = send_completed.clone();
                                    tokio::spawn(async move {
                                        let response_f = response;

                                        let file = match FileOperations::open_path(
                                            &data_dir,
                                            &page_id,
                                            po.get_file_number(),
                                        )
                                        .await
                                        {
                                            Ok(o) => o,
                                            Err(e) => {
                                                let _ = file_handle_ret.send((
                                                    page_id,
                                                    po.get_file_number(),
                                                    None,
                                                ));
                                                let _ = response_f.send(Err(
                                                    FileExecutorError::FileOperationsError(e),
                                                ));
                                                return;
                                            }
                                        };

                                        match FileOperations::add_chunk(file, &po, a).await {
                                            Ok(o) => {
                                                let _ = file_handle_ret.send((
                                                    page_id,
                                                    po.get_file_number(),
                                                    Some(o),
                                                ));
                                                let _ = response_f.send(Ok(()));
                                            }
                                            Err(e) => {
                                                let _ = file_handle_ret.send((
                                                    page_id,
                                                    po.get_file_number(),
                                                    None,
                                                ));
                                                let _ = response_f.send(Err(
                                                    FileExecutorError::FileOperationsError(e),
                                                ));
                                            }
                                        }
                                    });
                                }
                            }
                        }
                        RequestType::Read((r, response)) => {
                            match file_handle_cache.pop(&(page_id, r.get_file_number())) {
                                Some(maybe_file) => match maybe_file {
                                    Some(file) => {
                                        file_handle_cache.put((page_id, r.get_file_number()), None);
                                        let file_handle_ret = send_completed.clone();
                                        tokio::spawn(async move {
                                            let response_f = response;

                                            match FileOperations::read_chunk(file, &r).await {
                                                Ok((o, buffer)) => {
                                                    let _ = file_handle_ret.send((
                                                        page_id,
                                                        r.get_file_number(),
                                                        Some(o),
                                                    ));
                                                    let _ = response_f.send(Ok(buffer));
                                                }
                                                Err(e) => {
                                                    let _ = file_handle_ret.send((
                                                        page_id,
                                                        r.get_file_number(),
                                                        None,
                                                    ));
                                                    let _ = response_f.send(Err(
                                                        FileExecutorError::FileOperationsError(e),
                                                    ));
                                                }
                                            }
                                        });
                                    }
                                    None => {
                                        //Request in flight, skip for now, but have to reinsert into cache
                                        file_handle_cache.put((page_id, r.get_file_number()), None);
                                        new_request_queue
                                            .push_back((page_id, RequestType::Read((r, response))));
                                        continue;
                                    }
                                },
                                None => {
                                    file_handle_cache.put((page_id, r.get_file_number()), None);
                                    file_handles_open = file_handles_open.saturating_add(1);
                                    let data_dir = self.data_dir.clone();
                                    let file_handle_ret = send_completed.clone();
                                    tokio::spawn(async move {
                                        let response_f = response;

                                        let file = match FileOperations::open_path(
                                            &data_dir,
                                            &page_id,
                                            r.get_file_number(),
                                        )
                                        .await
                                        {
                                            Ok(o) => o,
                                            Err(e) => {
                                                let _ = file_handle_ret.send((
                                                    page_id,
                                                    r.get_file_number(),
                                                    None,
                                                ));
                                                let _ = response_f.send(Err(
                                                    FileExecutorError::FileOperationsError(e),
                                                ));
                                                return;
                                            }
                                        };

                                        match FileOperations::read_chunk(file, &r).await {
                                            Ok((o, maybe_buffer)) => {
                                                let _ = file_handle_ret.send((
                                                    page_id,
                                                    r.get_file_number(),
                                                    Some(o),
                                                ));
                                                let _ = response_f.send(Ok(maybe_buffer));
                                            }
                                            Err(e) => {
                                                let _ = file_handle_ret.send((
                                                    page_id,
                                                    r.get_file_number(),
                                                    None,
                                                ));
                                                let _ = response_f.send(Err(
                                                    FileExecutorError::FileOperationsError(e),
                                                ));
                                            }
                                        }
                                    });
                                }
                            }
                        }
                        RequestType::Update((u, buffer, response)) => {
                            match file_handle_cache.pop(&(page_id, u.get_file_number())) {
                                Some(maybe_file) => match maybe_file {
                                    Some(file) => {
                                        file_handle_cache.put((page_id, u.get_file_number()), None);
                                        let file_handle_ret = send_completed.clone();
                                        tokio::spawn(async move {
                                            let response_f = response;

                                            match FileOperations::update_chunk(file, &u, buffer)
                                                .await
                                            {
                                                Ok(o) => {
                                                    let _ = file_handle_ret.send((
                                                        page_id,
                                                        u.get_file_number(),
                                                        Some(o),
                                                    ));
                                                    let _ = response_f.send(Ok(()));
                                                }
                                                Err(e) => {
                                                    let _ = file_handle_ret.send((
                                                        page_id,
                                                        u.get_file_number(),
                                                        None,
                                                    ));
                                                    let _ = response_f.send(Err(
                                                        FileExecutorError::FileOperationsError(e),
                                                    ));
                                                }
                                            }
                                        });
                                    }
                                    None => {
                                        //Request in flight, skip for now, but have to reinsert into cache
                                        file_handle_cache.put((page_id, u.get_file_number()), None);
                                        new_request_queue.push_back((
                                            page_id,
                                            RequestType::Update((u, buffer, response)),
                                        ));
                                        continue;
                                    }
                                },
                                None => {
                                    file_handle_cache.put((page_id, u.get_file_number()), None);
                                    file_handles_open = file_handles_open.saturating_add(1);
                                    let data_dir = self.data_dir.clone();
                                    let file_handle_ret = send_completed.clone();
                                    tokio::spawn(async move {
                                        let response_f = response;

                                        let file = match FileOperations::open_path(
                                            &data_dir,
                                            &page_id,
                                            u.get_file_number(),
                                        )
                                        .await
                                        {
                                            Ok(o) => o,
                                            Err(e) => {
                                                let _ = file_handle_ret.send((
                                                    page_id,
                                                    u.get_file_number(),
                                                    None,
                                                ));
                                                let _ = response_f.send(Err(
                                                    FileExecutorError::FileOperationsError(e),
                                                ));
                                                return;
                                            }
                                        };

                                        match FileOperations::update_chunk(file, &u, buffer).await {
                                            Ok(o) => {
                                                let _ = file_handle_ret.send((
                                                    page_id,
                                                    u.get_file_number(),
                                                    Some(o),
                                                ));
                                                let _ = response_f.send(Ok(()));
                                            }
                                            Err(e) => {
                                                let _ = file_handle_ret.send((
                                                    page_id,
                                                    u.get_file_number(),
                                                    None,
                                                ));
                                                let _ = response_f.send(Err(
                                                    FileExecutorError::FileOperationsError(e),
                                                ));
                                            }
                                        }
                                    });
                                }
                            }
                        }
                        RequestType::GetOffset(response) => {
                            match self.get_next_po(&page_id).await {
                                Ok(po) => {
                                    let _ = response.send(Ok(po));
                                }
                                Err(e) => {
                                    let _ = response.send(Err(e));
                                    continue;
                                }
                            };
                        }
                    }
                }
                request_queue = new_request_queue;
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

    async fn get_next_po(&mut self, page_id: &PageId) -> Result<PageOffset, FileExecutorError> {
        //Find the resource key's latest offset so we can iterate on it for adds
        match self.resource_lookup.remove(page_id) {
            Some(po) => {
                self.resource_lookup.insert(*page_id, po.next());
                Ok(po)
            }
            None => {
                let po = self.find_next_offset(page_id).await?;
                self.resource_lookup.insert(*page_id, po.next());
                Ok(po)
            }
        }
    }

    async fn find_next_offset(&self, page_id: &PageId) -> Result<PageOffset, FileExecutorError> {
        let (path, count) =
            match Self::search_for_max_file(self.data_dir.as_path(), page_id).await? {
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
        let mut in_file_len = file_len;
        while in_file_len != 0 {
            //Move back to test a block
            in_file_len = file_len.saturating_sub(PAGE_SIZE as usize);

            let in_file_len_u64 = u64::try_from(in_file_len)?;
            file.seek(SeekFrom::Start(in_file_len_u64)).await?;

            //Each page should start with a non-zero number within the first 16 bytes, if it has data
            let mut buffer = BytesMut::with_capacity(EMPTY_BUFFER.len());
            file.read_buf(&mut buffer).await?;
            let buffer = buffer.freeze();
            if buffer == Bytes::from_static(&EMPTY_BUFFER) {
                //Okay we keep going
                continue;
            } else {
                //We can calucate our page offset now
                in_file_len = file_len.saturating_add(PAGE_SIZE as usize);
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
        data_dir: &Path,
        page_id: &PageId,
    ) -> Result<Option<(PathBuf, usize)>, FileExecutorError> {
        let sub_path = FileOperations::make_sub_path(data_dir, page_id).await?;
        let target_uuid = ResourceFormatter::format_uuid(&page_id.resource_key);
        let target_type = page_id.page_type.to_string();
        let target_filename = format!("{0}.{1}", target_uuid, target_type);

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

            if file_count >= max_file_count {
                max_file_count = file_count;
                max_file_path = Some(path);
            }
        }

        match max_file_path {
            Some(s) => Ok(Some((s, max_file_count))),
            None => Ok(None),
        }
    }

    fn format_os_string(input: &OsStr) -> String {
        input.to_ascii_lowercase().to_string_lossy().into_owned()
    }
}

#[derive(Debug, Error)]
pub enum FileExecutorError {
    #[error(transparent)]
    FileOperationsError(#[from] FileOperationsError),
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("Read {0} bytes instead of a page, the buffer has {1}")]
    IncompleteRead(usize, usize),
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
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use uuid::Uuid;

    use crate::{
        constants::PAGES_PER_FILE,
        engine::io::{page_formats::PageType, FileManager},
    };

    use super::*;

    fn get_test_page(fill: u8) -> Bytes {
        let mut test_page = BytesMut::with_capacity(PAGE_SIZE as usize);
        let free_space = vec![fill; PAGE_SIZE as usize];
        test_page.extend_from_slice(&free_space);
        test_page.freeze()
    }

    //Have a known bug, trying the simplest case
    #[tokio::test]
    async fn test_simple_startup() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path();

        //We're going to touch a single file to force it to think it has far more data than it does.
        //I don't normally write tests this way but I don't want to write GBs unnecessarily.
        let page_id = PageId {
            resource_key: Uuid::new_v4(),
            page_type: PageType::Data,
        };

        let mut test_file =
            FileOperations::open_path(tmp_dir, &page_id, PageOffset(0).get_file_number()).await?;

        let mut test_page = get_test_page(1);

        test_file.write_all(&mut test_page).await?;
        drop(test_file);

        //Now let's test add
        let fm = FileManager::new(tmp_dir.as_os_str().to_os_string())?;

        let test_page = get_test_page(2);
        let test_po = fm.get_offset(&page_id).await?;
        fm.add_page(&page_id, &test_po, test_page.clone()).await?;

        assert_eq!(test_po, PageOffset(2));

        let test_page_get = fm.get_page(&page_id, &test_po).await?.unwrap();

        assert_eq!(test_page, test_page_get);

        fm.shutdown().await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_rollover() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path();

        //We're going to touch a single file to force it to think it has far more data than it does.
        //I don't normally write tests this way but I don't want to write GBs unnecessarily.
        let page_id = PageId {
            resource_key: Uuid::new_v4(),
            page_type: PageType::Data,
        };

        let test_count: usize = 10;

        let mut test_file = FileOperations::open_path(
            tmp_dir,
            &page_id,
            PageOffset(PAGES_PER_FILE * test_count).get_file_number(),
        )
        .await?;

        let mut test_page = get_test_page(1);

        test_file.write_all(&mut test_page).await?;
        drop(test_file);

        //Now let's test add
        let fm = FileManager::new(tmp_dir.as_os_str().to_os_string())?;

        let test_page = get_test_page(2);
        let test_po = fm.get_offset(&page_id).await?;
        fm.add_page(&page_id, &test_po, test_page.clone()).await?;

        assert_eq!(test_po, PageOffset(PAGES_PER_FILE * test_count + 2));

        let test_page_get = fm.get_page(&page_id, &test_po).await?.unwrap();

        assert_eq!(test_page, test_page_get);

        fm.shutdown().await.unwrap();

        Ok(())
    }
}
