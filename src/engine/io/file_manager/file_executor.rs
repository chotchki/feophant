/// Inner type that implements the actual I/O operations so the outter type can
/// handle queue management.
use super::request_type::RequestType;
use crate::constants::PAGE_SIZE;
use crate::engine::io::file_manager::ResourceFormatter;
use crate::engine::io::page_formats::PageOffset;
use bytes::{Bytes, BytesMut};
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
/// Empty page buffer
const EMPTY_BUFFER: [u8; 16] = [0u8; 16];

#[derive(Debug)]
pub struct FileExecutor {
    data_dir: PathBuf,
    receive_queue: UnboundedReceiver<(Uuid, RequestType)>,
    receive_shutdown: UnboundedReceiver<Sender<()>>,
}

impl FileExecutor {
    pub fn new(
        raw_path: OsString,
        receive_queue: UnboundedReceiver<(Uuid, RequestType)>,
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
                    if let Some((resource_key, req_type)) = maybe_recv {
                        self
                            .handle_request(&mut resource_lookup, &resource_key, req_type)
                            .await;
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

    //TODO All these match blocks suck but I don't see a way around them.
    //I'm hoping clippy yells at me once I get it compiling
    async fn handle_request(
        &self,
        resource_lookup: &mut HashMap<Uuid, PageOffset>,
        resource_key: &Uuid,
        req_type: RequestType,
    ) -> Result<(), FileExecutorError> {
        //Find the resource key's latest offset so we can iterate on it for adds
        let next_po = match resource_lookup.get(resource_key) {
            Some(po) => *po,
            None => {
                let po = self.find_next_offset(resource_key).await?;
                println!("found {0}", po);
                resource_lookup.insert(*resource_key, po);
                po
            }
        };

        match req_type {
            RequestType::Add((buffer, response)) => {
                let mut buffer = buffer.clone();

                let new_po = next_po.next();
                println!("Increment {0} {1} {2}", next_po, new_po, resource_key);
                resource_lookup.insert(*resource_key, new_po);
                let file_path =
                    match Self::make_file_path(self.data_dir.as_path(), resource_key, &next_po)
                        .await
                    {
                        Ok(o) => o,
                        Err(e) => {
                            response.send(Err(e));
                            return Ok(());
                        }
                    };

                //Need a file handle
                let mut file = match OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(file_path)
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                };

                match file
                    .set_len(u64::try_from(next_po.get_file_chunk_size())?)
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                };

                match file
                    .seek(SeekFrom::Start(u64::try_from(next_po.get_file_seek())?))
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                };

                match file.write_all_buf(&mut buffer).await {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                };

                match file.sync_all().await {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                }

                println!("Add {0}", next_po);
                response.send(Ok(next_po));

                Ok(())
            }
            RequestType::Read((po, response)) => {
                let file_path =
                    match Self::make_file_path(self.data_dir.as_path(), resource_key, &po).await {
                        Ok(o) => o,
                        Err(e) => {
                            response.send(Err(e));
                            return Ok(());
                        }
                    };

                let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);

                let mut file = match File::open(file_path).await {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Ok(None));
                        return Ok(());
                    }
                };

                let file_meta = match file.metadata().await {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                };

                let file_len = file_meta.len();
                if u64::try_from(po.get_file_chunk_size())? > file_len {
                    response.send(Ok(None));
                    return Ok(());
                }

                match file
                    .seek(SeekFrom::Start(u64::try_from(po.get_file_seek())?))
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Ok(None));
                        return Ok(());
                    }
                };

                while buffer.len() != PAGE_SIZE as usize {
                    let readamt = match file.read_buf(&mut buffer).await {
                        Ok(o) => o,
                        Err(e) => {
                            response.send(Err(FileExecutorError::IOError(e)));
                            return Ok(());
                        }
                    };
                    if readamt == 0 {
                        response.send(Err(FileExecutorError::IncompleteRead(
                            readamt,
                            buffer.len(),
                        )));
                        return Ok(());
                    }
                }

                response.send(Ok(Some(buffer.freeze())));
                Ok(())
            }
            RequestType::Update((po, buffer, response)) => {
                let mut buffer = buffer.clone();
                let file_path =
                    match Self::make_file_path(self.data_dir.as_path(), resource_key, &po).await {
                        Ok(o) => o,
                        Err(e) => {
                            response.send(Err(e));
                            return Ok(());
                        }
                    };

                //Need a file handle
                let mut file = match OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(file_path)
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                };

                match file
                    .seek(SeekFrom::Start(u64::try_from(po.get_file_seek())?))
                    .await
                {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                };

                match file.write_all_buf(&mut buffer).await {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                };

                match file.sync_all().await {
                    Ok(o) => o,
                    Err(e) => {
                        response.send(Err(FileExecutorError::IOError(e)));
                        return Ok(());
                    }
                }

                response.send(Ok(()));

                Ok(())
            }
        }
    }

    async fn make_file_path(
        data_dir: &Path,
        resource_key: &Uuid,
        offset: &PageOffset,
    ) -> Result<PathBuf, FileExecutorError> {
        let mut sub_path = Self::make_sub_path(data_dir, resource_key).await?;
        let target_filename = ResourceFormatter::format_uuid(resource_key);
        let target_extension = offset.get_file_number();
        let name = format!("{0}.{1}", target_filename, target_extension);
        sub_path.push(name);
        Ok(sub_path)
    }

    async fn find_next_offset(&self, resource_key: &Uuid) -> Result<PageOffset, FileExecutorError> {
        let (path, count) =
            match Self::search_for_max_file(self.data_dir.as_path(), resource_key).await? {
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
        resource_key: &Uuid,
    ) -> Result<Option<(PathBuf, usize)>, FileExecutorError> {
        let sub_path = Self::make_sub_path(data_dir, resource_key).await?;
        let target_filename = ResourceFormatter::format_uuid(resource_key);

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

    //Makes the prefix folder so we don't fill up folders. Will consider more nesting eventually
    async fn make_sub_path(
        data_dir: &Path,
        resource_key: &Uuid,
    ) -> Result<PathBuf, FileExecutorError> {
        let subfolder = ResourceFormatter::get_uuid_prefix(resource_key);

        let mut path = PathBuf::new();
        path.push(data_dir);
        path.push(subfolder);

        fs::create_dir_all(path.as_path()).await?;
        Ok(path)
    }

    fn format_os_string(input: &OsStr) -> String {
        input.to_ascii_lowercase().to_string_lossy().into_owned()
    }
}

#[derive(Debug, Error)]
pub enum FileExecutorError {
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
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::time::timeout;

    use crate::{constants::PAGES_PER_FILE, engine::io::FileManager};

    use super::*;

    fn get_test_page(fill: u8) -> Bytes {
        let mut test_page = BytesMut::with_capacity(PAGE_SIZE as usize);
        let free_space = vec![fill; PAGE_SIZE as usize];
        test_page.extend_from_slice(&free_space);
        test_page.freeze()
    }

    #[tokio::test]
    async fn test_search_for_max_file() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;

        let test_uuid = Uuid::new_v4();
        let subpath = FileExecutor::make_sub_path(tmp.path(), &test_uuid).await?;

        let test_path =
            FileExecutor::make_file_path(tmp.path(), &test_uuid, &PageOffset(1)).await?;
        File::create(test_path.as_path()).await?;

        let path = FileExecutor::search_for_max_file(tmp.path(), &test_uuid).await?;
        assert_eq!(path, Some((test_path, 0)));

        let test_path =
            FileExecutor::make_file_path(tmp.path(), &test_uuid, &PageOffset(PAGES_PER_FILE * 100))
                .await?;
        File::create(test_path.as_path()).await?;

        let mut test1 = subpath.clone();
        test1.push("test.file");
        File::create(test1.as_path()).await?;

        let mut test2 = subpath.clone();
        test2.push(".file");
        File::create(test2.as_path()).await?;

        let path = FileExecutor::search_for_max_file(tmp.path(), &test_uuid).await?;
        assert_eq!(path, Some((test_path, 100)));

        Ok(())
    }

    #[tokio::test]
    async fn test_make_sub_path() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;

        let test_uuid = Uuid::new_v4();

        //Must be able to repeatedly make the sub_path
        FileExecutor::make_sub_path(tmp.path(), &test_uuid).await?;
        FileExecutor::make_sub_path(tmp.path(), &test_uuid).await?;

        Ok(())
    }

    //Have a known bug, trying the simplest case
    #[tokio::test]
    async fn test_simple_startup() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path();

        //We're going to touch a single file to force it to think it has far more data than it does.
        //I don't normally write tests this way but I don't want to write GBs unnecessarily.
        let test_uuid = Uuid::new_v4();

        let test_path = FileExecutor::make_file_path(tmp_dir, &test_uuid, &PageOffset(0)).await?;

        let mut test_page = get_test_page(1);
        println!("{:?}", test_path);

        let mut test_file = File::create(test_path).await?;
        test_file.write_all(&mut test_page).await?;
        drop(test_file);

        //Now let's test add
        let fm = FileManager::new(tmp_dir.as_os_str().to_os_string())?;

        let test_page = get_test_page(2);
        let test_page_num = fm.add_page(&test_uuid, test_page.clone()).await?;

        assert_eq!(test_page_num, PageOffset(2));
        println!("{:?}", test_page_num);

        let test_page_get = fm.get_page(&test_uuid, &test_page_num).await?.unwrap();

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
        let test_uuid = Uuid::new_v4();
        let test_count: usize = 10;

        let test_path = FileExecutor::make_file_path(
            tmp_dir,
            &test_uuid,
            &PageOffset(PAGES_PER_FILE * test_count),
        )
        .await?;

        let mut test_page = get_test_page(1);
        println!("{:?}", test_path);

        let mut test_file = File::create(test_path).await?;
        test_file.write_all(&mut test_page).await?;
        drop(test_file);

        //Now let's test add
        let fm = FileManager::new(tmp_dir.as_os_str().to_os_string())?;

        let test_page = get_test_page(2);
        let test_page_num = timeout(
            Duration::new(10, 0),
            fm.add_page(&test_uuid, test_page.clone()),
        )
        .await??;

        assert_eq!(test_page_num, PageOffset(PAGES_PER_FILE * test_count + 2));
        println!("{:?}", test_page_num);

        let test_page_get = timeout(
            Duration::new(10, 0),
            fm.get_page(&test_uuid, &test_page_num),
        )
        .await??
        .unwrap();

        assert_eq!(test_page, test_page_get);

        fm.shutdown().await.unwrap();

        Ok(())
    }
}
