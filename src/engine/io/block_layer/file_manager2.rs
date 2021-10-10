use bytes::{Bytes, BytesMut};
use moka::future::Cache;
use std::convert::TryFrom;
use std::io::SeekFrom;
use std::num::TryFromIntError;
use std::ops::DerefMut;
use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
};
use thiserror::Error;
use tokio::fs::{read_dir, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard};

use crate::constants::{MAX_FILE_HANDLE_COUNT, MAX_PAGE_CACHE};
use crate::engine::io::block_layer::ResourceFormatter;
use crate::{
    constants::PAGE_SIZE,
    engine::io::page_formats::{PageId, PageOffset},
};

use super::file_operations::{FileOperations, FileOperationsError};
use super::lock_manager::LockManager;

/// Empty page buffer
const EMPTY_BUFFER: [u8; 16] = [0u8; 16];

/// Attempt to move away from channels for the FileManager Service.
///
/// This code has ended up tremendously simpler than the prior version!
pub struct FileManager2 {
    data_dir: PathBuf,
    file_handles: Cache<(PageId, usize), Arc<Mutex<File>>>,
    file_offsets: Cache<PageId, Arc<AtomicUsize>>,
    lock_manager: LockManager,
    page_cache: Cache<(PageId, PageOffset), Bytes>,
}

impl FileManager2 {
    pub fn new(raw_path: OsString) -> Result<FileManager2, FileManager2Error> {
        let data_dir = Path::new(&raw_path).to_path_buf();

        if !data_dir.is_dir() {
            return Err(FileManager2Error::NeedDirectory(
                data_dir.to_string_lossy().to_string(),
            ));
        }

        Ok(FileManager2 {
            data_dir,
            file_handles: Cache::new(MAX_FILE_HANDLE_COUNT),
            file_offsets: Cache::new(10000),
            lock_manager: LockManager::new(),
            page_cache: Cache::new(MAX_PAGE_CACHE),
        })
    }

    pub async fn get_next_offset(
        &self,
        page_id: &PageId,
    ) -> Result<(PageOffset, OwnedRwLockWriteGuard<(PageId, PageOffset)>), FileManager2Error> {
        let data_dir = self.data_dir.clone();
        let page_id = *page_id;
        let current_offset = self
            .file_offsets
            .get_or_try_insert_with(page_id, async move {
                let po = Self::find_next_offset(&data_dir, &page_id).await?;
                let start_atomic: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(po.0));
                Ok::<Arc<AtomicUsize>, FileManager2Error>(start_atomic)
            })
            .await?;
        let new_offset = current_offset.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let new_po = PageOffset(new_offset);

        let write_lock = self.lock_manager.write(page_id, new_po).await;
        Ok((new_po, write_lock))
    }

    pub async fn add_page(
        &self,
        guard: OwnedRwLockWriteGuard<(PageId, PageOffset)>,
        page: Bytes,
    ) -> Result<(), FileManager2Error> {
        let data_dir = self.data_dir.clone();
        let page_id = guard.0;
        let file_number = guard.1.get_file_number();
        let file_handle = self
            .file_handles
            .get_or_try_insert_with((page_id, file_number), async move {
                let handle = FileOperations::open_path(&data_dir, &page_id, file_number).await?;
                Ok::<Arc<Mutex<File>>, FileManager2Error>(Arc::new(Mutex::const_new(handle)))
            })
            .await?;
        let mut file = file_handle.lock().await;

        self.page_cache
            .insert((page_id, guard.1), page.clone())
            .await;
        let _ = FileOperations::add_chunk(file.deref_mut(), &guard.1, page).await?;
        Ok(())
    }

    pub async fn get_page(
        &self,
        page_id: &PageId,
        offset: &PageOffset,
    ) -> Result<(Bytes, OwnedRwLockReadGuard<(PageId, PageOffset)>), FileManager2Error> {
        let read_lock = self.lock_manager.read(*page_id, *offset).await;

        let data_dir = self.data_dir.clone();
        let page_id = *page_id;
        let offset = *offset;
        let file_number = offset.get_file_number();
        let file_handles = self.file_handles.clone();

        let chunk = self
            .page_cache
            .get_or_try_insert_with((page_id, offset), async move {
                let file_handle = file_handles
                    .get_or_try_insert_with((page_id, file_number), async move {
                        let handle =
                            FileOperations::open_path(&data_dir, &page_id, file_number).await?;
                        Ok::<Arc<Mutex<File>>, FileManager2Error>(Arc::new(Mutex::const_new(
                            handle,
                        )))
                    })
                    .await?;
                let mut file = file_handle.lock().await;

                let chunk = FileOperations::read_chunk(file.deref_mut(), &offset).await?;
                Ok::<Bytes, FileManager2Error>(chunk)
            })
            .await?;

        Ok((chunk, read_lock))
    }

    pub async fn get_page_for_update(
        &self,
        page_id: &PageId,
        offset: &PageOffset,
    ) -> Result<(Bytes, OwnedRwLockWriteGuard<(PageId, PageOffset)>), FileManager2Error> {
        let write_lock = self.lock_manager.write(*page_id, *offset).await;

        let data_dir = self.data_dir.clone();
        let page_id = *page_id;
        let offset = *offset;
        let file_number = offset.get_file_number();
        let file_handles = self.file_handles.clone();

        let chunk = self
            .page_cache
            .get_or_try_insert_with((page_id, offset), async move {
                let file_handle = file_handles
                    .get_or_try_insert_with((page_id, file_number), async move {
                        let handle =
                            FileOperations::open_path(&data_dir, &page_id, file_number).await?;
                        Ok::<Arc<Mutex<File>>, FileManager2Error>(Arc::new(Mutex::const_new(
                            handle,
                        )))
                    })
                    .await?;
                let mut file = file_handle.lock().await;

                let chunk = FileOperations::read_chunk(file.deref_mut(), &offset).await?;
                Ok::<Bytes, FileManager2Error>(chunk)
            })
            .await?;

        Ok((chunk, write_lock))
    }

    pub async fn update_page(
        &self,
        guard: OwnedRwLockWriteGuard<(PageId, PageOffset)>,
        page: Bytes,
    ) -> Result<(), FileManager2Error> {
        let data_dir = self.data_dir.clone();
        let page_id = guard.0;
        let file_number = guard.1.get_file_number();
        let file_handle = self
            .file_handles
            .get_or_try_insert_with((page_id, file_number), async move {
                let handle = FileOperations::open_path(&data_dir, &page_id, file_number).await?;
                Ok::<Arc<Mutex<File>>, FileManager2Error>(Arc::new(Mutex::const_new(handle)))
            })
            .await?;
        let mut file = file_handle.lock().await;

        self.page_cache
            .insert((page_id, guard.1), page.clone())
            .await;
        let _ = FileOperations::update_chunk(file.deref_mut(), &guard.1, page).await?;
        Ok(())
    }

    async fn find_next_offset(
        data_dir: &Path,
        page_id: &PageId,
    ) -> Result<PageOffset, FileManager2Error> {
        let (path, count) = match Self::search_for_max_file(data_dir, page_id).await? {
            Some((p, c)) => (p, c),
            None => {
                return Ok(PageOffset(0));
            }
        };

        let mut file = File::open(path.clone()).await?;
        let file_meta = file.metadata().await?;
        let file_len = file_meta.len();

        if file_len % PAGE_SIZE as u64 != 0 {
            return Err(FileManager2Error::IncorrectPageSize(file_len, path));
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
    ) -> Result<Option<(PathBuf, usize)>, FileManager2Error> {
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
pub enum FileManager2Error {
    #[error(transparent)]
    FileManager2Error(#[from] Arc<FileManager2Error>),
    #[error(transparent)]
    FileOperationsError(#[from] FileOperationsError),
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
    use bytes::{Bytes, BytesMut};
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

        let fm = FileManager2::new(tmp_dir.as_os_str().to_os_string())?;

        let page_id = PageId {
            resource_key: Uuid::new_v4(),
            page_type: PageType::Data,
        };

        let test_page = get_test_page(1);
        let (test_po, test_guard) = fm.get_next_offset(&page_id).await?;
        fm.add_page(test_guard, test_page.clone()).await?;

        assert_eq!(test_po, PageOffset(0));

        let (test_page_get, test_guard) = fm.get_page_for_update(&page_id, &test_po).await?;
        assert_eq!(test_page, test_page_get);

        let test_page2 = get_test_page(2);
        fm.update_page(test_guard, test_page2.clone()).await?;

        let (test_page_get2, _test_page_guard2) = fm.get_page(&page_id, &test_po).await?;
        assert_eq!(test_page2, test_page_get2);

        let fm2 = FileManager2::new(tmp_dir.as_os_str().to_os_string())?;
        let test_page3 = get_test_page(3);
        let (test_po3, test_guard3) = fm2.get_next_offset(&page_id).await?;
        fm2.add_page(test_guard3, test_page3.clone()).await?;
        assert!(test_po3 > test_po);

        let (test_page_get2, _test_guard2) = fm2.get_page(&page_id, &test_po).await?;
        assert_eq!(test_page2, test_page_get2);

        Ok(())
    }
}
