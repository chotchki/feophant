//! Helper functions to aid in chaining async calls

use bytes::{Bytes, BytesMut};
use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::path::PathBuf;
use std::sync::Arc;
use std::{io::SeekFrom, path::Path};
use thiserror::Error;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncSeekExt,
};

use crate::constants::PAGE_SIZE;
use crate::engine::io::block_layer::ResourceFormatter;
use crate::engine::io::page_formats::{PageId, PageOffset};
pub struct FileOperations {}

impl FileOperations {
    pub async fn open_path(
        data_dir: &Path,
        page_id: &PageId,
        file_number: usize,
    ) -> Result<File, FileOperationsError> {
        let mut path = Self::make_sub_path(data_dir, page_id).await?;
        let file_stem = ResourceFormatter::format_uuid(&page_id.resource_key);
        let file_type = page_id.page_type.to_string();
        let filename = format!("{0}.{1}.{2}", file_stem, file_type, file_number);

        path.push(filename);

        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?)
    }

    /// Note the File Handle AND PageOffset should point to where the add should occur
    /// If the file is larger than requested nothing is done.
    pub async fn add_chunk(
        file: &mut File,
        page_offset: &PageOffset,
        buffer: Bytes,
    ) -> Result<(), FileOperationsError> {
        let metadata = file.metadata().await?;
        let chunk_size_u64 = u64::try_from(page_offset.get_file_chunk_size())?;

        if metadata.len() < chunk_size_u64 {
            file.set_len(chunk_size_u64).await?;
        }

        Self::update_chunk(file, page_offset, buffer).await?;
        Ok(())
    }

    //Makes the prefix folder so we don't fill up folders. Will consider more nesting eventually
    pub async fn make_sub_path(
        data_dir: &Path,
        page_id: &PageId,
    ) -> Result<PathBuf, FileOperationsError> {
        let subfolder = ResourceFormatter::get_uuid_prefix(&page_id.resource_key);

        let mut path = PathBuf::new();
        path.push(data_dir);
        path.push(subfolder);

        fs::create_dir_all(path.as_path()).await?;
        Ok(path)
    }

    pub async fn read_chunk(
        file: &mut File,
        page_offset: &PageOffset,
    ) -> Result<Bytes, FileOperationsError> {
        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);

        let file_meta = file.metadata().await?;

        let file_len = file_meta.len();
        if u64::try_from(page_offset.get_file_chunk_size())? > file_len {
            return Err(FileOperationsError::FileTooSmall(
                page_offset.get_file_chunk_size(),
                file_len,
            ));
        }

        file.seek(SeekFrom::Start(u64::try_from(page_offset.get_file_seek())?))
            .await?;

        while buffer.len() != PAGE_SIZE as usize {
            let readamt = file.read_buf(&mut buffer).await?;
            if readamt == 0 {
                return Err(FileOperationsError::IncompleteRead(readamt, buffer.len()));
            }
        }

        Ok(buffer.freeze())
    }

    pub async fn update_chunk(
        file: &mut File,
        page_offset: &PageOffset,
        mut buffer: Bytes,
    ) -> Result<(), FileOperationsError> {
        file.seek(SeekFrom::Start(u64::try_from(page_offset.get_file_seek())?))
            .await?;

        file.write_all_buf(&mut buffer).await?;

        //file.sync_all().await?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum FileOperationsError {
    #[error(transparent)]
    FileOperationsError(#[from] Arc<FileOperationsError>),
    #[error("Read {0} bytes instead of a page, the buffer has {1}")]
    IncompleteRead(usize, usize),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),
    #[error("File too small for requested read {0}, size is {1}")]
    FileTooSmall(usize, u64),
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::engine::io::page_formats::PageType;

    use super::*;

    #[tokio::test]
    async fn test_make_sub_path() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;

        let page_id = PageId {
            resource_key: Uuid::new_v4(),
            page_type: PageType::Data,
        };

        //Must be able to repeatedly make the sub_path
        FileOperations::make_sub_path(tmp.path(), &page_id).await?;
        FileOperations::make_sub_path(tmp.path(), &page_id).await?;

        Ok(())
    }
}
