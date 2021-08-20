//! This struct provides a lookup service to tell row_manager where / if there is free space to
//! add new tuples. It is designed to be extremely space efficent since it only uses 1 bit per
//! page to say the space is availible. This means each page here can cover 134MB of free space.

use crate::constants::PAGE_SIZE;

use super::{
    file_manager,
    page_formats::{PageId, PageOffset, PageType},
    FileManager, FileManagerError, LockManager,
};
use bytes::{Buf, Bytes, BytesMut};
use lru::LruCache;
use thiserror::Error;

const MAX_FREESPACE_COUNT: usize = 32;

pub struct FreeSpaceManager {
    file_manager: FileManager,
    freespace_cache: LruCache<(PageId, PageOffset), Bytes>,
    lock_manager: LockManager,
}

impl FreeSpaceManager {
    pub fn new(file_manager: FileManager, lock_manager: LockManager) -> FreeSpaceManager {
        FreeSpaceManager {
            file_manager,
            freespace_cache: LruCache::new(MAX_FREESPACE_COUNT),
            lock_manager,
        }
    }

    pub async fn get_next_free_page(
        &self,
        page_id: PageId,
    ) -> Result<PageOffset, FreeSpaceManagerError> {
        let mut offset = PageOffset(0);
        let free_id = PageId {
            resource_key: page_id.resource_key,
            page_type: PageType::FreeSpaceMap,
        };
        loop {
            let lock = self.lock_manager.get_lock(&free_id, &offset).await;
            let lock_read = lock.read();
            match self.file_manager.get_page(&free_id, &offset).await? {
                Some(mut s) => match Self::find_first_free_page_in_page(&mut s) {
                    Some(s) => {
                        let full_offset =
                            PageOffset(s) + offset * PageOffset(PAGE_SIZE as usize) * PageOffset(8);
                        return Ok(full_offset);
                    }
                    None => {
                        offset += PageOffset(1);
                        continue;
                    }
                },
                None => {
                    let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
                    let new_page = vec![0; PAGE_SIZE as usize];
                    buffer.extend_from_slice(&new_page);

                    //TODO: So I have the read lock now for the add, need figure out how do this sanely in code
                    let new_page = self
                        .file_manager
                        .add_page(&page_id, buffer.freeze())
                        .await?;
                    let new_offset = new_page * PageOffset(PAGE_SIZE as usize) * PageOffset(8);
                    return Ok(new_offset);
                }
            }
        }
    }

    pub async fn mark_page(
        &self,
        page_id: PageId,
        po: PageOffset,
        status: FreeStat,
    ) -> Result<(), FreeSpaceManagerError> {
        let (po, offset) = po.get_bitmask_offset();
        match self.file_manager.get_page(&page_id, &po).await? {
            Some(mut page) => {
                Self::set_status_inside_page(&mut page, offset, status);
                self.file_manager
                    .update_page(&page_id, page.freeze(), &po)
                    .await?;
                Ok(())
            }
            None => Err(FreeSpaceManagerError::PageDoesNotExist(page_id)),
        }
    }

    fn find_first_free_page_in_page(buffer: &mut impl Buf) -> Option<usize> {
        let mut i = 0;
        while buffer.has_remaining() {
            let mut val = buffer.get_u8();
            if val == 0xFF {
                i += 1;
                continue;
            }
            for j in 0..8 {
                if val & 0x1 == 0x0 {
                    return Some(i * 8 + j);
                }
                val >>= 1;
            }
            i += 1;
        }
        None
    }

    /// Gets the status of a field inside a page, you MUST pass an offset
    /// that fits in the buffer.
    fn get_status_inside_page(buffer: &BytesMut, offset: usize) -> FreeStat {
        let offset_index = offset / 8;
        let offset_subindex = offset % 8;

        let offset_value = buffer[offset_index];
        let bit_value = (offset_value >> offset_subindex) & 0x1;
        if bit_value == 0 {
            FreeStat::Free
        } else {
            FreeStat::InUse
        }
    }

    /// Sets the status of a field inside a page, you MUST pass an offset
    /// that fits in the buffer.
    fn set_status_inside_page(buffer: &mut BytesMut, offset: usize, status: FreeStat) {
        let offset_index = offset / 8;
        let offset_subindex = offset % 8;

        let current_value = buffer[offset_index];
        let mut pre_load = 0x1 << offset_subindex;
        let new_value;
        match status {
            FreeStat::Free => {
                pre_load = !pre_load;
                new_value = current_value & pre_load;
            }
            FreeStat::InUse => {
                new_value = current_value | pre_load;
            }
        }

        buffer[offset_index] = new_value;
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FreeStat {
    Free = 0,
    InUse = 1,
}

#[derive(Debug, Error)]
pub enum FreeSpaceManagerError {
    #[error(transparent)]
    FileManagerError(#[from] FileManagerError),
    #[error("Page Offset {0} doesn't exist")]
    PageDoesNotExist(PageId),
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;

    ///This test works by toggling each bit repeatedly and making sure it gives the correct result each time.
    #[test]
    fn test_get_and_set() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = BytesMut::with_capacity(2);
        test.put_u16(0x0);

        for i in 0..test.capacity() * 8 {
            assert_eq!(
                FreeSpaceManager::get_status_inside_page(&test, i),
                FreeStat::Free
            );
            FreeSpaceManager::set_status_inside_page(&mut test, i, FreeStat::InUse);
            assert_eq!(
                FreeSpaceManager::get_status_inside_page(&test, i),
                FreeStat::InUse
            );
            FreeSpaceManager::set_status_inside_page(&mut test, i, FreeStat::Free);
            assert_eq!(
                FreeSpaceManager::get_status_inside_page(&test, i),
                FreeStat::Free
            );
        }

        Ok(())
    }

    #[test]
    fn test_find_and_fill_pages() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = BytesMut::with_capacity(2);
        test.put_u8(0x0);
        test.put_u8(0x0);

        for i in 0..test.len() * 8 {
            let free_page = FreeSpaceManager::find_first_free_page_in_page(&mut test.clone());
            assert_eq!(free_page, Some(i));

            FreeSpaceManager::set_status_inside_page(&mut test, i, FreeStat::InUse);
        }
        assert_eq!(
            FreeSpaceManager::find_first_free_page_in_page(&mut test.clone()),
            None
        );

        Ok(())
    }
}