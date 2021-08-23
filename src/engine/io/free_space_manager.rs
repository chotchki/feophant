//! This struct provides a lookup service to tell row_manager where / if there is free space to
//! add new tuples. It is designed to be extremely space efficent since it only uses 1 bit per
//! page to say the space is availible. This means each page here can cover 134MB of free space.

use super::{
    page_formats::{PageId, PageOffset, PageType},
    LockCacheManager, LockCacheManagerError,
};
use crate::constants::PAGE_SIZE;
use bytes::{Buf, BytesMut};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct FreeSpaceManager {
    lock_cache_manager: LockCacheManager,
}

impl FreeSpaceManager {
    pub fn new(lock_cache_manager: LockCacheManager) -> FreeSpaceManager {
        FreeSpaceManager { lock_cache_manager }
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
            let page_handle = self.lock_cache_manager.get_page(free_id, offset).await?;
            match page_handle.as_ref() {
                Some(s) => {
                    let mut page_frozen = s.clone().freeze();
                    match Self::find_first_free_page_in_page(&mut page_frozen) {
                        Some(s) => {
                            let full_offset = PageOffset(s)
                                + offset * PageOffset(PAGE_SIZE as usize) * PageOffset(8);
                            return Ok(full_offset);
                        }
                        None => {
                            offset += PageOffset(1);
                            continue;
                        }
                    }
                }
                None => {
                    drop(page_handle);
                    //Get the next offset, BUT since there could be a gap, we're going to blindly write all free
                    //and loop to get it again.
                    let next_po = self.lock_cache_manager.get_offset(free_id).await?;
                    let mut new_page_handle = self
                        .lock_cache_manager
                        .get_page_for_update(free_id, next_po)
                        .await?;

                    let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
                    let new_page = vec![FreeStat::Free as u8; PAGE_SIZE as usize];
                    buffer.extend_from_slice(&new_page);
                    new_page_handle.replace(buffer);

                    self.lock_cache_manager
                        .add_page(free_id, next_po, new_page_handle)
                        .await?;

                    continue; //No increment since we could be at a gap
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
        let free_id = PageId {
            resource_key: page_id.resource_key,
            page_type: PageType::FreeSpaceMap,
        };
        let (po, offset) = po.get_bitmask_offset();
        let mut page_handle = self
            .lock_cache_manager
            .get_page_for_update(free_id, po)
            .await?;
        let mut page = page_handle
            .as_mut()
            .ok_or(FreeSpaceManagerError::PageDoesNotExist(page_id))?;
        Self::set_status_inside_page(&mut page, offset, status);

        Ok(self
            .lock_cache_manager
            .update_page(free_id, po, page_handle)
            .await?)
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
    //TODO Decide if I end up keeping this or maybe move it to the unit test section?
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
    LockCacheManagerError(#[from] LockCacheManagerError),
    #[error("Page Offset {0} doesn't exist")]
    PageDoesNotExist(PageId),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::BufMut;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::engine::io::FileManager;

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

    #[tokio::test]
    async fn test_get_next() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let lm = LockCacheManager::new(fm);
        let fsm = FreeSpaceManager::new(lm);

        let page_id = PageId {
            resource_key: Uuid::new_v4(),
            page_type: PageType::Data,
        };

        let first_free = fsm.get_next_free_page(page_id).await?;
        assert_eq!(first_free, PageOffset(0));

        fsm.mark_page(page_id, first_free, FreeStat::InUse).await?;

        let second_free = fsm.get_next_free_page(page_id).await?;
        assert_eq!(second_free, PageOffset(1));

        Ok(())
    }
}
