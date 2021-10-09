//! This struct provides a lookup service to tell row_manager where / if there is free space to
//! add new tuples. It is designed to be extremely space efficent since it only uses 1 bit per
//! page to say the space is availible. This means each page here can cover 134MB of free space.

use super::{
    super::page_formats::{PageId, PageOffset, PageType},
    file_manager2::{FileManager2, FileManager2Error},
};
use crate::constants::PAGE_SIZE;
use bytes::{Buf, BytesMut};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone)]
pub struct FreeSpaceManager {
    file_manager: Arc<FileManager2>,
}

impl FreeSpaceManager {
    pub fn new(file_manager: Arc<FileManager2>) -> FreeSpaceManager {
        FreeSpaceManager { file_manager }
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
            match self.file_manager.get_page(&free_id, &offset).await {
                Ok((s, _read_guard)) => {
                    let mut page_frozen = s.clone();
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
                Err(_) => {
                    // Create the next offset page and loop again as a test.
                    // Note: due to possible timing issues the next page might not be sequentially
                    // next so we will check again on the next loop

                    let (_, next_guard) = self.file_manager.get_next_offset(&free_id).await?;

                    let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
                    let new_page = vec![FreeStat::Free as u8; PAGE_SIZE as usize];
                    buffer.extend_from_slice(&new_page);

                    self.file_manager
                        .add_page(next_guard, buffer.freeze())
                        .await?;
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
        let (fs_po, inner_offset) = po.get_bitmask_offset();

        let (page, page_guard) = self
            .file_manager
            .get_page_for_update(&free_id, &fs_po)
            .await?;

        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        buffer.extend_from_slice(&page);

        Self::set_status_inside_page(&mut buffer, inner_offset, status);

        self.file_manager
            .update_page(page_guard, buffer.freeze())
            .await?;

        Ok(())
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
            FreeStat::Full => {
                new_value = current_value | pre_load;
            }
        }

        buffer[offset_index] = new_value;
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FreeStat {
    Free = 0,
    Full = 1,
}

#[derive(Debug, Error)]
pub enum FreeSpaceManagerError {
    #[error(transparent)]
    FileManager2Error(#[from] FileManager2Error),
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use std::sync::Arc;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;

    /// Gets the status of a field inside a page, you MUST pass an offset
    /// that fits in the buffer.
    //This was in the implementation, I just only needed it for unit tests
    fn get_status_inside_page(buffer: &BytesMut, offset: usize) -> FreeStat {
        let offset_index = offset / 8;
        let offset_subindex = offset % 8;

        let offset_value = buffer[offset_index];
        let bit_value = (offset_value >> offset_subindex) & 0x1;
        if bit_value == 0 {
            FreeStat::Free
        } else {
            FreeStat::Full
        }
    }

    ///This test works by toggling each bit repeatedly and making sure it gives the correct result each time.
    #[test]
    fn test_get_and_set() -> Result<(), Box<dyn std::error::Error>> {
        let mut test = BytesMut::with_capacity(2);
        test.put_u16(0x0);

        for i in 0..test.len() * 8 {
            assert_eq!(get_status_inside_page(&test, i), FreeStat::Free);
            FreeSpaceManager::set_status_inside_page(&mut test, i, FreeStat::Full);
            assert_eq!(get_status_inside_page(&test, i), FreeStat::Full);
            FreeSpaceManager::set_status_inside_page(&mut test, i, FreeStat::Free);
            assert_eq!(get_status_inside_page(&test, i), FreeStat::Free);
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

            FreeSpaceManager::set_status_inside_page(&mut test, i, FreeStat::Full);
        }
        assert_eq!(
            FreeSpaceManager::find_first_free_page_in_page(&mut test),
            None
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_next() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager2::new(tmp_dir)?);
        let fsm = FreeSpaceManager::new(fm);

        let page_id = PageId {
            resource_key: Uuid::new_v4(),
            page_type: PageType::Data,
        };

        let first_free = fsm.get_next_free_page(page_id).await?;
        assert_eq!(first_free, PageOffset(0));

        fsm.mark_page(page_id, first_free, FreeStat::Full).await?;

        let second_free = fsm.get_next_free_page(page_id).await?;
        assert_eq!(second_free, PageOffset(1));

        Ok(())
    }
}
