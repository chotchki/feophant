use crate::engine::io::{
    format_traits::{Parseable, Serializable},
    page_formats::{PageOffset, PageOffsetError},
};
use bytes::BufMut;
use thiserror::Error;

/// Special page that points to where the root page of the index is really located
#[derive(Debug, PartialEq)]
pub struct BTreeFirstPage {
    pub root_offset: PageOffset,
}

impl Parseable<BTreeFirstPageError> for BTreeFirstPage {
    type Output = Self;
    fn parse(buffer: &mut impl bytes::Buf) -> Result<Self::Output, BTreeFirstPageError> {
        let root_offset = PageOffset::parse(buffer)?;
        Ok(BTreeFirstPage { root_offset })
    }
}

impl Serializable for BTreeFirstPage {
    fn serialize(&self, buffer: &mut impl BufMut) {
        self.root_offset.serialize(buffer);
    }
}

#[derive(Debug, Error)]
pub enum BTreeFirstPageError {
    #[error(transparent)]
    PageOffsetError(#[from] PageOffsetError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::BytesMut;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::{
        constants::PAGE_SIZE,
        engine::io::{
            block_layer::{file_manager::FileManager, lock_cache_manager::LockCacheManager},
            page_formats::{PageId, PageType},
        },
    };

    use super::*;

    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let first = BTreeFirstPage {
            root_offset: PageOffset(1),
        };

        let mut buffer = BytesMut::with_capacity(PAGE_SIZE as usize);
        first.serialize(&mut buffer);

        let result = BTreeFirstPage::parse(&mut buffer)?;

        assert_eq!(first, result);

        Ok(())
    }

    #[tokio::test]
    async fn test_on_disk() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let lm = LockCacheManager::new(fm.clone());

        let page_id = PageId {
            resource_key: Uuid::new_v4(),
            page_type: PageType::Data,
        };

        let first_offset = lm.get_offset(page_id).await?;
        assert_eq!(first_offset, PageOffset(0));

        let mut first_page = lm.get_page_for_update(page_id, &first_offset).await?;
        let root_offset = lm.get_offset_non_zero(page_id).await?;
        assert_ne!(root_offset, PageOffset(0));

        let btfp = BTreeFirstPage { root_offset };
        btfp.serialize_and_pad(&mut first_page);
        lm.update_page(page_id, first_offset, first_page).await?;

        // Okay now its time to actually test, without drop
        let mut new_first_page = lm.get_page(page_id, &PageOffset(0)).await?.clone();
        if let Some(s) = new_first_page.as_mut() {
            let btfp2 = BTreeFirstPage::parse(&mut s.clone())?;
            assert_ne!(btfp2.root_offset, PageOffset(0));
        } else {
            panic!("That page should exist!");
        }

        // Test again with a drop
        drop(lm);
        let lm2 = LockCacheManager::new(fm);

        let mut new_first_page2 = lm2.get_page(page_id, &PageOffset(0)).await?.clone();
        if let Some(s) = new_first_page2.as_mut() {
            let btfp2 = BTreeFirstPage::parse(&mut s.clone())?;
            assert_ne!(btfp2.root_offset, PageOffset(0));
        } else {
            panic!("That page should exist!");
        }

        Ok(())
    }
}
