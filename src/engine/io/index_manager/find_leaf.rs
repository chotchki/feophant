use crate::engine::{
    io::{
        block_layer::lock_cache_manager::{LockCacheManager, LockCacheManagerError},
        format_traits::{Parseable, Serializable},
        index_formats::{
            BTreeBranchError, BTreeFirstPage, BTreeFirstPageError, BTreeLeaf, BTreeNode,
            BTreeNodeError,
        },
        page_formats::{PageId, PageOffset, PageType},
    },
    objects::{Index, SqlTuple},
};
use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::sync::OwnedRwLockWriteGuard;

pub async fn find_leaf(
    lcm: &LockCacheManager,
    index_def: &Index,
    new_key: &SqlTuple,
) -> Result<(OwnedRwLockWriteGuard<Option<Bytes>>, PageOffset, BTreeLeaf), FindLeafError> {
    let page_id = PageId {
        resource_key: index_def.id,
        page_type: PageType::Data,
    };

    let mut prior_offset = PageOffset(0);
    let mut offset = PageOffset(0);

    loop {
        let mut page = lcm.get_page_for_update(page_id, &offset).await?;

        //Handle the first page
        if offset == PageOffset(0) {
            offset = match page.as_mut() {
                Some(s) => {
                    let mut page_node = BTreeFirstPage::parse(&mut s.clone())?;
                    if page_node.root_offset == PageOffset(0) {
                        debug!("root is zero");
                        page_node.root_offset = lcm.get_offset_non_zero(page_id).await?;

                        page_node.serialize_and_pad(&mut page);
                        lcm.update_page(page_id, offset, page).await?;
                    }
                    page_node.root_offset
                }
                None => {
                    debug!("page doesn't exist");
                    let root_offset = lcm.get_offset_non_zero(page_id).await?;
                    let page_node = BTreeFirstPage { root_offset };

                    page_node.serialize_and_pad(&mut page);
                    lcm.update_page(page_id, offset, page).await?;

                    page_node.root_offset
                }
            };
            continue;
        }

        match page.as_mut() {
            None => {
                //Special case, should only be due to root not existing
                return Ok((page, offset, BTreeLeaf::new(prior_offset)));
            }
            Some(s) => {
                let node = BTreeNode::parse(s, index_def)?;

                prior_offset = offset;
                match node {
                    BTreeNode::Branch(b) => {
                        offset = *b.search(new_key..new_key)?;
                        continue;
                    }
                    BTreeNode::Leaf(l) => {
                        return Ok((page, offset, l));
                    }
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum FindLeafError {
    #[error(transparent)]
    BTreeBranchError(#[from] BTreeBranchError),
    #[error(transparent)]
    BTreeFirstPageError(#[from] BTreeFirstPageError),
    #[error(transparent)]
    BTreeNodeError(#[from] BTreeNodeError),
    #[error(transparent)]
    LockCacheManagerError(#[from] LockCacheManagerError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::engine::{
        io::{
            block_layer::file_manager::FileManager, index_manager::find_leaf, page_formats::UInt12,
            row_formats::ItemPointer,
        },
        objects::types::{BaseSqlTypes, BaseSqlTypesMapper, SqlTypeDefinition},
    };

    use super::*;

    //Super unsafe function to get test data, just don't count too high
    fn get_key(index: usize) -> (SqlTuple, ItemPointer) {
        (
            SqlTuple(vec![Some(BaseSqlTypes::Integer(index as u32))]),
            ItemPointer::new(PageOffset(index), UInt12::new(index as u16).unwrap()),
        )
    }

    #[tokio::test]
    async fn test_find() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let lm = LockCacheManager::new(fm.clone());

        let index = Index {
            id: Uuid::new_v4(),
            name: "test".to_string(),
            columns: Arc::new(SqlTypeDefinition(vec![(
                "foo".to_string(),
                BaseSqlTypesMapper::Integer,
            )])),
            unique: false,
        };
        let page_id = PageId {
            resource_key: index.id,
            page_type: PageType::Data,
        };

        let first_offset = lm.get_offset(page_id).await?;
        assert_eq!(first_offset, PageOffset(0));

        let mut first_page = lm.get_page_for_update(page_id, &first_offset).await?;
        let root_offset = lm.get_offset_non_zero(page_id).await?;
        assert_ne!(root_offset, PageOffset(0));
        let mut root_page = lm.get_page_for_update(page_id, &root_offset).await?;

        let btfp = BTreeFirstPage { root_offset };
        btfp.serialize_and_pad(&mut first_page);
        lm.update_page(page_id, first_offset, first_page).await?;

        let mut root = BTreeLeaf::new(first_offset);
        let (key, ptr) = get_key(42);
        root.add(key.clone(), ptr)?;
        root.serialize_and_pad(&mut root_page);
        lm.update_page(page_id, root_offset, root_page).await?;

        // Okay now its time to actually test
        let (_, offset, leaf) = find_leaf(&lm, &index, &key).await?;
        assert_eq!(leaf, root);
        assert_ne!(offset, PageOffset(0));

        let (_, offset2, leaf2) = find_leaf(&lm, &index, &key).await?;
        assert_eq!(leaf2, root);
        assert_eq!(offset, offset2);
        Ok(())
    }
}
