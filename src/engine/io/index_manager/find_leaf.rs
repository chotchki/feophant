use crate::engine::{
    io::{
        block_layer::file_manager2::{FileManager2, FileManager2Error},
        format_traits::{Parseable, Serializable},
        index_formats::{
            BTreeBranchError, BTreeFirstPage, BTreeFirstPageError, BTreeLeaf, BTreeNode,
            BTreeNodeError,
        },
        page_formats::{PageId, PageOffset, PageType},
    },
    objects::{Index, SqlTuple},
};
use thiserror::Error;
use tokio::sync::OwnedRwLockWriteGuard;

pub async fn find_leaf(
    fm: &FileManager2,
    index_def: &Index,
    new_key: &SqlTuple,
) -> Result<(OwnedRwLockWriteGuard<(PageId, PageOffset)>, BTreeLeaf), FindLeafError> {
    let page_id = PageId {
        resource_key: index_def.id,
        page_type: PageType::Data,
    };

    let mut prior_offset = PageOffset(0);
    let mut offset = PageOffset(0);

    loop {
        match fm.get_page_for_update(&page_id, &offset).await {
            Ok((mut buffer, page_guard)) => {
                if offset == PageOffset(0) {
                    let first_node = BTreeFirstPage::parse(&mut buffer)?;
                    if first_node.root_offset == PageOffset(0) {
                        debug!("root is zero");

                        let (root_offset, root_guard) = fm.get_next_offset(&page_id).await?;
                        let root_node = BTreeLeaf::new(offset);
                        fm.add_page(root_guard, root_node.serialize_and_pad())
                            .await?;

                        let first_node = BTreeFirstPage { root_offset };
                        fm.add_page(page_guard, first_node.serialize_and_pad())
                            .await?;

                        offset = root_offset;
                        continue;
                    }
                    offset = first_node.root_offset
                } else {
                    let node = BTreeNode::parse(&mut buffer, index_def)?;

                    prior_offset = offset;
                    match node {
                        BTreeNode::Branch(b) => {
                            offset = *b.search(new_key..new_key)?;
                            continue;
                        }
                        BTreeNode::Leaf(l) => {
                            return Ok((page_guard, l));
                        }
                    }
                }
            }
            Err(e) => {
                if offset == PageOffset(0) {
                    //Try to make the first page node
                    let (new_offset, new_guard) = fm.get_next_offset(&page_id).await?;
                    if new_offset != PageOffset(0) {
                        //Someone else is concurrently making this
                        continue;
                    }

                    let (root_offset, root_guard) = fm.get_next_offset(&page_id).await?;
                    let root_node = BTreeLeaf::new(new_offset);
                    fm.add_page(root_guard, root_node.serialize_and_pad())
                        .await?;

                    let first_node = BTreeFirstPage { root_offset };
                    fm.add_page(new_guard, first_node.serialize_and_pad())
                        .await?;

                    offset = root_offset;
                    continue;
                }
                return Err(FindLeafError::FileManager2(e));
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum FindLeafError {
    #[error(transparent)]
    BTreeBranch(#[from] BTreeBranchError),
    #[error(transparent)]
    BTreeFirstPage(#[from] BTreeFirstPageError),
    #[error(transparent)]
    BTreeNode(#[from] BTreeNodeError),
    #[error(transparent)]
    FileManager2(#[from] FileManager2Error),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::engine::{
        io::{index_manager::find_leaf, page_formats::UInt12, row_formats::ItemPointer},
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

        let fm = Arc::new(FileManager2::new(tmp_dir)?);

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

        let (first_offset, first_guard) = fm.get_next_offset(&page_id).await?;
        assert_eq!(first_offset, PageOffset(0));

        let (root_offset, root_guard) = fm.get_next_offset(&page_id).await?;
        assert_ne!(root_offset, PageOffset(0));

        let btfp = BTreeFirstPage { root_offset };
        fm.update_page(first_guard, btfp.serialize_and_pad())
            .await?;

        let mut root = BTreeLeaf::new(first_offset);
        let (key, ptr) = get_key(42);
        root.add(key.clone(), ptr)?;

        fm.update_page(root_guard, root.serialize_and_pad()).await?;

        // Okay now its time to actually test
        let (guard, leaf) = find_leaf(&fm, &index, &key).await?;
        assert_eq!(leaf, root);
        assert_ne!(guard.1, PageOffset(0));

        let (guard2, leaf2) = find_leaf(&fm, &index, &key).await?;
        assert_eq!(leaf2, root);
        assert_eq!(guard.1, guard2.1);
        Ok(())
    }
}
