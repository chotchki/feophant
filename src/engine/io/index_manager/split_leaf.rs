use crate::engine::{
    io::{
        block_layer::lock_cache_manager::{LockCacheManager, LockCacheManagerError},
        format_traits::Serializable,
        index_formats::{BTreeLeaf, BTreeLeafError},
        page_formats::{PageId, PageOffset, PageType},
        row_formats::ItemPointer,
    },
    objects::{Index, SqlTuple},
};
use thiserror::Error;

/// Takes a leaf node and produces a new right node
pub async fn split_leaf(
    lcm: &LockCacheManager,
    index_def: &Index,
    mut leaf: BTreeLeaf,
    new_key: SqlTuple,
    item_ptr: ItemPointer,
) -> Result<(SqlTuple, PageOffset, PageOffset, PageOffset), SplitLeafError> {
    let page_id = PageId {
        resource_key: index_def.id,
        page_type: PageType::Data,
    };

    let left_node_offset = lcm.get_offset_non_zero(page_id).await?;
    let right_node_offset = lcm.get_offset_non_zero(page_id).await?;

    let mut left_node_page = lcm.get_page_for_update(page_id, &left_node_offset).await?;
    let mut right_node_page = lcm.get_page_for_update(page_id, &right_node_offset).await?;

    let (new_split_key, new_right_node) =
        leaf.add_and_split(left_node_offset, right_node_offset, new_key, item_ptr)?;

    let parent_node_offset = leaf.parent_node;

    leaf.serialize_and_pad(&mut left_node_page);
    new_right_node.serialize_and_pad(&mut right_node_page);

    lcm.update_page(page_id, left_node_offset, left_node_page)
        .await?;
    lcm.update_page(page_id, right_node_offset, right_node_page)
        .await?;

    Ok((
        new_split_key,
        parent_node_offset,
        left_node_offset,
        right_node_offset,
    ))
}

#[derive(Debug, Error)]
pub enum SplitLeafError {
    #[error(transparent)]
    BTreeLeafError(#[from] BTreeLeafError),
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
            block_layer::file_manager::FileManager, index_formats::BTreeNode, page_formats::UInt12,
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
    async fn test_split_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let tmp_dir = tmp.path().as_os_str().to_os_string();

        let fm = Arc::new(FileManager::new(tmp_dir)?);
        let lcm = LockCacheManager::new(fm);

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

        let parent_offset = lcm.get_offset_non_zero(page_id).await?;
        let mut leaf = BTreeLeaf::new(parent_offset);
        let leaf_size = leaf.nodes.len();

        for i in 0..10 {
            let (key, ptr) = get_key(i);
            if leaf.can_fit(&key) {
                leaf.add(key, ptr)?;
            }
        }

        let (key, ptr) = get_key(11);

        let (split_key, parent_node, left_offset, right_offset) =
            split_leaf(&lcm, &index, leaf, key, ptr).await?;

        let left_page = lcm.get_page(page_id, &left_offset).await?;
        let mut left_buffer = left_page.as_ref().unwrap().clone();
        let left_node = match BTreeNode::parse(&mut left_buffer, &index)? {
            BTreeNode::Branch(_) => panic!("Unexpected branch"),
            BTreeNode::Leaf(l) => l,
        };

        let right_page = lcm.get_page(page_id, &right_offset).await?;
        let mut right_buffer = right_page.as_ref().unwrap().clone();
        let right_node = match BTreeNode::parse(&mut right_buffer, &index)? {
            BTreeNode::Branch(_) => panic!("Unexpected branch"),
            BTreeNode::Leaf(l) => l,
        };

        assert_eq!(parent_node, left_node.parent_node);
        assert_eq!(parent_node, right_node.parent_node);

        for n in left_node.nodes {
            assert!(n.0 <= split_key);
        }

        for n in right_node.nodes {
            assert!(n.0 > split_key);
        }

        Ok(())
    }
}
