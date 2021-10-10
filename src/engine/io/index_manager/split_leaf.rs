use crate::engine::{
    io::{
        block_layer::file_manager2::{FileManager2, FileManager2Error},
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
    fm: &FileManager2,
    index_def: &Index,
    mut leaf: BTreeLeaf,
    new_key: SqlTuple,
    item_ptr: ItemPointer,
) -> Result<(SqlTuple, PageOffset, PageOffset, PageOffset), SplitLeafError> {
    let page_id = PageId {
        resource_key: index_def.id,
        page_type: PageType::Data,
    };

    let (left_node_offset, left_node_guard) = fm.get_next_offset(&page_id).await?;
    let (right_node_offset, right_node_guard) = fm.get_next_offset(&page_id).await?;

    let (new_split_key, new_right_node) =
        leaf.add_and_split(left_node_offset, right_node_offset, new_key, item_ptr)?;

    let parent_node_offset = leaf.parent_node;

    fm.update_page(left_node_guard, leaf.serialize_and_pad())
        .await?;
    fm.update_page(right_node_guard, new_right_node.serialize_and_pad())
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
    FileManager2Error(#[from] FileManager2Error),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::engine::{
        io::{
            block_layer::file_manager2::FileManager2, index_formats::BTreeNode,
            page_formats::UInt12,
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

        let (parent_offset, _parent_guard) = fm.get_next_offset(&page_id).await?;
        let mut leaf = BTreeLeaf::new(parent_offset);
        //let leaf_size = leaf.nodes.len();

        for i in 0..10 {
            let (key, ptr) = get_key(i);
            if leaf.can_fit(&key) {
                leaf.add(key, ptr)?;
            }
        }

        let (key, ptr) = get_key(11);

        let (split_key, parent_node, left_offset, right_offset) =
            split_leaf(&fm, &index, leaf, key, ptr).await?;

        let (mut left_page, _left_guard) = fm.get_page(&page_id, &left_offset).await?;
        let left_node = match BTreeNode::parse(&mut left_page, &index)? {
            BTreeNode::Branch(_) => panic!("Unexpected branch"),
            BTreeNode::Leaf(l) => l,
        };

        let (mut right_page, _right_guard) = fm.get_page(&page_id, &right_offset).await?;
        let right_node = match BTreeNode::parse(&mut right_page, &index)? {
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
