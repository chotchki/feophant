use crate::{constants::BuiltinSqlTypes, engine::io::page_formats::ItemIdData};

#[derive(Clone, Debug)]
pub enum BTreeNode {
    Branch(BTreeBranch),
    Leaf(BTreeLeaf),
}

#[derive(Clone, Debug)]
pub struct BTreeBranch {
    pub left_node: Option<BTreePage>,
    pub right_node: Option<BTreePage>,
    pub nodes: Vec<(Vec<BuiltinSqlTypes>, BTreePage)>,
}

#[derive(Clone, Debug)]
pub struct BTreeLeaf {
    pub left_node: Option<BTreePage>,
    pub right_node: Option<BTreePage>,
    pub nodes: Vec<(Vec<BuiltinSqlTypes>, ItemIdData)>,
}

#[derive(Clone, Copy, Debug)]
pub enum NodeType {
    Node,
    Leaf,
}

#[derive(Clone, Copy, Debug)]
pub struct BTreePage(pub usize);
