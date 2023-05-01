use super::page::Page;

mod basic_node;
mod internal_node;
mod leaf_node;
mod head_node;

pub use head_node::HeadNode;
pub use leaf_node::LeafNode;
pub use internal_node::InternalNode;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum NodeType {
    Head = 1,
    Leaf = 2,
    Internal = 3,
}

pub fn get_node_type(page: &Page) -> NodeType {
    match page.buf()[0] {
        1 => NodeType::Head,
        2 => NodeType::Leaf,
        3 => NodeType::Internal,
        _ => panic!("unexcepted type")
    }
}

