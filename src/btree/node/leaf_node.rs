use crate::{btree::page::{PageId, Page}, hash::Hash, offset::Offset};

use super::{NodeType, basic_node::{BasicNode, Record, self}};

#[derive(Clone, Copy)]
#[repr(C)]
pub struct LeafNodeHdr {
    node_type: NodeType,
}

pub struct LeafNode {
    node: BasicNode<LeafNodeHdr, Hash, Offset>,
}

impl LeafNode {
    /// Create a new node by the page.
    /// 
    /// # Safety
    /// 
    /// We are not sure that it is a leaf node or not. So you should make
    /// sure or just use `init` to get a empty leaf node.
    pub unsafe fn new_unchecked(page: Page) -> Self {
        Self { node: BasicNode::new_unchecked(page) }
    }

    /// Init self as an empty internal node.
    /// 
    /// # Safety
    /// 
    /// Remember to use `make_dirty` and sync.
    pub unsafe fn init(&mut self) {
        self.node.init();
        let hdr = self.node.mut_page_wrapper().mut_hdr();
        hdr.node_type = NodeType::Leaf;
    }

    pub fn page_id(&self) -> PageId {
        self.node.page_id()
    }

    /// Get the offset by hash key.
    pub fn get(&self, key: &Hash) -> Option<Offset> {
        self.node.get(key)
    }

    /// Put a new record. Cool?
    /// 
    /// # Safety
    /// 
    /// - Are you sure there is more space to hold a new record? Use `is_full`
    ///   to check it.
    /// - Remember to use `make_dirty` and sync.
    pub unsafe fn put(&mut self, key: &Hash, value: &Offset) {
        self.node.put(key, value)
    }

    /// Is me full?
    pub fn is_full(&self) -> bool {
        self.node.is_full()
    }

    /// Make the inner page dirty.
    pub fn make_dirty(&mut self) {
        self.node.make_dirty()
    }

    /// Shift half of records from `self` to `rhs`.
    ///
    /// # Safety
    ///
    /// - It is your duty to make sure `rhs` is not full: maybe `is_full()` can
    ///   help you.
    /// - It is also your duty to make sure `self` is not empty: maybe
    ///   `is_empty()` can help you.
    /// - Remember to use `make_dirty` and sync - both `self` and `rhs`.
    pub unsafe fn split(&mut self, rhs: &mut Self) {
        self.node.split(&mut rhs.node);
    }

    /// # Safety
    /// 
    /// Do not touch it unless you will call `make_dirty` and sync it.
    pub unsafe fn mut_page(&mut self) -> &mut Page {
        unsafe { self.node.mut_page() }
    }

    /// # Safety
    /// 
    /// Make sure self is not empty node.
    pub unsafe fn rightest_key(&self) -> &Hash {
        &self.node.rightest_record().key
    }

    pub fn into_iter<'a>(&'a self) -> basic_node::BasicNodeIter<'a, LeafNodeHdr, Hash, Offset> {
        self.node.into_iter()
    }
}
