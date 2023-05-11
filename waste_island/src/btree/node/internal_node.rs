use crate::{btree::page::{PageId, Page}, hash::Hash, offset::Offset};

use super::{NodeType, basic_node::{BasicNode, BasicNodeIter, Record}};

#[derive(Clone, Copy)]
#[repr(C)]
pub struct InternalNodeHdr {
    node_type: NodeType,
    pub rightest_page_id: PageId,
}

#[derive(Clone)]
pub struct InternalNode {
    node: BasicNode<InternalNodeHdr, Hash, PageId>,
}

impl InternalNode {
    /// Create a new node by the page.
    /// 
    /// # Safety
    /// 
    /// We are not sure that it is a internal node or not. So you should make
    /// sure or just use `init` to get a empty internal node.
    pub unsafe fn new_unchecked(page: Page) -> Self {
        Self { node: BasicNode::new_unchecked(page) }
    }

    /// Init self as an empty internal node.
    /// 
    /// # Safety
    /// 
    /// Remember to use `make_dirty` and sync.
    pub unsafe fn init(&mut self, rightest_page_id: PageId) {
        self.node.init();
        let hdr = self.node.mut_page_wrapper().mut_hdr();
        hdr.node_type = NodeType::Internal;
        hdr.rightest_page_id = rightest_page_id;
    }

    pub fn page_id(&self) -> PageId {
        self.node.page_id()
    }

    pub fn is_full(&self) -> bool {
        self.node.is_full()
    }

    /// Get the page ID of the next page.
    pub fn get(&self, key: &Hash) -> (Option<Hash>, PageId) {
        match self.node.get_lower_bound_record(key) {
            Some(r) => (Some(r.key), r.value),
            None => (None, self.node.page_wrapper().hdr().rightest_page_id)
        }
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
    /// - You should make sure that it is not empty.
    pub unsafe fn pop_rightest_record(&mut self) -> Record<Hash, PageId> {
        self.node.pop_righest_record()
    }

    /// # Safety
    /// 
    /// If you change the header, then you should remember make it dirty and
    /// sync it.
    pub unsafe fn hdr_mut(&mut self) -> &mut InternalNodeHdr {
        unsafe { self.node.mut_page_wrapper().mut_hdr() }
    }

    /// Put the new record - I mean, (key, left_page_id) into this node.
    /// 
    /// # Safety
    /// 
    /// - Remember to use `make_dirty` and sync.
    /// - Make sure it has more space to store.
    pub unsafe fn put(&mut self, key: &Hash, left_page_id: &PageId) {
        self.node.put(key, left_page_id)
    }

    pub fn make_dirty(&mut self) {
        self.node.make_dirty()
    }

    /// # Safety
    /// 
    /// Do not touch it unless you will call `make_dirty` and sync it.
    pub unsafe fn mut_page(&mut self) -> &mut Page {
        unsafe { self.node.mut_page() }
    }

    pub fn into_iter<'a>(&'a self) -> BasicNodeIter<'a, InternalNodeHdr, Hash, PageId> {
        self.node.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let page = unsafe { Page::new_uninited(PageId::new(114)) };
        let mut node = unsafe { InternalNode::new_unchecked(page) };
        unsafe { node.init(PageId::new(514)) };
    }
}
