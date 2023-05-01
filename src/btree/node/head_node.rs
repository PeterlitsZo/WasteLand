use crate::btree::page::{Page, PageId};

use super::NodeType;

/// `HeadNode` is the most node of the `BTree` - it MUST be defined by the
/// first page. It contains some metadata of this B-tree.
pub struct HeadNode(Page);

const HEAD_NODE_MAGIC: &'static str = "skogkatt.org/WasteIsland/B-Plus-Tree";

#[repr(C)]
pub struct HeadNodeHdr {
    node_type: NodeType,
    version: u8,
    magic: [u8; HEAD_NODE_MAGIC.len()],
    pub root_node_page_id: PageId,
}

impl HeadNode {
    /// Create a new `HeadNode` by the page.
    /// 
    /// # Safety
    /// 
    /// It will not check it is valid or not. So remember to check its
    /// `NodeType` before call this method. Or maybe you can just use `init`.
    pub unsafe fn new_unchecked(page: Page) -> Self {
        Self(page)
    }

    pub unsafe fn mut_hdr(&mut self) -> &mut HeadNodeHdr {
        unsafe { &mut *(self.0.mut_buf() as *mut [u8] as *mut HeadNodeHdr) }
    }

    pub fn hdr(&self) -> &HeadNodeHdr {
        unsafe { &*(self.0.buf() as *const [u8] as *const HeadNodeHdr) }
    }

    /// Get the underlying page...
    /// 
    /// # Safety
    /// 
    /// Do not touch the page unless you remember to sync it.
    pub unsafe fn mut_page(&mut self) -> &mut Page {
        &mut self.0
    }

    /// Init the `HeadNode`.
    /// 
    /// # Safety
    /// 
    /// Remember to use `make_dirty` and sync.
    pub unsafe fn init(&mut self, root_node_page_id: PageId) {
        self.0.make_dirty();
        let hdr = self.mut_hdr();
        hdr.node_type = NodeType::Head;
        hdr.version = 0;
        hdr.magic = HEAD_NODE_MAGIC.as_bytes().try_into().unwrap();
        hdr.root_node_page_id = root_node_page_id;
    }

    /// Check to make sure this page is really a `HeadNode`: by check its magic
    /// bytes, version and something else.
    pub fn check(&self) -> bool {
        let hdr = self.hdr();

        hdr.node_type == NodeType::Head
            && hdr.version == 0
            && hdr.magic == HEAD_NODE_MAGIC.as_bytes()
    }

    /// Make self is dirty.
    pub fn make_dirty(&mut self) {
        self.0.make_dirty()
    }
}
