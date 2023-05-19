use crate::btree::page::{Page, PageId};

use super::NodeType;

/// `HeadNode` is the most node of the `BTree` - it MUST be defined by the
/// first page. It contains some metadata of this B-tree.
pub struct HeadNode(Page);

const HEAD_NODE_MAGIC: &'static str = "skogkatt.org/WasteIsland/B-Plus-Tree";

#[repr(C)]
pub struct HeadNodeHdr {
    // node_type + version + magic = 64 bytes
    node_type: NodeType,
    version: u8,
    magic: [u8; 62],

    // 4 bytes
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

    /// # Safety
    /// 
    /// Remember to make it dirty and sync it if you change it.
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
    /// Do not touch the page unless you remember to make it dirty and sync it.
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
        let mut magic = vec![0u8; 62];
        magic[0..HEAD_NODE_MAGIC.len()].copy_from_slice(HEAD_NODE_MAGIC.as_bytes());
        hdr.magic = magic.as_slice().try_into().unwrap();
        hdr.root_node_page_id = root_node_page_id;
    }

    /// Check to make sure this page is really a `HeadNode`: by check its magic
    /// bytes, version and something else.
    pub fn check(&self) -> bool {
        let hdr = self.hdr();

        let magic_matched = (|| {
            for i in 0..HEAD_NODE_MAGIC.len() {
                if hdr.magic[i] != HEAD_NODE_MAGIC.as_bytes()[i] {
                    return false
                }
            }
            for i in HEAD_NODE_MAGIC.len()..62 {
                if hdr.magic[i] != 0u8 {
                    return false
                }
            }
            return true
        })();

        hdr.node_type == NodeType::Head
            && hdr.version == 0
            && magic_matched
    }

    /// Make self is dirty.
    pub fn make_dirty(&mut self) {
        self.0.make_dirty()
    }
}
