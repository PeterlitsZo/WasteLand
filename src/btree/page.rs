use std::{fmt::Debug, alloc::{alloc, Layout, dealloc}};

/// The size of page in the b-tree file.
pub const PAGE_SIZE: usize = 4usize << 10; // 4 KB

/// The ID refered to a page. It should be unikey in the B-Tree. It need
/// `PAGE_ID_LENGTH` bytes to hold data.
#[derive(Eq, Hash, PartialEq, Clone, Copy)]
pub struct PageId(u32);

struct PageInner {
    id: PageId,
    ref_cnt: usize,
    is_dirty: bool,
    buf: [u8; PAGE_SIZE],
}

/// The cached page. It will have a data in the heap.
/// 
/// There are maybe more than one want to access the page --- and it is safe
/// if you use `clone`. Because it contains a reference counter in the heap.
/// 
/// The implement don't use `Rc` because we need to avoid cycle. And I have no
/// idea how to alloc a uninited memory and turn it into `Weak` as well.
pub struct Page {
    inner: *mut PageInner,
}

impl PageId {
    pub const fn new(page_id: usize) -> Self {
        Self(page_id as u32)
    }

    pub fn raw(&self) -> u32 {
        return self.0;
    }

    pub fn invalid() -> Self {
        Self(u32::MAX)
    }
}

impl Debug for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("PageId({})", self.0))
    }
}

impl Page {
    /// Get a `Page` containing an uninited buffer in it. The buffer of the
    /// uninited `Page` is not all zeroed.
    /// 
    /// # Safety
    /// 
    /// Make sure the `Page`'s buffer will be as same as the disk content...
    pub unsafe fn new_uninited(id: PageId) -> Self {
        let inner = {
            let ptr = unsafe {
                let ptr = alloc(Layout::new::<PageInner>()) as *mut PageInner;
                &mut *ptr
            };
            ptr.id = id;
            ptr.is_dirty = false;
            ptr.ref_cnt = 1;
            ptr as *mut PageInner
        };
        Self { inner }
    }

    /// Get the mutable reference to the inner buffer.
    /// 
    /// # Safety
    /// 
    /// If you changed the content in the buffer, involve `make_dirty`.
    pub unsafe fn mut_buf(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.mut_inner().buf
    }

    /// Get the unmutable reference to the inner buffer.
    pub fn buf(&self) -> &[u8; PAGE_SIZE] {
        &self.inner().buf
    }

    /// Get the ID of this page.
    pub fn id(&self) -> PageId {
        self.inner().id
    }

    /// Set the `is_dirty` flag is true.
    pub fn make_dirty(&mut self) {
        unsafe { self.mut_inner().is_dirty = true; }
    }

    /// Get the `is_dirty` flag.
    pub fn is_dirty(&self) -> bool {
        self.inner().is_dirty
    }

    /// Clear the `is_dirty` flag.
    pub fn clear(&mut self) {
        unsafe { self.mut_inner().is_dirty = false; }
    }

    /// Get the mutable reference to the inner struct.
    /// 
    /// # Safety
    /// 
    /// Make sure those fields in the inner struct will not be dirty.
    unsafe fn mut_inner(&mut self) -> &mut PageInner {
        &mut *self.inner
    }

    /// Get the unmutable reference to the inner struct.
    fn inner(&self) -> &PageInner {
        unsafe { &*self.inner }
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        let mut result = Self { inner: self.inner };
        unsafe { result.mut_inner().ref_cnt += 1; }
        result
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        let inner = unsafe { self.mut_inner() };
        inner.ref_cnt -= 1;
        if inner.ref_cnt == 0 {
            unsafe { dealloc(self.inner as *mut u8, Layout::new::<PageInner>()); }
        }
    }
}
