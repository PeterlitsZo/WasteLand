use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::{RwLock, Arc}
};

use crate::error::{Error, ToInnerResult};

use super::page::{PageId, Page, PAGE_SIZE};

pub struct PagerInner {
    file: File,
    pages_len: usize,
    /// The cache of pages.
    page_map: HashMap<PageId, Page>,
}

#[derive(Clone)]
pub struct Pager {
    inner: Arc<RwLock<PagerInner>>,
}

/// From page ID to its file seek.
fn page_id_to_file_seek(page_id: PageId) -> SeekFrom {
    let offset = page_id.raw() as u64 * PAGE_SIZE as u64;
    SeekFrom::Start(offset)
}

impl Pager {
    /// Create a new pager by a file.
    pub fn new(file: File) -> Result<Self, Error> {
        let metadata = file.metadata().to_inner_result("get metadata")?;
        let inner = PagerInner {
            file,
            pages_len: (metadata.len() as usize / PAGE_SIZE),
            page_map: HashMap::new(),
        };
        Ok(Pager { inner: Arc::new(RwLock::new(inner)) })
    }

    /// Get the length of the pages.
    pub fn len(&self) -> usize {
        let pager = self.inner.read().unwrap();
        pager.pages_len
    }

    /// Append a new empty page and return it.
    pub fn append_empty_uninited_page(&mut self) -> Result<Page, Error> {
        let mut pager = self.inner.write().unwrap();
        pager.file
            .seek(SeekFrom::End(0))
            .to_inner_result("seek to offset")?;

        let page = unsafe {
            Page::new_uninited(PageId::new(pager.pages_len))
        };
        pager.file
            .write_all(page.buf())
            .to_inner_result("write to file")?;

        pager.page_map.insert(page.id(), page.clone());
        pager.pages_len += 1;

        Ok(page)
    }

    /// Get the page by its page ID.
    pub fn get_page(&mut self, id: PageId) -> Result<Page, Error> {
        let mut pager = self.inner.write().unwrap();
        match pager.page_map.get(&id) {
            Some(p) => {
                let page = p.clone();
                Ok(page)
            }
            None => {
                pager.file
                    .seek(page_id_to_file_seek(id))
                    .to_inner_result("seek to offset")?;

                let mut page = unsafe {
                    Page::new_uninited(id)
                };
                pager.file
                    .read_exact(unsafe { page.mut_buf() })
                    .to_inner_result("read to buffer")?;

                pager.page_map.insert(id, page.clone());

                Ok(page)
            }
        }
    }

    /// Sync the page if the page is dirty (if `page.isDirty` is ture)
    pub fn sync_page(&mut self, page: &mut Page) -> Result<(), Error> {
        if page.is_dirty() {
            page.clear();
            let mut pager = self.inner.write().unwrap();
            pager.file
                .seek(page_id_to_file_seek(page.id()))
                .to_inner_result("seek to page to sync")?;
            pager.file
                .write_all(page.buf())
                .to_inner_result("write page to sync")?;
        }

        Ok(())
    }
}
