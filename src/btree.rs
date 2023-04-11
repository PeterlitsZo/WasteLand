use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    rc::Rc,
    sync::{Mutex, MutexGuard, RwLock},
    cell::RefCell,
};

use crate::{
    error::{Error, ToInnerResult},
    hash::{Hash, RefHash, HASH_LENGTH},
    offset::{Offset, OFFSET_LENGTH},
};

/// The size of page in the b-tree file.
const PAGE_SIZE: usize = 4usize << 10; // 4 KB

pub struct BTree {
    file: InnerFile,
    head_page: HeadPage,
}

type InnerFile = Rc<Mutex<File>>;

struct Page {
    file: InnerFile,
    page_id: PageId,
    buf: Option<Rc<RwLock<[u8; PAGE_SIZE]>>>,
}

#[derive(Debug, PartialEq)]
enum PageType {
    HeadPage,
    LeafPage,
    NonLeafPage,
}

const PAGE_ID_LENGTH: usize = 2;

/// The ID refered to a page. It should be unikey in the B-Tree. It need
/// `PAGE_ID_LENGTH` bytes to hold data.
type PageId = u32;

/// The offset of the record ID (See `RecordId`).
type RecordIdOffset = usize;

/// The ID refered to a record. It just need be unikey in the page. It only need
/// 1 byte to hold data.
type RecordId = u8;

/// `HeadPage` is the first page of the `BTree`.
///
/// Its struct is:
///
///   1. byte `0`: The type of the page - It must be `PageType::HeadPage`.
///   2. byte `1 ..= 2`: The page ID of the root page.
struct HeadPage(Page);

/// `LeafPage` is the leaf node of the `BTree`. It is a set of records, which
/// are the pairs of `(Hash, Offset)`.
///
/// Its struct is:
///
///   1. byte `0`: The type of the page - It must be `PageType::LeafPage`.
///   2. byte `1`: The length of the records set.
///   3. byte `2 ..= 2 + Self::CAPACITY`: The ordered record ID (unikey in
///      page).
///
///      For example, if there are 3 records: `(1f00, 4)`'s ID is 3,
///      `(3f00, 1)`'s ID is 0, `(2f00, 3)`'s ID is 1. It should be
///      `[3, 1, 0]` (to represents the records `(1f00, 4)` < `(2f00, 3)` <
///      `(3f00, 1)`).
///
///      There are almost `Self::CAPACITY` records here.
///
///   4. byte `2+Self::CAPACITY+1 .. PAGE_SIZE - Self::RECORD_SIZE * Self::CAPACITY`:
///      Unused space.
///   5. byte `PAGE_SIZE - Self::RECORD_SIZE * Self::CAPACITY .. PAGE_SIZE`:
///      The records. Refered by the record ID.
struct LeafPage(Page);

struct NonLeafPage(Page);

impl BTree {
    const HEAD_PAGE_ID: PageId = 0;

    fn append_new_page(file: InnerFile) {
        let mut file = file.lock().unwrap();
        file.seek(SeekFrom::End(0)).unwrap();
        file.write(&[0u8; PAGE_SIZE]).unwrap();
    }

    pub fn new(file_name: &PathBuf) -> Result<BTree, Error> {
        let file = File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(file_name)
            .to_inner_result("open or create index data file in read-write mode")?;
        let file = Rc::new(Mutex::new(file));
        let mut head_page = Page::new(file.clone(), Self::HEAD_PAGE_ID);
        let head_page = if !head_page.exist() {
            // Append root page and head page.
            Self::append_new_page(file.clone());
            Self::append_new_page(file.clone());

            // Set the root page.
            let mut root_page = Page::new(file.clone(), Self::HEAD_PAGE_ID + 1);
            root_page.set_type(PageType::LeafPage);
            root_page.sync();

            // Set the head page.
            head_page.set_type(PageType::HeadPage);
            let mut head_page = HeadPage::new(head_page).unwrap();
            head_page.set_root_page_id(Self::HEAD_PAGE_ID + 1);
            head_page.sync();
            head_page
        } else {
            HeadPage::new(head_page).unwrap()
        };
        let result = BTree {
            file: file.clone(),
            head_page,
        };
        Ok(result)
    }

    pub fn put(&mut self, hash: &Hash, offset: Offset) -> Result<(), Error> {
        let root_page_id = self.head_page.root_page_id();
        let mut root_page = Page::new(self.file.clone(), root_page_id);

        let root_page_type = root_page.get_type().to_inner_result("get type")?;

        assert!(root_page_type == PageType::LeafPage);

        let mut root_page =
            LeafPage::new(root_page).to_inner_result("turn root page into leaf page")?;

        let record_id_offset = root_page.find_record_id_offset(hash);
        let record_id = root_page.record_id(record_id_offset);
        let record_hash = root_page.ref_hash(record_id);
        if hash == &record_hash {
            return Ok(());
        }

        let record_id = root_page.insert_new_record(hash.as_ref(), offset);
        root_page.insert_new_record_id(record_id_offset, record_id);

        root_page.sync();

        Ok(())
    }

    pub fn get(&mut self, hash: &Hash) -> Result<Option<Offset>, Error> {
        let root_page_id = self.head_page.root_page_id();
        let mut root_page = Page::new(self.file.clone(), root_page_id);

        let root_page_type = root_page.get_type().to_inner_result("get type")?;
        assert!(root_page_type == PageType::LeafPage);
        let mut root_page =
            LeafPage::new(root_page).to_inner_result("turn root page into leaf page")?;

        let record_id_offset = root_page.find_record_id_offset(hash);
        let record_id = root_page.record_id(record_id_offset);
        let record_hash = root_page.ref_hash(record_id);
        let record_offset = root_page.ref_offset(record_id);

        Ok(if hash == &record_hash {
            Some(record_offset)
        } else {
            None
        })
    }
}

impl Page {
    fn exist(&self) -> bool {
        let file = self.file.lock().unwrap();
        let file_len = file.metadata().unwrap().len();

        self.page_offset_in_file() + PAGE_SIZE as u64 <= file_len
    }

    fn page_offset_in_file(&self) -> u64 {
        PAGE_SIZE as u64 * self.page_id as u64
    }

    fn seek_file(&self) -> MutexGuard<File> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(self.page_offset_in_file())).unwrap();
        file
    }

    fn get_buf(&mut self) -> Rc<RwLock<[u8; PAGE_SIZE]>> {
        match &self.buf {
            Some(buf) => { buf.clone() },
            None => {
                let mut buf = [0u8; PAGE_SIZE];
                self.seek_file().read(&mut buf).unwrap();
                let buf = Rc::new(RwLock::new(buf));
                self.buf = Some(buf.clone());
                buf
            }
        }
    }

    fn sync(&mut self) {
        if (&self.buf).is_none() {
            return
        }
        let buf = self.get_buf();
        let buf = buf.read().unwrap();
        self.seek_file().write_all(buf.as_ref()).unwrap();
    }

    /// Get the page type by the first byte of the page.
    fn get_type(&mut self) -> Result<PageType, Error> {
        let buf = self.get_buf();
        let buf = buf.read().unwrap();
        PageType::from_u8(buf[0])
    }

    /// Set the page type.
    fn set_type(&mut self, page_type: PageType) {
        let buf = self.get_buf();
        let mut buf = buf.write().unwrap();
        buf[0] = page_type.to_u8();
    }

    fn new(file: InnerFile, page_id: PageId) -> Self {
        Self {
            file,
            page_id,
            buf: None,
        }
    }
}

impl PageType {
    fn from_u8(u8_integer: u8) -> Result<Self, Error> {
        Ok(match u8_integer {
            0 => Self::HeadPage,
            1 => Self::LeafPage,
            2 => Self::NonLeafPage,
            _ => {
                return Err(Error::new(
                    "get unexpected u8 argument when try to turn a u8 into type PageType",
                ))
            }
        })
    }

    fn to_u8(&self) -> u8 {
        match &self {
            Self::HeadPage => 0,
            Self::LeafPage => 1,
            Self::NonLeafPage => 2,
        }
    }
}

impl HeadPage {
    fn new(mut page: Page) -> Result<Self, Error> {
        if page.get_type()? != PageType::HeadPage {
            return Err(Error::new(
                "cannot create a new HeadPage by Page whose type is not PageType::HeadPage",
            ));
        }
        Ok(Self(page))
    }

    fn sync(&mut self) {
        self.0.sync()
    }

    fn root_page_id(&mut self) -> PageId {
        let buf = self.0.get_buf();
        let buf = buf.read().unwrap();
        let result_0 = 0u32 + (buf[1] as u32);
        let result = (result_0 << 8) + (buf[2] as u32);
        result
    }

    /// Set the page ID of the root page.
    ///
    /// Remember to use `sync` to write to file.
    fn set_root_page_id(&mut self, root_page_id: PageId) {
        let buf = self.0.get_buf();
        let mut buf = buf.write().unwrap();
        buf[1] = (root_page_id >> 8) as u8;
        buf[2] = root_page_id as u8;
    }
}

impl LeafPage {
    const RECORD_SIZE: usize = HASH_LENGTH + OFFSET_LENGTH;

    fn new(mut page: Page) -> Result<Self, Error> {
        if page.get_type()? != PageType::LeafPage {
            return Err(Error::new(
                "cannot create a new HeadPage by Page whose type is not PageType::HeadPage",
            ));
        }
        Ok(Self(page))
    }

    fn sync(&mut self) {
        self.0.sync()
    }

    fn len(&mut self) -> usize {
        let buf = self.0.get_buf();
        let buf = buf.read().unwrap();
        buf[1] as usize
    }

    fn set_len(&mut self, new_len: usize) {
        let buf = self.0.get_buf();
        let mut buf = buf.write().unwrap();
        buf[1] = new_len as u8;
    }

    fn ref_hash(&mut self, record_id: RecordId) -> Hash {
        let record_self_offset = PAGE_SIZE - Self::RECORD_SIZE * (record_id as usize + 1);
        let buf = self.0.get_buf();
        let buf = buf.read().unwrap();
        let hash_bytes = &buf[record_self_offset..record_self_offset + HASH_LENGTH];
        Hash::from_bytes(hash_bytes.try_into().unwrap())
    }

    fn ref_offset(&mut self, record_id: RecordId) -> Offset {
        let record_self_offset = PAGE_SIZE - Self::RECORD_SIZE * (record_id as usize + 1);
        let buf = self.0.get_buf();
        let buf = buf.read().unwrap();
        let offset_bytes = &buf[record_self_offset + HASH_LENGTH..record_self_offset + Self::RECORD_SIZE];
        Offset::from_bytes(offset_bytes.try_into().unwrap())
    }

    fn record_id(&mut self, offset: RecordIdOffset) -> RecordId {
        let buf = self.0.get_buf();
        let buf = buf.read().unwrap();
        let record_id = buf[2 + offset];
        record_id
    }

    fn set_recrod_id(&mut self, offset: RecordIdOffset, record_id: RecordId) {
        let buf = self.0.get_buf();
        let mut buf = buf.write().unwrap();
        buf[2 + offset] = record_id;
    }

    /// Find the offset of the record ID, which refers first record that bigger
    /// than or equal to the given hash.
    ///
    /// If we cann't find it, return the `self.len()` (as we know, the range of
    /// the record ID is `[0, self.len() - 1]`).
    fn find_record_id_offset(&mut self, hash: &Hash) -> RecordIdOffset {
        let mut left = 0;
        let mut right = self.len();
        while right != left {
            let mid = (left + right) / 2;
            let record_id = self.record_id(mid);
            let ref_hash = self.ref_hash(record_id);
            if &ref_hash < hash {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }

    /// Insert a new record and return its ID.
    ///
    /// Remember to use method `sync`.
    fn insert_new_record(&mut self, hash: RefHash, offset: Offset) -> RecordId {
        let buf = self.0.get_buf();
        let len = self.len();
        let mut buf = buf.write().unwrap();

        let start = PAGE_SIZE - Self::RECORD_SIZE * (len + 1);
        let end = start + Self::RECORD_SIZE;
        buf[start..start + HASH_LENGTH].copy_from_slice(hash.to_bytes_ref());
        buf[start + HASH_LENGTH..end].copy_from_slice(&offset.to_bytes());

        len as RecordId
    }

    fn insert_new_record_id(&mut self, offset: RecordIdOffset, record_id: RecordId) {
        for i in (offset..self.len()).rev() {
            let record_id = self.record_id(i);
            self.set_recrod_id(i + 1, record_id)
        }
        self.set_recrod_id(offset, record_id);
        let new_len = self.len() + 1;
        self.set_len(new_len);
    }
}
