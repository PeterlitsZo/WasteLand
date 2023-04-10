use std::io::{Write, Seek, self};
use std::mem::swap;
use std::{fs::File, path::PathBuf, io::Read};

use crate::error::{Error, ToInnerResult};
use crate::utils::{
    offset_usize_to_bytes,
    hash_string_to_bytes,
    HASH_LENGTH,
    OFFSET_LENGTH, offset_bytes_to_usize,
};

/// The size of page in the index file.
const PAGE_SIZE: usize = 4usize << 10; // 4 KB

/// The size of leaf page's head. See type `PageHead` to know more.
const LEAF_PAGE_HEAD_SIZE: usize = 2usize;

/// The size of non-leaf page's head.
const NON_LEAF_PAGE_HEAD_SIZE: usize = 2usize + RECORD_OFFSET_ID_SIZE;

/// The size of head in the index file.
const HEAD_SIZE: usize = PAGE_SIZE;

/// The number of bytes of the page ID.
const PAGE_ID_LENGTH: usize = 2; // 2 byte => 65536 pages

/// The size of record's offset ID.
const RECORD_OFFSET_ID_SIZE: usize = 1;

#[derive(PartialEq)]
enum PageType {
    RecordLeafPage = 0,
    RecordNonLeafPage = 1,
}

impl PageType {
    fn from_u8(u8_integer: u8) -> Result<Self, Error> {
        Ok(match u8_integer {
            0 => Self::RecordLeafPage,
            1 => Self::RecordNonLeafPage,
            _ => return Err(Error::new("get unexpected argument when try to turn a u8 into type PageType"))
        })
    }
}

/// The size of record in the leaf page.
const LEAF_PAGE_RECORD_SIZE: usize = HASH_LENGTH + OFFSET_LENGTH;

/// The capacity of records in the leaf page.
const LEAF_PAGE_RECORD_CAPACITY: usize = (PAGE_SIZE - LEAF_PAGE_HEAD_SIZE) / LEAF_PAGE_RECORD_SIZE;

/// The size of record in the non-leaf page.
const NON_LEAF_PAGE_RECORD_SIZE: usize = HASH_LENGTH + PAGE_ID_LENGTH;

/// The capacity of records in the non-leaf page.
const NON_LEAF_PAGE_RECORD_CAPACITY: usize = (PAGE_SIZE - NON_LEAF_PAGE_HEAD_SIZE) / NON_LEAF_PAGE_RECORD_SIZE;

struct PageHead {
    records_length: u8,
    page_type: PageType,
}

fn get_page_head(page_buf: &[u8; PAGE_SIZE]) -> Result<PageHead, Error> {
    Ok(PageHead {
        records_length: page_buf[0],
        page_type: PageType::from_u8(page_buf[1])
            .to_inner_result("get page type from page buf")?,
    })
}

impl PageHead {
    fn get_cap(&self) -> usize {
        match self.page_type {
            PageType::RecordLeafPage => LEAF_PAGE_RECORD_CAPACITY,
            PageType::RecordNonLeafPage => NON_LEAF_PAGE_RECORD_CAPACITY,
        }
    }
}

/// The all metadata. It is stored in the index's head.
#[derive(Debug)]
struct Metadata {
    /// The root page ID.
    root_page_id: usize,
}

impl Metadata {
    fn from_head_buf(head_buf: [u8; HEAD_SIZE]) -> Self {
        Self {
            root_page_id: (head_buf[0] as usize) << 8 + head_buf[1],
        }
    }
}

/// Indexer is a struct representing the object storage's index, which maps the
/// object hash to the object's offset.
pub struct Indexer {
    /// The index file in disk.
    file: File,

    /// The metadata of the indexer.
    metadata: Metadata,

    /// The temp buffer for page.
    page_buf: [u8; PAGE_SIZE],

    /// The temp buffer for head.
    head_buf: [u8; HEAD_SIZE],
}

impl Indexer {
    fn open_or_create_rw_index_file(root_path: &PathBuf) -> Result<File, Error> {
        let file = File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(root_path.join("index"))
            .to_inner_result("open or create index data file in read-write mode")?;
        Ok(file)
    }

    /// Load the page by its page id into the field `page_buf`.
    fn load_page_by_page_id(&mut self, page_id: usize) {
        self.file.seek(std::io::SeekFrom::Start((HEAD_SIZE + page_id * PAGE_SIZE) as u64)).unwrap();
        self.file.read(&mut self.page_buf).unwrap();
    }

    fn write_page_by_page_id_and_content(&mut self, page_id: usize) {
        self.file.seek(io::SeekFrom::Start((HEAD_SIZE + page_id * PAGE_SIZE) as u64)).unwrap();
        self.file.write(&self.page_buf).unwrap();
    }

    /// Load the head by its page. The result is stored in the field `head_buf`.
    fn load_head(&mut self) {
        self.file.seek(std::io::SeekFrom::Start(0)).unwrap();
        self.file.read(&mut self.head_buf).unwrap();
    }

    /// Create a new `Indexer` by path, it will:
    ///
    ///   - Create a new index file in the path.
    ///   - Return `Indexer` itself.
    ///
    /// If there is already a index data file, use method `open` rather than
    /// me.
    pub fn create(path: &PathBuf) -> Result<Self, Error> {
        let index_file = Self::open_or_create_rw_index_file(path)?;
        let mut result = Self {
            file: index_file,
            metadata: Metadata {
                root_page_id: 0usize
            },
            page_buf: [0u8; PAGE_SIZE],
            head_buf: [0u8; HEAD_SIZE],
        };

        result.head_buf[1] = 0u8;
        result.file.write_all(&result.head_buf).unwrap();

        Ok(result)
    }

    /// Open a `Index` by path from a existing index data file.
    pub fn open(path: &PathBuf) -> Result<Self, Error> {
        let index_file = Self::open_or_create_rw_index_file(path)?;
        let mut result = Self {
            file: index_file,
            metadata: Metadata {
                root_page_id: 0usize
            },
            page_buf: [0u8; PAGE_SIZE],
            head_buf: [0u8; HEAD_SIZE],
        };

        result.load_head();
        result.metadata = Metadata::from_head_buf(result.head_buf);

        Ok(result)
    }

    /// Put a new record: a mapping from hash to the offset in data file.
    /// 
    /// See method `get` as well.
    pub fn put(&mut self, hash: &str, offset: u64) -> Result<(), Error> {
        // Turn hash and offset into bytes.
        let hash = hash_string_to_bytes(hash);
        let offset = offset_usize_to_bytes(offset as usize);

        // Try to get the page of the record to put (or insert).
        let root_page_id = self.metadata.root_page_id;
        self.load_page_by_page_id(root_page_id);
        let page_head = get_page_head(&self.page_buf)
            .to_inner_result("get page head from a page")?;
        let page_cap = page_head.get_cap();
        assert!((page_head.records_length as usize) < page_cap);
        assert!(page_head.page_type == PageType::RecordLeafPage);

        // Try to insert a record into the leaf page.
        let mut tmp: u8 = 0;
        let mut shift_index: usize = 0;
        let mut ok_to_shift: bool = false;
        let mut need_to_update: bool = true;
        for record_index in 0..(page_head.records_length as usize) {
            let offset = LEAF_PAGE_HEAD_SIZE + RECORD_OFFSET_ID_SIZE * record_index;
            let record_offset_id = self.page_buf[offset] as usize;
            let record_offset = PAGE_SIZE - (record_offset_id + 1) * LEAF_PAGE_RECORD_SIZE;
            let record_hash: [u8; HASH_LENGTH] = self.page_buf[record_offset..record_offset+HASH_LENGTH].try_into().unwrap();
            if record_hash > hash {
                ok_to_shift = true;
                tmp = record_offset_id as u8;
                self.page_buf[offset] = page_head.records_length;
                shift_index = record_index + 1;
                break;
            } else if record_hash == hash {
                need_to_update = false;
                break;
            }
        }
        if !need_to_update {
            return Ok(());
        }

        if ok_to_shift {
            for record_index in shift_index..=(page_head.records_length as usize) {
                let offset = LEAF_PAGE_HEAD_SIZE + RECORD_OFFSET_ID_SIZE * record_index;
                swap(&mut tmp, &mut self.page_buf[offset]);
            }
        } else {
            let offset = LEAF_PAGE_HEAD_SIZE + RECORD_OFFSET_ID_SIZE * (page_head.records_length as usize);
            self.page_buf[offset] = page_head.records_length;
        }
        let record_offset = PAGE_SIZE - (page_head.records_length as usize + 1) * LEAF_PAGE_RECORD_SIZE;
        for i in 0..HASH_LENGTH {
            self.page_buf[record_offset+i] = hash[i];
        }
        for i in 0..OFFSET_LENGTH {
            self.page_buf[record_offset+HASH_LENGTH+i] = offset[i];
        }
        self.page_buf[0] += 1;

        // Try to write to index file
        self.write_page_by_page_id_and_content(root_page_id);

        Ok(())
    }

    /// Get the offset in the data file by the hash.
    pub fn get(&mut self, hash: &str) -> Result<Option<u64>, Error> {
        // Turn hash into bytes.
        let hash = hash_string_to_bytes(hash);

        // Try to get the page of the record to get.
        let root_page_id = self.metadata.root_page_id;
        self.load_page_by_page_id(root_page_id);
        let page_head = get_page_head(&self.page_buf)
            .to_inner_result("get page head from a page")?;
        let page_cap = page_head.get_cap();
        assert!((page_head.records_length as usize) < page_cap);
        assert!(page_head.page_type == PageType::RecordLeafPage);

        for record_index in 0..(page_head.records_length as usize) {
            let offset = LEAF_PAGE_HEAD_SIZE + RECORD_OFFSET_ID_SIZE * record_index;
            let record_offset_id = self.page_buf[offset] as usize;
            let record_offset = PAGE_SIZE - (record_offset_id + 1) * LEAF_PAGE_RECORD_SIZE;
            let record_hash: [u8; HASH_LENGTH] = self.page_buf[record_offset..record_offset+HASH_LENGTH].try_into().unwrap();
            let record_data_offset: [u8; OFFSET_LENGTH] = self.page_buf[record_offset+HASH_LENGTH..record_offset+HASH_LENGTH+OFFSET_LENGTH].try_into().unwrap();
            if record_hash == hash {
                return Ok(Some(offset_bytes_to_usize(record_data_offset) as u64));
            }
        }

        Ok(None)
    }
}
