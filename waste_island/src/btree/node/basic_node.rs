use std::{cmp::{min, max}, marker::PhantomData, mem::size_of, fmt::Debug};

use crate::{btree::page::{PAGE_SIZE, Page, PageId}, debug};

#[derive(Clone)]
pub struct BasicNode<H, K, V>
where
    H: Copy,
    K: Debug + Copy,
    V: Debug + Copy,
{
    page: Page,

    _extra_hdr: PhantomData<H>,
    _kv: PhantomData<Record<K, V>>,
}

#[derive(Debug, Clone)]
#[repr(C)]
pub struct Record<K, V> where K: Debug, V: Debug {
    pub key: K,
    pub value: V,
}

pub struct BasicNodeIter<'a, H, K, V>
where
    H: Copy,
    K: Debug + Copy,
    V: Debug + Copy,
{
    node:  &'a BasicNode<H, K, V>,
    record_id_offset: Offset,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub union BasicNodePageWrapper<H>
where
    H: Copy,
{
    buf: [u8; PAGE_SIZE],
    hdr: BasicNodeHdr<H>,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct BasicNodeHdr<H>
where
    H: Copy,
{
    org_hdr: H,

    records_length: u8,
    first_free_record_id: RecordId,
}

/// The ID of the record. It is started from 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordId(u8);

/// The offset in the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct Offset(usize);

/// A linked list to hold all free space for record.
///
/// If it do not have next list node, the `next` should be `RecordId::invalid()`.
#[derive(Clone, Copy)]
pub struct FreeRecord {
    pub length: u8,
    pub next: RecordId,
}

impl<H, K, V> BasicNode<H, K, V>
where
    H: Copy,
    K: PartialOrd + Copy + Debug,
    V: Copy + Debug,
{
    /// The size of the page head.
    const PAGE_HEAD_SIZE: usize = size_of::<BasicNodeHdr<H>>();

    /// The size of the record.
    const RECORD_SIZE: usize = size_of::<Record<K, V>>();

    /// Get the node view of the page.
    ///
    /// # Safety
    ///
    /// - The page maybe is not valid. If the page is not even inited, call
    ///   `init` to init the inner page.
    /// - It can only at most store 255 records. So make sure the K and V's
    ///   sizes are not too small to avoid watsing space. Don't your mom tell
    ///   you do not waste anything?
    /// - Make sure that `size_of::<Record<K, V>> >= size_of::<FreeRecord>(2 bytes)`.
    pub unsafe fn new_unchecked(page: Page) -> Self {
        Self {
            page,
            _extra_hdr: PhantomData,
            _kv: PhantomData,
        }
    }

    /// Init self as zero-lengthed node.
    ///
    /// # Safety
    ///
    /// Remember to use `make_dirty` and sync.
    pub unsafe fn init(&mut self) {
        let page_hdr = &mut self.mut_page_wrapper().hdr;

        let first_free_record_id = RecordId::new(0);
        page_hdr.first_free_record_id = first_free_record_id;
        page_hdr.records_length = 0;

        let cap = self.cap() as u8;
        let free_record = self.mut_free_record(first_free_record_id);
        free_record.next = RecordId::invalid();
        free_record.length = cap;
    }

    pub fn page_id(&self) -> PageId {
        self.page.id()
    }

    /// Put a new record.
    ///
    /// # Safety
    ///
    /// - It is your duty to make sure it is not full: maybe `is_full()` can help you.
    /// - Remember to use `make_dirty` and sync.
    pub unsafe fn put(&mut self, key: &K, value: &V) {
        let new_record_id_offset = self.lower_bound(key);
        if new_record_id_offset != self.record_id_offset_right() {
            // If the record we need to put is not the biggest element, then we
            // should check if it is necessary to insert a new record or can we
            // just update the origin record having the same key.
            let record_id = self.record_id_by_offset(new_record_id_offset);
            let record = self.mut_record(*record_id);
            if &record.key == key {
                // And it is already existing... So we just update it and no
                // more insert!
                record.value = *value;
                return;
            }
        }

        // Now we need to insert it...
        let (new_record_id, new_record) = self.alloc_new_record();
        new_record.key = *key;
        new_record.value = *value;
        self.insert_new_record_id(new_record_id, new_record_id_offset);
        self.mut_page_wrapper().hdr.records_length += 1;
    }

    /// Get the value by key.
    pub fn get(&self, key: &K) -> Option<V> {
        let record_id_offset = self.lower_bound(key);
        if record_id_offset == self.record_id_offset_right() {
            return None;
        }

        let record_id = unsafe { self.record_id_by_offset(record_id_offset) };
        let record = unsafe { self.record(*record_id) };
        if &record.key == key {
            Some(record.value)
        } else {
            #[cfg(test)]
            {
                eprintln!("BEGIN");
                for r in self.into_iter() {
                    eprintln!("    {:?}", r);
                }
                eprintln!("END");
            }

            None
        }
    }

    /// Get by the lower bound.
    pub fn get_lower_bound(&self, key: &K) -> Option<V> {
        let record_id_offset = self.lower_bound(key);
        if record_id_offset == self.record_id_offset_right() {
            return None;
        }

        let record_id = unsafe { self.record_id_by_offset(record_id_offset) };
        let record = unsafe { self.record(*record_id) };
        Some(record.value)
    }

    /// Get by the lower bound.
    pub fn get_lower_bound_record(&self, key: &K) -> Option<&Record<K, V>> {
        let record_id_offset = self.lower_bound(key);
        if record_id_offset == self.record_id_offset_right() {
            return None;
        }

        let record_id = unsafe { self.record_id_by_offset(record_id_offset) };
        let record = unsafe { self.record(*record_id) };
        Some(record)
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
    pub unsafe fn split(&mut self, rhs: &mut BasicNode<H, K, V>) {
        let to_shift_records_len = self.len() / 2;
        for _ in 0..to_shift_records_len {
            self.shift_rightest_record(rhs);
        }
    }

    /// Get the rightest record.
    /// 
    /// # Safety
    /// 
    /// - You should make sure that it is not empty.
    pub unsafe fn rightest_record(&self) -> &Record<K, V> {
        debug_assert!(!self.is_empty());

        let record_id = self.record_id_by_offset(self.record_id_offset_right().offset(-1));
        self.record(*record_id)
    }

    /// Pop the rightest record.
    /// 
    /// # Safety
    /// 
    /// - You should make sure that it is not empty.
    pub unsafe fn pop_righest_record(&mut self) -> Record<K, V> {
        debug_assert!(!self.is_empty());

        let rightest_record_id_offset = self.record_id_offset_right().offset(-1);
        let record_id = self.record_id_by_offset(rightest_record_id_offset);
        let record = (*self.record(*record_id)).clone();

        self.dealloc_record(rightest_record_id_offset);
        record
    }

    /// Shift a rightest record from `self` to `rhs`.
    ///
    /// # Safety
    ///
    /// - It is your duty to make sure `rhs` is not full: maybe `is_full()` can
    ///   help you.
    /// - It is also your duty to make sure `self` is not empty: maybe
    ///   `is_empty()` can help you.
    /// - Remember to use `make_dirty` and sync - both `self` and `rhs`.
    unsafe fn shift_rightest_record(&mut self, rhs: &mut BasicNode<H, K, V>) {
        assert!(!self.is_empty());
        assert!(!rhs.is_full());

        let rightest_record_id_offset = self.record_id_offset_right().offset(-1);
        let record_id = self.record_id_by_offset(rightest_record_id_offset);
        let record = self.record(*record_id);

        rhs.put(&record.key, &record.value);
        self.dealloc_record(rightest_record_id_offset);
    }

    /// The length of the node - or how many records in the node. Tht length is
    /// less than `u8::MAX`(255) because it only use 1 byte to store the length.
    pub fn len(&self) -> usize {
        unsafe { self.page_wrapper().hdr }.records_length as usize
    }

    /// The capacity of the node. Tht capacity is less than `u8::MAX`(255) because
    /// it only use 1 byte to store the length.
    pub fn cap(&self) -> usize {
        let cap = (PAGE_SIZE - Self::PAGE_HEAD_SIZE) / (Self::RECORD_SIZE + size_of::<RecordId>());
        min(cap, u8::MAX as usize)
    }

    /// Is it full? Is it `len() == cap()`?
    pub fn is_full(&self) -> bool {
        self.len() == self.cap()
    }

    /// Is it empty? Is it `len() == 0`?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert the ID of new record at the offset.
    ///
    /// It will:
    ///
    /// - shift the bytes located in `offset .. self.record_id_offset_right()`.
    /// - Insert the new record at the location which the offset point to. ❤
    ///
    /// # Safety
    ///
    /// - It is your duty to make sure the offset is valid.
    /// - Make sure you will call `make_dirty` and sync.
    unsafe fn insert_new_record_id(&mut self, record_id: RecordId, offset: Offset) {
        let record_id_offset_right = self.record_id_offset_right();
        let page = self.mut_page_wrapper();

        // Step 1: Shift those right bytes.
        for i in (offset.raw()..record_id_offset_right.raw()).rev() {
            let i = Offset::new(i);
            *page.mut_ptr_by_offset(i.offset(1)) = *page.mut_ptr_by_offset(i);
        }

        // Step 2: Insert the record ID.
        *page.mut_ptr_by_offset(offset) = record_id.raw();
    }

    /// Point to the first of the record IDs.
    fn record_id_offset_left(&self) -> Offset {
        Offset::new(Self::PAGE_HEAD_SIZE)
    }

    /// Point to the next element after the last of the record IDs.
    fn record_id_offset_right(&self) -> Offset {
        Offset::new(Self::PAGE_HEAD_SIZE + unsafe { self.page_wrapper().hdr }.records_length as usize)
    }

    /// Return a offset, which pointer to the record ID of lower bound by key.
    fn lower_bound(&self, key: &K) -> Offset {
        let mut left = self.record_id_offset_left();
        let mut right = self.record_id_offset_right();
        while left != right {
            let mid = Offset::mid(left, right);
            let mid_record_id = unsafe { self.record_id_by_offset(mid) };
            let mid_record = unsafe { self.record(*mid_record_id) };
            // debug!(left, right, mid_record, key);
            if key <= &mid_record.key {
                right = mid;
            } else {
                left = mid.offset(1);
            }
        }
        left
    }

    /// Alloc a new space to contain a record. It will return the ID of the
    /// record and a mutable reference pointing to it.
    ///
    /// # Safety
    ///
    /// - Maybe there is no more space to alloc.
    /// - Remember to use `make_dirty` and sync.
    /// - Use `insert_record_id` to store the ID of new record.
    unsafe fn alloc_new_record(&mut self) -> (RecordId, &mut Record<K, V>) {
        let page = self.mut_page_wrapper();
        let first_free_record_id = page.hdr.first_free_record_id;
        debug_assert_ne!(first_free_record_id, RecordId::invalid());
        let free_record = {
            let ptr = page.mut_ptr_by_offset(Self::record_page_offset(first_free_record_id));
            &mut *(ptr as *mut FreeRecord)
        };

        if free_record.length == 1 {
            // Let the free record point to the next free record.
            let origin_first_free_record_id = first_free_record_id;
            page.hdr.first_free_record_id = free_record.next;
            let record = &mut *(free_record as *mut FreeRecord as *mut Record<K, V>);
            (origin_first_free_record_id, record)
        } else {
            free_record.length -= 1;
            let record_id = first_free_record_id.offset(free_record.length as isize);
            (record_id, self.mut_record(record_id))
        }
    }

    /// Dealloc record by the offset point to its record ID.
    /// 
    /// # Safety
    /// 
    /// - Make sure the `record_id_offset` point to a valid record ID.
    unsafe fn dealloc_record(&mut self, record_id_offset: Offset) {
        debug_assert!(record_id_offset >= self.record_id_offset_left());
        debug_assert!(record_id_offset < self.record_id_offset_right());

        // Put the record into free record linked list.
        let record_id = *self.record_id_by_offset(record_id_offset);
        let first_free_record_id = self.page_wrapper().hdr.first_free_record_id;
        let free_record = self.mut_free_record(record_id);
        free_record.length = 1;
        free_record.next = first_free_record_id;
        self.mut_page_wrapper().hdr.first_free_record_id = record_id;

        // Shift the righter record IDs.
        let right = self.record_id_offset_right().raw();
        let page = self.mut_page_wrapper();
        for i in record_id_offset.raw() .. right {
            let i = Offset::new(i);
            *page.mut_ptr_by_offset(i) = *page.mut_ptr_by_offset(i.offset(1));
        }
        page.hdr.records_length -= 1;
    }

    /// Get the page wrapper.
    pub fn page_wrapper(&self) -> &BasicNodePageWrapper<H> {
        let page_buf = self.page.buf();
        unsafe { &*(page_buf as *const [u8; PAGE_SIZE] as *const BasicNodePageWrapper<H>) }
    }

    /// Get the mutable reference of the page wrapper.
    /// 
    /// # Unsafe
    /// 
    /// Do not modify the page... Unless you will call `make_dirty` and sync it.
    pub unsafe fn mut_page(&mut self) -> &mut Page {
        &mut self.page
    }

    /// Make the inner page dirty.
    pub fn make_dirty(&mut self) {
        self.page.make_dirty()
    }

    /// Get the page.
    ///
    /// # Safety
    ///
    /// If the inner page is changed, remember to use `make_dirty` and sync.
    pub unsafe fn mut_page_wrapper(&mut self) -> &mut BasicNodePageWrapper<H> {
        let page_buf = self.page.mut_buf();
        &mut *(page_buf as *mut [u8; PAGE_SIZE] as *mut BasicNodePageWrapper<H>)
    }

    /// Get the record's offset in page - just by its ID.
    fn record_page_offset(id: RecordId) -> Offset {
        Offset::new(PAGE_SIZE - Self::RECORD_SIZE * (id.raw() as usize + 1))
    }

    /// Get the free record by its ID.
    ///
    /// # Safety
    ///
    /// - If you change the free record, remember to use `make_dirty` and sync.
    /// - The ID may is not point to the free record - so be careful.
    unsafe fn mut_free_record(&mut self, id: RecordId) -> &mut FreeRecord {
        let offset = Self::record_page_offset(id);
        &mut *(self.mut_page_wrapper().mut_ptr_by_offset(offset) as *mut FreeRecord)
    }

    /// Get the record by its ID.
    ///
    /// # Safety
    ///
    /// - If you change the free record, remember to use `make_dirty` and sync.
    /// - The ID may is not point to the valid record - so be careful.
    unsafe fn mut_record(&mut self, id: RecordId) -> &mut Record<K, V> {
        let offset = Self::record_page_offset(id);
        self.mut_record_by_offset(offset)
    }

    /// Get the record by its ID.
    ///
    /// # Safety
    ///
    /// - The ID may is not point to the valid record - so be careful.
    unsafe fn record(&self, id: RecordId) -> &Record<K, V> {
        let offset = Self::record_page_offset(id);
        self.record_by_offset(offset)
    }

    /// Get the record by its offset.
    ///
    /// # Safety
    ///
    /// - The offset may is not point to the valid record - so be careful.
    unsafe fn record_by_offset(&self, offset: Offset) -> &Record<K, V> {
        &*(self.page_wrapper().ptr_by_offset(offset) as *const Record<K, V>)
    }

    /// Get the record by its offset.
    ///
    /// # Safety
    ///
    /// - If you change the free record, remember to use `make_dirty` and sync.
    /// - The offset may is not point to the valid record - so be careful.
    unsafe fn mut_record_by_offset(&mut self, offset: Offset) -> &mut Record<K, V> {
        &mut *(self.mut_page_wrapper().mut_ptr_by_offset(offset) as *mut Record<K, V>)
    }

    /// Get the record ID by its offset in the page.
    ///
    /// # Safety
    ///
    /// It is your duty to make sure the offset is right and it point to a
    /// valid record ID.
    unsafe fn record_id_by_offset(&self, offset: Offset) -> &RecordId {
        debug_assert!(
            offset >= self.record_id_offset_left() && offset < self.record_id_offset_right()
        );
        &*(self.page_wrapper().ptr_by_offset(offset) as *const RecordId)
    }
}

impl<'a, H, K, V> IntoIterator for &'a BasicNode<H, K, V>
where
    H: Copy,
    K: PartialOrd + Copy + Debug,
    V: Copy + Debug,
{
    type Item = &'a Record<K, V>;
    type IntoIter = BasicNodeIter<'a, H, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter::new(self)
    }
}

impl<'a, H, K, V> BasicNodeIter<'a, H, K, V>
where
    H: Copy,
    K: PartialOrd + Copy + Debug,
    V: Copy + Debug,
{
    fn new(node: &'a BasicNode<H, K, V>) -> Self {
        Self {
            node,
            record_id_offset: node.record_id_offset_left(),
        }
    }
}

impl<'a, H, K, V> Iterator for BasicNodeIter<'a, H, K, V>
where
    H: Copy,
    K: PartialOrd + Copy + Debug,
    V: Copy + Debug,
{
    type Item = &'a Record<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.record_id_offset == self.node.record_id_offset_right() {
            return None;
        }
        let record_id = unsafe { self.node.record_id_by_offset(self.record_id_offset) };
        let record = unsafe { self.node.record(*record_id) };
        self.record_id_offset = self.record_id_offset.offset(1);
        Some(record)
    }
}

impl<H> BasicNodePageWrapper<H>
where
    H: Copy,
{
    /// Get the raw pointer that point to byte at the offset in the buffer.
    ///
    /// # Safety
    ///
    /// - If you change the buffer, remember to use `make_dirty`.
    /// - The offset may is not valid or it does not point to a valid value.
    unsafe fn mut_ptr_by_offset(&mut self, offset: Offset) -> *mut u8 {
        unsafe { self.buf.as_mut_ptr().add(offset.raw()) }
    }

    /// Get the raw pointer that point to byte at the offset in the buffer.
    ///
    /// # Safety
    ///
    /// - The offset may is not valid or it does not point to a valid value.
    unsafe fn ptr_by_offset(&self, offset: Offset) -> *const u8 {
        unsafe { self.buf.as_ptr().add(offset.raw()) }
    }

    // Get the mutable reference point to the `H`.
    pub fn mut_hdr(&mut self) -> &mut H {
        unsafe { &mut self.hdr.org_hdr }
    }

    // Get the reference point to the `H`.
    pub fn hdr(&self) -> &H {
        unsafe { &self.hdr.org_hdr }
    }
}

impl RecordId {
    pub fn new(value: u8) -> Self {
        Self(value)
    }

    pub fn raw(&self) -> u8 {
        self.0
    }

    pub fn offset(self, offset: isize) -> Self {
        Self::new((self.raw() as isize + offset) as u8)
    }

    pub fn invalid() -> Self {
        RecordId(u8::MAX)
    }
}

impl Offset {
    pub fn new(value: usize) -> Self {
        Self(value)
    }

    /// Self((l.raw() + r.raw()) / 2)
    pub fn mid(l: Self, r: Self) -> Self {
        Self((l.raw() + r.raw()) / 2)
    }

    pub fn offset(self, offset: isize) -> Self {
        Self::new((self.raw() as isize + offset) as usize)
    }

    pub fn raw(&self) -> usize {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::btree::page::{Page, PageId};

    use super::*;

    #[test]
    fn it_works() {
        let page = unsafe { Page::new_uninited(PageId::new(114)) };

        // Init the node.
        let mut node: BasicNode<(), u8, u8> = unsafe { BasicNode::new_unchecked(page) };
        unsafe {
            node.init();
        }

        // Put and get! Cool, alright.
        println!("{}", node.cap());
        for i in 0..node.cap() {
            let i = i as u8;
            assert_eq!(node.get(&i), None);
            unsafe {
                node.put(&i, &i);
            }
            assert_eq!(node.get(&i), Some(i));
        }

        assert_eq!(node.len(), node.cap())
    }

    #[test]
    fn we_can_split_node() {
        let page1 = unsafe { Page::new_uninited(PageId::new(114)) };
        let page2 = unsafe { Page::new_uninited(PageId::new(514)) };

        let mut node1: BasicNode<(), u64, u64> = unsafe { BasicNode::new_unchecked(page1) };
        let mut node2: BasicNode<(), u64, u64> = unsafe { BasicNode::new_unchecked(page2) };

        unsafe { node1.init(); }
        unsafe { node2.init(); }

        let length = 5;
        for i in 0..length {
            let i = i as u64;
            unsafe { node1.put(&i, &i) };
        }
        unsafe { node1.split(&mut node2); }
        assert_eq!(node1.len() + node2.len(), length);
        assert!(node1.len() <= (length + 1) / 2);
        assert!(node2.len() <= (length + 1) / 2);

        // Very sure that all keys in node1 shoule be less than node2.
        let node1_max_key = (&node1).into_iter().fold(u64::MIN, |a, b| min(a, b.key));
        let node2_min_key = (&node2).into_iter().fold(u64::MAX, |a, b| max(a, b.key));
        assert!(node1_max_key < node2_min_key);
    }
}
