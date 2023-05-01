use std::{fs::File, path::Path, rc::Rc, sync::Mutex};

use crate::{
    btree::{
        node::{get_node_type, HeadNode, LeafNode},
        page::Page,
    },
    debug,
    error::{Error, ToInnerResult},
    hash::Hash,
    offset::Offset,
};

use super::{
    node::{InternalNode, NodeType},
    page::PageId,
    pager::Pager,
};

pub struct BTree {
    pager: Pager,
    head_node: HeadNode,
}

impl BTree {
    const HEAD_PAGE_ID: PageId = PageId::new(0);

    pub fn new<P>(file_name: P) -> Result<BTree, Error>
    where
        P: AsRef<Path>,
    {
        let file = File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(file_name)
            .to_inner_result("open or create index data file in read-write mode")?;

        let mut pager = Pager::new(file).to_inner_result("create pager")?;

        if pager.len() == 0 {
            // Look like the paper need to be inited.

            let head_page = pager.append_empty_uninited_page()?;
            debug_assert_eq!(head_page.id(), Self::HEAD_PAGE_ID);
            let root_page = pager.append_empty_uninited_page()?;

            let mut head_node = unsafe { HeadNode::new_unchecked(head_page) };
            head_node.make_dirty();
            unsafe {
                head_node.init(root_page.id());
            }
            pager.sync_page(unsafe { head_node.mut_page() })?;

            let mut root_node = unsafe { LeafNode::new_unchecked(root_page) };
            root_node.make_dirty();
            unsafe {
                root_node.init();
            }
            pager.sync_page(unsafe { root_node.mut_page() })?;
            debug_assert_eq!(
                unsafe { get_node_type(root_node.mut_page()) },
                NodeType::Leaf
            );
        }

        let head_page = pager.get_page(Self::HEAD_PAGE_ID)?;
        let head_node = unsafe { HeadNode::new_unchecked(head_page) };
        if !head_node.check() {
            return Err(Error::new("the head node is not valid"));
        }

        Ok(Self { pager, head_node })
    }

    pub fn put(&mut self, key: &Hash, value: &Offset) -> Result<(), Error> {
        let root_page_id = self.head_node.hdr().root_node_page_id;
        let root_page = self.pager.get_page(root_page_id)?;

        enum InnerPut { SplitMe(Hash, PageId), Alright }
        fn inner_put(
            slf: &mut BTree,
            page: Page,
            key: &Hash,
            value: &Offset,
        ) -> Result<InnerPut, Error> {
            match get_node_type(&page) {
                NodeType::Leaf => {
                    let mut node = unsafe { LeafNode::new_unchecked(page) };

                    if node.is_full() {
                        // Split root_node into new_node.
                        let new_page = slf.pager.append_empty_uninited_page()?;
                        let mut new_node = unsafe { LeafNode::new_unchecked(new_page) };
                        unsafe { new_node.init() };
                        unsafe { node.split(&mut new_node) };
                        new_node.make_dirty();
                        node.make_dirty();
                        slf.pager.sync_page(unsafe { new_node.mut_page() })?;
                        slf.pager.sync_page(unsafe { node.mut_page() })?;

                        return Ok(
                            InnerPut::SplitMe(unsafe { *node.rightest_key() }, new_node.page_id())
                        );
                    }

                    unsafe { node.put(key, value) };
                    node.make_dirty();
                    slf.pager.sync_page(unsafe { node.mut_page() })?;
                    Ok(InnerPut::Alright)
                }
                NodeType::Internal => {
                    let mut node = unsafe { InternalNode::new_unchecked(page) };

                    if node.is_full() {
                        // Split me into two new node.
                        let new_page = slf.pager.append_empty_uninited_page()?;
                        let mut new_node = unsafe { InternalNode::new_unchecked(new_page) };
                        unsafe { new_node.init(node.hdr_mut().rightest_page_id) };
                        unsafe { node.split(&mut new_node) };

                        // Change inner struct...
                        let mid_record = unsafe { node.pop_rightest_record() };
                        unsafe { node.hdr_mut().rightest_page_id = mid_record.value };

                        slf.pager.sync_page(unsafe { new_node.mut_page() })?;
                        slf.pager.sync_page(unsafe { node.mut_page() })?;

                        return Ok(InnerPut::SplitMe(mid_record.key, new_node.page_id()));
                    }

                    let (origin_key, next_page_id) = node.get(key);
                    let next_page = slf.pager.get_page(next_page_id)?;
                    match inner_put(slf, next_page, key, value)? {
                        InnerPut::Alright => { return Ok(InnerPut::Alright); }
                        InnerPut::SplitMe(new_key, new_value) => {
                            match origin_key {
                                Some(ori_k) => {
                                    unsafe { node.put(&ori_k, &new_value) };
                                    unsafe { node.put(&new_key, &next_page_id) };
                                },
                                None => {
                                    unsafe { node.hdr_mut().rightest_page_id = new_value };
                                    unsafe { node.put(&new_key, &next_page_id) };
                                },
                            };
                            node.make_dirty();
                            let node_page = unsafe { node.mut_page() };
                            slf.pager.sync_page(&mut node_page.clone())?;
                            inner_put(slf, node_page.clone(), key, value)
                        }
                    }
                }
                typ => panic!("unexcepted node type: {:?}", typ),
            }
        }

        match inner_put(self, root_page, key, value)? {
            InnerPut::Alright => {}
            InnerPut::SplitMe(new_key, new_value) => {
                let parent_page = self.pager.append_empty_uninited_page()?;
                let mut parent_node = unsafe { InternalNode::new_unchecked(parent_page.clone()) };
                unsafe { parent_node.init(new_value) };
                unsafe { parent_node.put(&new_key, &root_page_id) }
                unsafe {
                    self.head_node.mut_hdr().root_node_page_id =
                        parent_node.page_id();
                }
                inner_put(self, parent_page, key, value)?;
            }
        };
        Ok(())
    }

    pub fn get(&mut self, key: &Hash) -> Result<Option<Offset>, Error> {
        let root_page_id = self.head_node.hdr().root_node_page_id;
        let root_page = self.pager.get_page(root_page_id)?;

        fn inner_get(slf: &mut BTree, page: Page, key: &Hash) -> Result<Option<Offset>, Error> {
            match get_node_type(&page) {
                NodeType::Leaf => {
                    let node = unsafe { LeafNode::new_unchecked(page) };
                    let result = node.get(key);
                    Ok(result)
                }
                NodeType::Internal => {
                    let node = unsafe { InternalNode::new_unchecked(page) };
                    let (_, next_page_id) = node.get(key);
                    let page = slf.pager.get_page(next_page_id)?;
                    inner_get(slf, page, key)
                }
                _ => panic!("unsupported node"),
            }
        }

        inner_get(self, root_page, key)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, path::PathBuf};

    use crate::hash::HASH_SIZE;

    use super::*;

    fn cleanup_and_create_new_btree_file(btree_filename: &str) -> PathBuf {
        let directory_path = Path::new("/tmp/waste-land/");
        if !directory_path.exists() {
            fs::create_dir(directory_path).unwrap();
        }
        let btree_path = directory_path.join(btree_filename);
        if btree_path.exists() {
            fs::remove_file(&btree_path).unwrap();
        }
        File::create(&btree_path).unwrap();
        btree_path
    }

    #[test]
    fn it_works() {
        let btree_path = cleanup_and_create_new_btree_file("it-works.btree");

        let mut btree = BTree::new(btree_path).unwrap();
        let (key1, value1) = (&Hash::from_bytes([14u8; HASH_SIZE]), Offset::new(114514));
        let (key2, value2) = (&Hash::from_bytes([21u8; HASH_SIZE]), Offset::new(63));
        btree.put(&key1, &value2).unwrap();
        btree.put(&key2, &value2).unwrap();
        btree.put(&key1, &value1).unwrap();
        assert_eq!(btree.get(key1).unwrap(), Some(value1));
        assert_eq!(btree.get(key2).unwrap(), Some(value2));
        assert_eq!(
            btree.get(&Hash::from_bytes([33u8; HASH_SIZE])).unwrap(),
            None
        );
    }

    #[test]
    fn a_simple_tree_with_internal_node() {
        let btree_path =
            cleanup_and_create_new_btree_file("a-simple-tree-with-internal-node.btree");

        let mut btree = BTree::new(btree_path).unwrap();
        let mut mem_map = HashMap::new();
        for i in 0..0xff {
            dbg!(i);
            let key = Hash::from_bytes([i; HASH_SIZE]);
            let value = Offset::new(i as u64);
            if i == 99 {
                eprintln!("在这停顿！")
            }
            btree.put(&key, &value).unwrap();
            mem_map.insert(key, value);
        }

        for (i, k) in mem_map.keys().enumerate() {
            dbg!(i);
            let v = &btree.get(k).unwrap();
            match v {
                Some(v) => { assert_eq!(v, mem_map.get(k).unwrap()) },
                None => {
                    &btree.get(k).unwrap();
                }
            }
            assert_eq!(&btree.get(k).unwrap().unwrap(), mem_map.get(k).unwrap());
        }
    }

    #[test]
    fn how_about_1e5_key_values_aha() {
        let btree_path = cleanup_and_create_new_btree_file("how-about-1e5-key-values-aha.btree");

        let mut btree = BTree::new(btree_path).unwrap();
        let mut mem_map = HashMap::new();
        for i in 0..(1e5 as usize) {
            dbg!(i);
            let key = Hash::from_bytes(rand::random::<[u8; HASH_SIZE]>());
            let value = Offset::new(rand::random::<u64>());
            btree.put(&key, &value).unwrap();
            mem_map.insert(key, value);
        }

        for (i, k) in mem_map.keys().enumerate() {
            dbg!(i);
            assert_eq!(&btree.get(k).unwrap().unwrap(), mem_map.get(k).unwrap());
        }
    }
}
