use std::io::{ErrorKind, Write};
use std::{fs::File, collections::HashMap, path::PathBuf, io::Read};

use crate::error::Error;
use crate::utils::{offset_bytes_to_usize, offset_usize_to_bytes, hash_bytes_to_string, hash_string_to_bytes};
use crate::try_or_return_error;
use crate::{HASH_LENGTH, OFFSET_LENGTH};

pub struct Indexer {
    data: File,
    offset: HashMap<String, u64>,
}

impl Indexer {
    fn open_or_create_rw_data(root_path: &PathBuf) -> Result<File, Error> {
        let file = try_or_return_error!(
            File::options()
                .write(true)
                .read(true)
                .create(true)
                .open(root_path.join("index")),
            "open or create index data file in read-write mode"
        );
        Ok(file)
    }

    /// Create a new `Indexer` by path, it will:
    ///
    ///   - Create a new index data file in the path.
    ///   - Return `Indexer` itself.
    ///
    /// If there is already a index data file, use method `open` rather than
    /// me.
    pub fn create(path: &PathBuf) -> Result<Self, Error> {
        let data = Self::open_or_create_rw_data(path)?;

        Ok(Self {
            data: data,
            offset: HashMap::new(),
        })
    }

    pub fn open(path: &PathBuf) -> Result<Self, Error> {
        let data = Self::open_or_create_rw_data(path)?;
        let mut result = Self {
            data: data,
            offset: HashMap::new(),
        };

        loop {
            let mut hash = [0u8; HASH_LENGTH];
            match result.data.read_exact(&mut hash) {
                Ok(_) => (),
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(Error::new(&format!(
                        "read data to build offset hashmap: get size: {e}"
                    )))
                }
            }
            let hash = hash_bytes_to_string(&hash);

            let mut offset = [0u8; OFFSET_LENGTH as usize];
            match result.data.read_exact(&mut offset) {
                Ok(_) => (),
                Err(e) => {
                    return Err(Error::new(&format!(
                        "read data to build offset hashmap: get size: {e}"
                    )))
                }
            }
            let offset = offset_bytes_to_usize(offset);

            result.offset.insert(hash, offset as u64);
        }

        Ok(result)
    }

    pub fn put(&mut self, hash: &str, offset: u64) -> Result<(), Error> {
        self.offset.insert(hash.to_string(), offset);

        let mut record = [0u8; HASH_LENGTH + OFFSET_LENGTH];
        let hash = hash_string_to_bytes(hash);
        let offset = offset_usize_to_bytes(offset as usize);
        for i in 0..(HASH_LENGTH) {
            record[i] = hash[i];
        }
        for i in 0..(OFFSET_LENGTH) {
            record[HASH_LENGTH + i] = offset[i];
        }

        try_or_return_error!(self.data.write(&record), "write new record");

        Ok(())
    }

    pub fn get(&self, hash: &str) -> Option<&u64> {
        let result = self.offset.get(hash);
        result
    }
}
