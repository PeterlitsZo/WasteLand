mod indexer;
mod error;
mod utils;

use sha256::digest;
use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str,
};

use indexer::Indexer;
use error::Error;
use utils::{HASH_LENGTH, OFFSET_LENGTH};

use crate::utils::{offset_usize_to_bytes, offset_bytes_to_usize};

pub struct Database {
    path: PathBuf,
    data: fs::File,
    index: Indexer,
}

impl Database {
    const VERSION: [u8; 1] = [0u8];

    pub fn gen_waste_hash(data: &[u8]) -> String {
        digest(data)
    }

    fn open_data(database_path: &PathBuf) -> Result<fs::File, Error> {
        let file = try_or_return_error!(
            fs::File::options()
                .write(true)
                .read(true)
                .create(true)
                .open(database_path.join("data")),
            "open data file in write-read mode"
        );
        Ok(file)
    }

    fn version_path(database_path: &PathBuf) -> PathBuf {
        database_path.join("version")
    }

    fn check_version(database_path: &PathBuf) -> Result<(), Error> {
        let mut file = try_or_return_error!(
            fs::File::open(Self::version_path(database_path)),
            "open version by read-only mode"
        );
        let mut version = [0u8; 1];
        try_or_return_error!(file.read(&mut version), "read version");
        if version != Self::VERSION {
            return Err(Error::new("unsupported version"));
        }
        Ok(())
    }

    fn create_version(database_path: &PathBuf) -> Result<(), Error> {
        try_or_return_error!(
            fs::write(Self::version_path(database_path), Self::VERSION),
            "create version and write"
        );
        Ok(())
    }

    pub fn create<P>(database_path: P) -> Result<Database, Error>
    where
        P: AsRef<Path>,
    {
        let database_path = PathBuf::from(database_path.as_ref());
        if database_path.exists() {
            return Err(Error::new("looks like there is already a database here"));
        }

        try_or_return_error!(
            fs::create_dir_all(&database_path),
            format!("create database directory {:?}", database_path)
        );
        Self::create_version(&database_path)?;

        Ok(Database {
            data: Self::open_data(&database_path)?,
            index: Indexer::create(&database_path)?,
            path: database_path,
        })
    }

    pub fn open<P>(database_path: P) -> Result<Database, Error>
    where
        P: AsRef<Path>,
    {
        let database_path = PathBuf::from(database_path.as_ref());
        let data = Self::open_data(&database_path)?;
        Self::check_version(&database_path)?;

        let database = Database {
            index: Indexer::open(&database_path)?,
            path: database_path,
            data: data,
        };

        Ok(database)
    }

    pub fn put(&mut self, data: &[u8]) -> Result<String, Error> {
        let hash = Self::gen_waste_hash(data);

        let offset = try_or_return_error!(self.data.stream_position(), "get waste's offset");
        try_or_return_error!(
            self.data.write(&offset_usize_to_bytes(data.len())),
            "write waste's length"
        );
        try_or_return_error!(self.data.write_all(data), "write waste's data");

        self.index.put(&hash, offset)?;

        Ok(hash)
    }

    pub fn get(&mut self, hash: &str) -> Result<Vec<u8>, Error> {
        let offset = self.index.get(hash);
        let offset = match offset {
            None => return Err(Error::new("not found")),
            Some(o) => o,
        };

        try_or_return_error!(self.data.seek(SeekFrom::Start(*offset)), "set offset");

        let mut size = [0u8; 8];
        try_or_return_error!(self.data.read_exact(&mut size), "read size");
        let size = offset_bytes_to_usize(size);

        let mut content = vec![0u8; size];
        try_or_return_error!(self.data.read_exact(&mut content), "read waste");
        Ok(content)
    }

    pub fn drop(self) -> Result<(), Error> {
        try_or_return_error!(
            fs::remove_dir_all(&self.path),
            format!("remove directory {}", &self.path.display())
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;

    fn clean_up(database_path: &str) {
        match fs::remove_dir_all(database_path) {
            Err(e) if e.kind() != ErrorKind::NotFound => {
                panic!("{}", e)
            }
            _ => (),
        }
    }

    #[test]
    fn it_works() {
        let database_path = "/tmp/waste-land.skogatt.org/it-works";
        clean_up(database_path);

        assert!(Database::open(database_path).is_err());
        Database::create(database_path).unwrap();
        let mut database = Database::open(database_path).unwrap();
        let waste_hash = database.put(b"hello world").unwrap();
        let waste2_hash = database.put(b"hello world again").unwrap();
        assert_eq!(database.get(&waste_hash).unwrap(), b"hello world");
        assert_eq!(database.get(&waste2_hash).unwrap(), b"hello world again");
    }

    #[test]
    fn it_works_even_after_reopen() {
        let database_path = "/tmp/waste-land.skogatt.org/it-works-even-after-reopen";
        clean_up(database_path);

        let mut database = Database::create(database_path).unwrap();
        let hash1 = database.put(b"this is a content number 1.").unwrap();
        let hash2 = database.put(b"this is a content number 2.").unwrap();

        let mut database = Database::open(database_path).unwrap();
        assert_eq!(
            database.get(&hash1).unwrap(),
            b"this is a content number 1."
        );
        assert_eq!(
            database.get(&hash2).unwrap(),
            b"this is a content number 2."
        );
    }
}
