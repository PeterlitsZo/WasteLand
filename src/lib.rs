mod indexer;
mod error;
mod hash;
mod btree;
mod offset;
mod utils;

use offset::Offset;
use sha256::digest;
use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str,
};

use indexer::Indexer;
use error::{Error, ToInnerResult};

pub struct Database {
    path: PathBuf,
    data: fs::File,
    index: Indexer,
}

impl Database {
    const VERSION: &str = "waste_island.version='0.1.0'";

    /// Gen the waste hash from the content of data.
    pub fn gen_waste_hash(data: &[u8]) -> String {
        digest(data)
    }

    fn open_data(database_path: &PathBuf) -> Result<fs::File, Error> {
        let file = fs::File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(database_path.join("data"))
            .to_inner_result("open data file in write-read mode")?;
        Ok(file)
    }

    fn version_path(database_path: &PathBuf) -> PathBuf {
        database_path.join("version")
    }

    fn check_version(database_path: &PathBuf) -> Result<(), Error> {
        let mut file = fs::File::open(Self::version_path(database_path))
            .to_inner_result("open version by read-only mode")?;
        let mut version = String::new();
        file.read_to_string(&mut version).to_inner_result("read version")?;
        if version != Self::VERSION {
            return Err(Error::new("unsupported version"));
        }
        Ok(())
    }

    fn create_version(database_path: &PathBuf) -> Result<(), Error> {
        fs::write(Self::version_path(database_path), Self::VERSION)
            .to_inner_result("create version and write")?;
        Ok(())
    }

    /// Create a new database at the given path.
    /// 
    /// An error will be raised if the path is not an empty folder, as
    /// attemping to create a new database in a non-empty folder may mess the
    /// folder up.
    pub fn create<P>(database_path: P) -> Result<Database, Error>
    where
        P: AsRef<Path>,
    {
        let database_path = PathBuf::from(database_path.as_ref());
        if database_path.exists() {
            return Err(Error::new("The given path if not a empty folder"));
        }

        fs::create_dir_all(&database_path)
            .to_inner_result(&format!("create database directory {:?}", database_path))?;
        Self::create_version(&database_path)?;

        Ok(Database {
            data: Self::open_data(&database_path)?,
            index: Indexer::open(&database_path)?,
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

        let offset = self.data.stream_position().to_inner_result("get waste's offset")?;
        self.data.write(&Offset::new(data.len() as u64).to_bytes())
            .to_inner_result("write waste's length")?;
        self.data.write_all(data).to_inner_result("write waste's data")?;

        self.index.put(&hash, offset)?;

        Ok(hash)
    }

    pub fn get(&mut self, hash: &str) -> Result<Vec<u8>, Error> {
        let offset = self.index.get(hash).to_inner_result("get offset by hash")?;
        let offset = match offset {
            None => return Err(Error::new("hash not found")),
            Some(o) => o,
        };

        self.data.seek(SeekFrom::Start(offset.to_u64()))
            .to_inner_result("set offset")?;

        let mut size = [0u8; 8];
        self.data.read_exact(&mut size).to_inner_result("read size")?;
        let size = Offset::from_bytes(size).to_u64() as usize;

        let mut content = Vec::with_capacity(size);
        unsafe { content.set_len(size) };
        self.data.read_exact(&mut content).to_inner_result("read waste")?;
        Ok(content)
    }

    pub fn drop(self) -> Result<(), Error> {
        fs::remove_dir_all(&self.path)
            .to_inner_result(&format!("remove directory {}", &self.path.display()))?;
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
