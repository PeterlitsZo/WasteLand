use sha256::digest;
use std::{
    collections::HashMap,
    fs,
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str,
};

pub struct Database {
    path: PathBuf,
    data: fs::File,
    index: Indexer,
}

pub struct Indexer {
    data: fs::File,
    offset: HashMap<String, u64>,
}

#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Error {
    fn new(message: &str) -> Self {
        Error {
            message: message.to_string(),
        }
    }
}

macro_rules! try_or_return_error {
    ($result:expr, $message_prefix:expr) => {
        match $result {
            Ok(r) => r,
            Err(e) => return Err(Error::new(&format!("{}: {}", $message_prefix, e))),
        }
    };
}

const HASH_LENGTH: i32 = 32;
const OFFSET_LENGTH: i32 = 8;

fn offset_usize_to_bytes(n: usize) -> [u8; OFFSET_LENGTH as usize] {
    let mut bytes = [0u8; 8];
    for i in 0..8 {
        bytes[i] = (n >> (i * 8)) as u8;
    }
    bytes
}

fn offset_bytes_to_usize(bytes: [u8; OFFSET_LENGTH as usize]) -> usize {
    let mut n = 0usize;
    for i in 0..8 {
        n |= (bytes[i] as usize) << (i * 8);
    }
    n
}

fn hash_bytes_to_string(bytes: &[u8; HASH_LENGTH as usize]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>()
}

fn hash_string_to_bytes(s: &str) -> [u8; HASH_LENGTH as usize] {
    let mut result = [0u8; HASH_LENGTH as usize];
    for i in 0..(HASH_LENGTH as usize) {
        let byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap();
        result[i] = byte;
    }
    result
}

impl Indexer {
    fn open_or_create_rw_data(root_path: &PathBuf) -> Result<fs::File, Error> {
        let file = try_or_return_error!(
            fs::File::options()
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
            let mut hash = [0u8; HASH_LENGTH as usize];
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

    fn put(&mut self, hash: &str, offset: u64) -> Result<(), Error> {
        self.offset.insert(hash.to_string(), offset);

        let mut record = [0u8; (HASH_LENGTH + OFFSET_LENGTH) as usize];
        let hash = hash_string_to_bytes(hash);
        let offset = offset_usize_to_bytes(offset as usize);
        for i in 0..(HASH_LENGTH as usize) {
            record[i] = hash[i];
        }
        for i in 0..(OFFSET_LENGTH as usize) {
            record[HASH_LENGTH as usize + i] = offset[i];
        }

        try_or_return_error!(self.data.write(&record), "write new record");

        Ok(())
    }

    fn get(&self, hash: &str) -> Option<&u64> {
        let result = self.offset.get(hash);
        result
    }
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
