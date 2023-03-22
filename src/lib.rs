use sha256::digest;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    str,
};

pub struct Database {
    path: PathBuf,
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

fn usize_to_u8_array(n: usize) -> [u8; 8] {
    let mut bytes = [0u8; 8];
    for i in 0..8 {
        bytes[i] = (n >> (i * 8)) as u8;
    }
    bytes
}

fn u8_array_to_usize(bytes: [u8; 8]) -> usize {
    let mut n = 0usize;
    for i in 0..8 {
        n |= (bytes[i] as usize) << (i * 8);
    }
    n
}

impl Database {
    fn gen_waste_hash(data: &[u8]) -> String {
        digest(data)
    }

    fn waste_path(&self) -> PathBuf {
        self.path.join("waste")
    }

    fn version_path(&self) -> PathBuf {
        self.path.join("version")
    }

    fn data_path(&self) -> PathBuf {
        self.path.join("data")
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

    pub fn create(database_path: &str) -> Result<Database, Error> {
        let database_path = PathBuf::from(database_path);
        try_or_return_error!(
            fs::create_dir_all(&database_path),
            format!("create database directory {:?}", database_path)
        );

        let data = Self::open_data(&database_path)?;

        let database = Database {
            path: database_path,
            data: data,
            offset: HashMap::new(),
        };

        try_or_return_error!(
            fs::write(database.version_path(), b"0"),
            "create version file"
        );

        Ok(database)
    }

    pub fn open(database_path: &str) -> Result<Database, Error> {
        let database_path = PathBuf::from(database_path);
        let data = Self::open_data(&database_path)?;

        let database = Database {
            path: database_path,
            data: data,
            offset: HashMap::new(),
        };

        let version_data =
            try_or_return_error!(fs::read(database.version_path()), "read version file");
        if version_data != b"0" {
            return Err(Error::new(&format!(
                "unexcepted version {}",
                String::from_utf8_lossy(&version_data)
            )));
        }

        Ok(database)
    }

    pub fn put(&mut self, data: &[u8]) -> Result<String, Error> {
        let hash = Self::gen_waste_hash(data);

        let offset = try_or_return_error!(self.data.stream_position(), "get waste's offset");
        try_or_return_error!(
            self.data.write(&usize_to_u8_array(data.len())),
            "write waste's length"
        );
        try_or_return_error!(self.data.write_all(data), "write waste's data");

        self.offset.insert(hash.clone(), offset);

        Ok(hash)
    }

    pub fn get(&mut self, hash: &str) -> Result<Vec<u8>, Error> {
        let offset = self.offset.get(hash);
        let offset = match offset {
            None => return Err(Error::new("not found")),
            Some(o) => o,
        };

        try_or_return_error!(self.data.seek(SeekFrom::Start(*offset)), "set offset");

        let mut size = [0u8; 8];
        try_or_return_error!(self.data.read_exact(&mut size), "read size");
        let size = u8_array_to_usize(size);

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

    fn clean_up(database_name: &str) {
        use std::io::ErrorKind;

        match fs::remove_dir_all(database_name) {
            Err(e) if e.kind() != ErrorKind::NotFound => {
                panic!("{}", e)
            }
            _ => (),
        }
    }

    #[test]
    fn main() {
        let database_name = "/tmp/waste-land.skogatt.org/";
        clean_up(database_name);

        assert!(Database::open(database_name).is_err());
        Database::create(database_name).unwrap();
        let mut database = Database::open(database_name).unwrap();
        let waste_hash = database.put(b"hello world").unwrap();
        let waste2_hash = database.put(b"hello world again").unwrap();
        assert_eq!(database.get(&waste_hash).unwrap(), b"hello world");
        assert_eq!(database.get(&waste2_hash).unwrap(), b"hello world again");
    }
}
