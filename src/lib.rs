use sha256::digest;
use std::{fs, path::PathBuf, str};

pub struct Database {
    database_path: PathBuf,
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

impl Database {
    fn gen_waste_hash(data: &[u8]) -> String {
        digest(data)
    }

    fn waste_path(&self) -> PathBuf {
        self.database_path.join("waste")
    }

    fn version_path(&self) -> PathBuf {
        self.database_path.join("version")
    }

    pub fn create(database_path: &str) -> Result<Database, Error> {
        let database = Database {
            database_path: PathBuf::from(database_path),
        };

        try_or_return_error!(
            fs::create_dir_all(&database.database_path),
            format!("create database directory {:?}", database.database_path)
        );
        try_or_return_error!(
            fs::create_dir(database.waste_path()),
            format!("create waste directory {:?}", database.waste_path())
        );
        try_or_return_error!(
            fs::write(database.version_path(), b"0"),
            "create version file"
        );

        Ok(database)
    }

    pub fn open(database_path: &str) -> Result<Database, Error> {
        let database = Database {
            database_path: PathBuf::from(database_path),
        };

        let data = try_or_return_error!(fs::read(database.version_path()), "read version file");

        if data != b"0" {
            return Err(Error::new(&format!(
                "unexcepted version {}",
                String::from_utf8_lossy(&data)
            )));
        }

        Ok(database)
    }

    pub fn put(&self, data: &[u8]) -> Result<String, Error> {
        let hash = Self::gen_waste_hash(data);
        let waste_path = self.waste_path().join(&hash);
        try_or_return_error!(fs::write(waste_path, data), "write to file");
        Ok(hash)
    }

    pub fn get(&self, hash: &str) -> Result<Vec<u8>, Error> {
        let waste_path = self.waste_path().join(hash);
        let data = try_or_return_error!(fs::read(waste_path), "read from file");
        Ok(data)
    }

    pub fn drop(self) -> Result<(), Error> {
        try_or_return_error!(
            fs::remove_dir_all(&self.database_path),
            format!("remove directory {}", &self.database_path.display())
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
        let database = Database::open(database_name).unwrap();
        let waste_hash = database.put(b"hello world").unwrap();
        let waste2_hash = database.put(b"hello world again").unwrap();
        assert_eq!(database.get(&waste_hash).unwrap(), b"hello world");
        assert_eq!(database.get(&waste2_hash).unwrap(), b"hello world again");
    }
}
