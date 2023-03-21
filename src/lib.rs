use std::{fs, str, path::{Path, PathBuf}};
use sha256::digest;

pub struct Database {
    database_path: PathBuf,
}

#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Database {
    fn gen_waste_hash(data: &[u8]) -> String {
        return digest(data)
    }

    fn waste_path(&self) -> PathBuf {
        self.database_path.join("waste")
    }

    fn version_path(&self) -> PathBuf {
        self.database_path.join("version")
    }

    pub fn create(database_path: &str) -> Result<Database, Error> {
        let database = Database{ database_path: PathBuf::from(database_path) };

        match fs::create_dir_all(database.waste_path()) {
            Ok(_) => (),
            Err(e) => {
                return Err(Error {
                    message: format!(
                        "create database directory {}: {}",
                        database.waste_path().display(), e
                    ),
                })
            }
        }

        match fs::write(database.version_path(), b"0") {
            Ok(_) => (),
            Err(e) => return Err(Error {
                message: format!("create version file: {}", e.to_string())
            })
        }

        Ok(database)
    }

    pub fn open(database_name: &str) -> Result<Database, Error> {
        let data = match fs::read(format!("{}/version", database_name)) {
            Ok(data) => data,
            Err(e) => return Err(Error { message: format!("read version file: {}", e.to_string()) })
        };

        if data != b"0" {
            return Err(Error { message: format!("unexcepted version {}", String::from_utf8_lossy(&data)) })
        }

        Ok(Database {
            database_path: PathBuf::from(database_name),
        })
    }

    pub fn put(&self, data: &[u8]) -> Result<String, Error> {
        let hash = Self::gen_waste_hash(data);
        let waste_path = self.waste_path().join(&hash);
        match fs::write(waste_path, data) {
            Ok(_) => Ok(hash),
            Err(e) => Err(Error{ message: format!("write to file: {}", e) })
        }
    }

    pub fn get(&self, hash: &str) -> Result<Vec<u8>, Error> {
        let waste_path = self.waste_path().join(hash);
        match fs::read(waste_path) {
            Ok(data) => Ok(data),
            Err(e) => Err(Error{ message: format!("read from file: {}", e) })
        }
    }

    pub fn drop(self) -> Result<(), Error> {
        match fs::remove_dir_all(&self.database_path) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error {
                message: format!(
                    "remove directory {}: {}",
                    &self.database_path.display(), e
                ),
            }),
        }
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
            },
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
