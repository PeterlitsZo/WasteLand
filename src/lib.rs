use std::fs::{self, File};
use std::str;
use sha256::digest;

pub struct Database {
    database_name: String,
}

#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Database {
    fn gen_waste_name(data: &[u8]) -> String {
        return digest(data)
    }

    pub fn create(database_name: &str) -> Result<Database, Error> {
        match fs::create_dir_all(format!("{}/waste", database_name)) {
            Ok(_) => (),
            Err(e) => {
                return Err(Error {
                    message: format!("create database directory {}: {}", database_name, e),
                })
            }
        }

        match fs::write(format!("{}/version", database_name), b"0") {
            Ok(_) => (),
            Err(e) => return Err(Error { message: format!("create version file: {}", e.to_string()) })
        }

        Ok(Database {
            database_name: String::from(database_name),
        })
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
            database_name: String::from(database_name),
        })
    }

    pub fn put(&self, data: &[u8]) -> Result<String, Error> {
        let waste_name = Self::gen_waste_name(data);
        let waste_path = format!("{}waste/{}", self.database_name, waste_name);
        match fs::write(waste_path, data) {
            Ok(_) => Ok(waste_name),
            Err(e) => Err(Error{ message: format!("write to file: {}", e) })
        }
    }

    pub fn get(&self, hash: &str) -> Result<Vec<u8>, Error> {
        let waste_path = format!("{}waste/{}", self.database_name, hash);
        match fs::read(waste_path) {
            Ok(data) => Ok(data),
            Err(e) => Err(Error{ message: format!("read from file: {}", e) })
        }
    }

    pub fn drop(self) -> Result<(), Error> {
        match fs::remove_dir_all(&self.database_name) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error {
                message: format!("remove directory {}: {}", &self.database_name, e),
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
