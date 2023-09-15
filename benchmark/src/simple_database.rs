use std::{path::PathBuf, io, fs};

use waste_island::Database;

pub struct SimpleDatabase {
    path: PathBuf,
}

impl SimpleDatabase {
    pub fn new(path: &PathBuf) -> Self {
        Self { path: path.clone() }
    }

    pub fn get(&self, hash: &str) -> Result<Vec<u8>, io::Error> {
        let content_path = self.path.join(hash);
        Ok(fs::read(content_path)?)
    }

    pub fn put(&self, content: &[u8]) -> Result<String, io::Error> {
        let hash = Database::gen_waste_hash(content);
        fs::write(self.path.join(&hash), content)?;
        Ok(hash)
    }
}