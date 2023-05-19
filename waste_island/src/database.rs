use std::{
    path::{PathBuf, Path}, fs, io::{Seek, Write, SeekFrom, Read},
};

use sha256::digest;

use crate::{indexer::Indexer, Error, error::ToInnerResult, offset::Offset, testutils::PictureCache};

pub struct Database {
    path: PathBuf,
    data: fs::File,
    indexer: Indexer,
}

impl Database {
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

    /// Create or open a new database at the given path.
    /// 
    /// An error will be raised if the path is not an empty folder, as
    /// attemping to create a new database in a non-empty folder may mess the
    /// folder up.
    pub fn new<P>(database_path: P) -> Result<Database, Error>
    where
        P: AsRef<Path>,
    {
        let database_path = PathBuf::from(database_path.as_ref());

        fs::create_dir_all(&database_path)
            .to_inner_result(&format!("create database directory {:?}", database_path))?;

        Ok(Database {
            data: Self::open_data(&database_path).to_inner_result("open data file")?,
            indexer: Indexer::open(&database_path).to_inner_result("open indexer")?,
            path: database_path,
        })
    }

    pub fn list(&mut self) -> Result<Vec<String>, Error> {
        self.indexer.list()
    }

    pub fn put(&mut self, data: &[u8]) -> Result<String, Error> {
        let hash = Self::gen_waste_hash(data);

        let offset = self.data.stream_position().to_inner_result("get waste's offset")?;
        self.data.write(&Offset::new(data.len() as u64).to_bytes())
            .to_inner_result("write waste's length")?;
        self.data.write_all(data).to_inner_result("write waste's data")?;

        self.indexer.put(&hash, offset)?;

        Ok(hash)
    }

    pub fn get(&mut self, hash: &str) -> Result<Vec<u8>, Error> {
        let offset = self.indexer.get(hash).to_inner_result("get offset by hash")?;
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
    use rand::{self, seq::SliceRandom};

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

        let mut database = Database::new(database_path).unwrap();
        let waste_hash = database.put(b"hello world").unwrap();
        let waste2_hash = database.put(b"hello world again").unwrap();
        assert_eq!(database.get(&waste_hash).unwrap(), b"hello world");
        assert_eq!(database.get(&waste2_hash).unwrap(), b"hello world again");
    }

    #[test]
    fn it_works_on_large_data() {
        let database_path = "/tmp/waste-land.skogatt.org/it-works-on-large-data";
        clean_up(database_path);

        let mut database = Database::new(database_path).unwrap();
        let size = 30000;
        let cache = PictureCache::new(size);

        let mut step = 0;
        let mut bef_step = 0;
        for p in &cache.data_pathes {
            step += 1;
            if step * 100 / cache.data_hashes.len() != bef_step {
                eprintln!("{} / 100", step * 100 / cache.data_hashes.len());
                bef_step += 1;
            }

            let content = fs::read(p).unwrap();
            database.put(&content).unwrap();
        }

        for step in 0..100 {
            eprintln!("{} / 100", step);
            for h in &cache.data_hashes[size / 100 * step .. size / 100 * (step + 1)] {
                let mut database = Database::new(database_path).unwrap();
                database.get(h).unwrap();
            }
        }
    }

    #[test]
    fn it_works_even_after_reopen() {
        let database_path = "/tmp/waste-land.skogatt.org/it-works-even-after-reopen";
        clean_up(database_path);

        let mut database = Database::new(database_path).unwrap();
        let hash1 = database.put(b"this is a content number 1.").unwrap();
        let hash2 = database.put(b"this is a content number 2.").unwrap();

        let mut database = Database::new(database_path).unwrap();
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
