use std::{fs, path::PathBuf};

use waste_island::Database;

pub struct PictureCache {
    /// Cache data (yes, it is a picture) files' hashes.
    pub data_hashes: Vec<String>,

    /// Cache data (yes, it is a picture) files' paths.
    pub data_pathes: Vec<PathBuf>,
}

impl PictureCache {
    pub fn new(path: &PathBuf) -> PictureCache {
        let picture_cache_path = path;
        fs::create_dir_all(&picture_cache_path).unwrap();

        let mut data_pathes = vec![];
        for i in 0..64 {
            let pic_path = picture_cache_path.join(format!("pic_{i}.jpg"));
            data_pathes.push(pic_path.clone());
            if pic_path.exists() {
                continue;
            }

            let resp =
                reqwest::blocking::get(format!("https://cataas.com/cat/says/random {i}")).unwrap();
            fs::write(pic_path, resp.bytes().unwrap()).unwrap();
        }

        let mut data_hashes = vec![];
        for f in fs::read_dir(&picture_cache_path).unwrap() {
            let f_path = f.unwrap().path();
            let content = fs::read(&f_path).unwrap();
            let hash = Database::gen_waste_hash(&content);
            data_hashes.push(hash);
        }

        PictureCache {
            data_hashes: data_hashes,
            data_pathes: data_pathes,
        }
    }
}