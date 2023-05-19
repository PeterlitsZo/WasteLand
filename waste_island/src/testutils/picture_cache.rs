use std::{fs, path::PathBuf, ffi::OsString};

use home::home_dir;
use crate::Database;

pub struct PictureCache {
    /// Cache data (yes, it is a picture) files' hashes.
    pub data_hashes: Vec<String>,

    /// Cache data (yes, it is a picture) files' paths.
    pub data_pathes: Vec<PathBuf>,
}

impl PictureCache {
    pub fn new(size: usize) -> PictureCache {
        let picture_cache_path = home_dir().unwrap().join("tmp/waste_land_picture_cache");
        fs::create_dir_all(&picture_cache_path).unwrap();

        if !picture_cache_path.join("SUCCESS").exists() {
            eprintln!("Can't find file named 'SUCCESS' in the {}", picture_cache_path.display());
            eprintln!("Put images into the directory, and touch a file named 'SUCCESS'");
            eprintln!();
            eprintln!("Run downloader.sh maybe can help you");
            panic!();
        }

        let mut data_hashes = vec![];
        let mut data_pathes = vec![];
        for f in fs::read_dir(&picture_cache_path).unwrap() {
            let f_path = f.unwrap().path();
            if f_path.file_name() == Some(&OsString::from("SUCCESS")) {
                continue;
            }
            let content = fs::read(&f_path).unwrap();
            let hash = Database::gen_waste_hash(&content);
            data_pathes.push(f_path);
            data_hashes.push(hash);

            if data_hashes.len() == size {
                break;
            }
        }

        assert!(size == data_hashes.len());
        PictureCache {
            data_hashes,
            data_pathes,
        }
    }
}