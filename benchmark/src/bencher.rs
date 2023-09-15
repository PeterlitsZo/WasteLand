use std::{
    fs::{self, create_dir_all, File},
    io::Read,
    path::PathBuf,
};

use criterion::{measurement::Measurement, BenchmarkGroup};
use rand::{rngs::ThreadRng, Rng, seq::SliceRandom};
use rocksdb::DB;
use sqlite::Value;
use waste_island::Database;
use crate::picture_cache::PictureCache;

use crate::simple_database::SimpleDatabase;

/// Get the temp path.
fn temp_path() -> PathBuf {
    PathBuf::from("/tmp/waste_island")
}

/// Create a path of empty folder by given benchmark name.
///
/// It will delete all data in the folder if there is some data existing.
pub fn benchmark_path(benchmark_name: &str) -> PathBuf {
    let result = temp_path().join("benchmark").join(benchmark_name);
    if result.exists() {
        fs::remove_dir_all(&result).unwrap();
    }
    create_dir_all(&result).unwrap();
    result
}

/// Get data from file's head.
///
/// We only get the 1 / div of total file --- It is helpful if we just want to
/// test on small file.
pub fn get_data(file_path: &PathBuf, div: u64) -> Vec<u8> {
    let mut file = File::open(file_path).unwrap();
    let len = (file.metadata().unwrap().len() / div) as usize;
    let mut content = Vec::with_capacity(len);
    unsafe { content.set_len(len) };
    file.read_exact(&mut content).unwrap();

    content
}

pub struct Bencher {
    cache: PictureCache,
    rng: ThreadRng,
}

impl Bencher {
    /// Create a new cache.
    pub fn new() -> Self {
        Self {
            cache: PictureCache::new(41000),
            rng: rand::thread_rng(),
        }
    }

    pub fn bench_waste_island<T: Measurement>(
        &mut self,
        g: &mut BenchmarkGroup<'_, T>,
        path: &str,
        size: usize,
        div: u64,
    ) {
        g.bench_function(path, |b| {
            let database_path = benchmark_path("1_put_and_99_reads");
            let mut database = Database::new(&database_path).unwrap();

            b.iter(|| {
                let mut hashes = vec![];
                for p in &self.cache.data_pathes[0..size] {
                    // Put the data.
                    let content = get_data(p, div);
                    let hash = database.put(&content).unwrap();
                    hashes.push(hash);

                    // And try to read a lot.
                    for _ in 0..99 {
                        let hidx =
                            (self.rng.gen::<f64>().sqrt() * hashes.len() as f64).floor() as usize;
                        let hash = &hashes[hidx];
                        database.get(hash).unwrap();
                    }
                }
            })
        });
    }

    pub fn bench_sqlite<T: Measurement>(
        &mut self,
        g: &mut BenchmarkGroup<'_, T>,
        path: &str,
        size: usize,
        div: u64,
    ) {
        g.bench_function(path, |b| {
            let baseline_path =
                benchmark_path("1_put_and_99_reads_baseline_sqlite").join("data.sqlite3");
            let connection = sqlite::open(baseline_path).unwrap();
            let query = "
                CREATE TABLE tests (key TEXT, value BLOB);
                CREATE INDEX tests_key ON tests (key);
            ";
            connection.execute(query).unwrap();
            b.iter(|| {
                let mut hashes = vec![];
                for p in &self.cache.data_pathes[0..size] {
                    let content = get_data(p, div);
                    let hash = Database::gen_waste_hash(&content);
                    let mut ins_stat = connection
                        .prepare("INSERT INTO tests VALUES (?, ?)")
                        .unwrap();
                    ins_stat
                        .bind::<&[(_, Value)]>(&[(1, hash.clone().into()), (2, content.into())][..])
                        .unwrap();
                    ins_stat.next().unwrap();

                    hashes.push(hash);
                    for _ in 0..99 {
                        let hidx = (self.rng.gen::<f64>().sqrt() * hashes.len() as f64).floor() as usize;
                        let hash = &hashes[hidx];
                        let mut get_stat = connection
                            .prepare("SELECT value FROM tests WHERE key = ?")
                            .unwrap();
                        get_stat.bind((1, hash.as_str())).unwrap();
                        get_stat.next().unwrap();
                    }
                }
            })
        });
    }

    pub fn bench_rocksdb<T: Measurement>(
        &mut self,
        g: &mut BenchmarkGroup<'_, T>,
        path: &str,
        size: usize,
        div: u64,
    ) {
        g.bench_function(path, |b| {
            let baseline_path = benchmark_path("1_put_and_99_reads_baseline_rocksdb");
            let db = DB::open_default(baseline_path).unwrap();

            b.iter(|| {
                let mut hashes = vec![];
                for p in &self.cache.data_pathes[0..size] {
                    let content = get_data(p, div);
                    let hash = Database::gen_waste_hash(&content);
                    db.put(&hash, &content).unwrap();
                    hashes.push(hash);
                    for _ in 0..99 {
                        let hidx = (self.rng.gen::<f64>().sqrt() * hashes.len() as f64).floor() as usize;
                        let hash = &hashes[hidx];
                        db.get(&hash).unwrap();
                    }
                }
            })
        });
    }

    pub fn bench_fs<T: Measurement>(
        &mut self,
        g: &mut BenchmarkGroup<'_, T>,
        path: &str,
        size: usize,
        div: u64,
    ) {
        g.bench_function(path, |b| {
            let baseline_path = benchmark_path("1_put_and_99_reads_baseline_fs");
            let sd = SimpleDatabase::new(&baseline_path);

            let mut avg_len = 0;
            b.iter(|| {
                avg_len = 0;
                let mut hashes = vec![];
                for p in &self.cache.data_pathes[0..size] {
                    let content = get_data(p, div);
                    avg_len += content.len() as u128;
                    let hash = sd.put(&content).unwrap();
                    hashes.push(hash);
                    for _ in 0..99 {
                        let hidx = (self.rng.gen::<f64>().sqrt() * hashes.len() as f64).floor() as usize;
                        let hash = &hashes[hidx];
                        sd.get(&hash).unwrap();
                    }
                }
                let path = self.cache.data_pathes.choose(&mut rand::thread_rng()).unwrap();
                fs::read(path).unwrap();
                avg_len = avg_len / (size as u128);
            });
            eprintln!("size: {}, avg_len: {}", size, avg_len);
        });
    }
}
