mod simple_database;

use std::{fs::{self, File}, path::PathBuf, io::Read};

use criterion::{criterion_group, criterion_main, Criterion};
use rand::{self, seq::SliceRandom};

use waste_island::{ Database, __Test_PictureCache as PictureCache };
use simple_database::SimpleDatabase;

fn temp_path() -> PathBuf {
    PathBuf::from("/tmp/waste_island")
}

fn benchmark_path(benchmark_name: &str) -> PathBuf {
    temp_path().join("benchmark").join(benchmark_name)
}

fn bench_1_put_and_99_reads(c: &mut Criterion) {
    let cache = PictureCache::new(41000);
    let database_path = benchmark_path("1_put_and_99_reads");

    // Start benchmark
    let mut group = c.benchmark_group("1_put_and_99_reads");
    group.sample_size(10);

    for size in [10, 100, 1000, 10000, 41000] {
        group.bench_function(format!("waste_island_database_with_size_{}_and_3K", size), |b| {
            if database_path.exists() {
                fs::remove_dir_all(&database_path).unwrap();
            }
            let mut database = Database::new(&database_path).unwrap();

            b.iter(|| {
                for p in &cache.data_pathes[0..size] {
                    let mut file = File::open(&p).unwrap();
                    let len = (file.metadata().unwrap().len() / 100) as usize;
                    let mut content = Vec::with_capacity(len);
                    unsafe { content.set_len(len) };
                    file.read_exact(&mut content).unwrap();

                    let hash = database.put(&content).unwrap();
                    for _ in 0..99 {
                        database.get(&hash).unwrap();
                    }
                }
            })
        });
        group.bench_function(format!("baseline_with_size_{}_and_3K", size), |b| {
            let baseline_path = benchmark_path("1_put_and_99_reads_baseline");
            if baseline_path.exists() {
                fs::remove_dir_all(&baseline_path).unwrap();
            }
            fs::create_dir(&baseline_path).unwrap();
            let sd = SimpleDatabase::new(&baseline_path);

            let mut avg_len = 0;
            b.iter(|| {
                avg_len = 0;
                for p in &cache.data_pathes[0..size] {
                    let mut file = File::open(&p).unwrap();
                    let len = (file.metadata().unwrap().len() / 100) as usize;
                    let mut content = Vec::with_capacity(len);
                    unsafe { content.set_len(len) };
                    file.read_exact(&mut content).unwrap();

                    avg_len += content.len() as u128;
                    let hash = sd.put(&content).unwrap();
                    for _ in 0..99 {
                        sd.get(&hash).unwrap();
                    }
                }
                let path = cache.data_pathes.choose(&mut rand::thread_rng()).unwrap();
                fs::read(path).unwrap();
                avg_len = avg_len / (size as u128);
            });
            eprintln!("size: {}, avg_len: {}", size, avg_len);
        });
    }

    group.finish();
}

fn bench_boost_quickly_for_pictures(c: &mut Criterion) {
    let size = 100;
    let cache = PictureCache::new(size);

    // Init the database
    let database_path = benchmark_path("boost_quickly_for_pictures");
    if database_path.exists() {
        fs::remove_dir_all(&database_path).unwrap();
    }

    let mut database = Database::new(&database_path).unwrap();
    for p in &cache.data_pathes {
        let content = fs::read(p).unwrap();
        database.put(&content).unwrap();
    }

    // Start benchmark
    let mut group = c.benchmark_group("boost_quickly_for_pictures");
    group.bench_function("waste_island_database", |b| {
        b.iter(|| {
            let mut database =
                Database::new(&database_path).unwrap();
            let hash = cache.data_hashes.choose(&mut rand::thread_rng()).unwrap();
            database.get(hash).unwrap();
        });
    });
    group.bench_function("baseline", |b| {
        b.iter(|| {
            let path = cache.data_pathes.choose(&mut rand::thread_rng()).unwrap();
            fs::read(path).unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_1_put_and_99_reads, bench_boost_quickly_for_pictures);
criterion_main!(benches);