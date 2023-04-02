mod picture_cache;
mod simple_database;

use std::{fs, path::PathBuf};

use criterion::{criterion_group, criterion_main, Criterion};
use rand::{self, seq::SliceRandom};

use waste_island::Database;
use picture_cache::PictureCache;
use simple_database::SimpleDatabase;

fn temp_path() -> PathBuf {
    PathBuf::from("/tmp/waste_island")
}

fn picture_cache_path() -> PathBuf {
    temp_path().join("picture_cache")
}

fn benchmark_path(benchmark_name: &str) -> PathBuf {
    temp_path().join("benchmark").join(benchmark_name)
}

fn bench_1_put_and_99_reads(c: &mut Criterion) {
    let cache = PictureCache::new(&picture_cache_path());
    let database_path = benchmark_path("1_put_and_99_reads");

    // Start benchmark
    let mut group = c.benchmark_group("1_put_and_99_reads");
    group.bench_function("waste_island_batabase", |b| {
        if database_path.exists() {
            fs::remove_dir_all(&database_path).unwrap();
        }
        let mut database = Database::create(&database_path).unwrap();

        b.iter(|| {
            for p in &cache.data_pathes {
                let content = fs::read(&p).unwrap();
                let hash = database.put(&content).unwrap();
                for _ in 0..99 {
                    database.get(&hash).unwrap();
                }
            }
        })
    });
    group.bench_function("baseline", |b| {
        let baseline_path = benchmark_path("1_put_and_99_reads_baseline");
        if baseline_path.exists() {
            fs::remove_dir_all(&baseline_path).unwrap();
        }
        fs::create_dir(&baseline_path).unwrap();
        let sd = SimpleDatabase::new(&baseline_path);

        b.iter(|| {
            for p in &cache.data_pathes {
                let content = fs::read(&p).unwrap();
                let hash = sd.put(&content).unwrap();
                for _ in 0..99 {
                    sd.get(&hash).unwrap();
                }
            }
            let path = cache.data_pathes.choose(&mut rand::thread_rng()).unwrap();
            fs::read(path).unwrap();
        });
    });
}

fn bench_picture_cache(c: &mut Criterion) {
    let cache = PictureCache::new(&picture_cache_path());

    // Init the database
    let database_path = benchmark_path("boost_quickly_for_pictures");
    if database_path.exists() {
        fs::remove_dir_all(&database_path).unwrap();
    }

    let mut database = Database::create(&database_path).unwrap();
    for p in &cache.data_pathes {
        let content = fs::read(p).unwrap();
        database.put(&content).unwrap();
    }

    // Start benchmark
    let mut group = c.benchmark_group("boost_quickly_for_pictures");
    group.bench_function("waste_island_database", |b| {
        b.iter(|| {
            let mut database =
                Database::open(&database_path).unwrap();
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

criterion_group!(benches, bench_1_put_and_99_reads, bench_picture_cache);
criterion_main!(benches);
