use std::fs;

use criterion::{
    criterion_group, criterion_main, Criterion,
};
use rand::{self, seq::SliceRandom};

use benchmark::{bencher::{Bencher, benchmark_path}, picture_cache::PictureCache};
use waste_island::Database;

/// Bench test about little write but a lot of read.
fn bench_1_put_and_99_reads(c: &mut Criterion) {
    let mut b = Bencher::new();

    for size in [10, 100, 1000, 10000] {
        for (value_size, div) in [("3K", 100), ("30K", 10), ("300K", 1)] {
            let group_name = format!("1_put_and_99_reads__size={}__content={}", size, value_size);
            let mut group = c.benchmark_group(group_name);
            group.sample_size(10);

            // Test for WasteIsland database.
            b.bench_waste_island(&mut group, "waste_island_database", size, div);

            // Test for SQLite database.
            b.bench_sqlite(&mut group, "baseline_sqlite", size, div);

            // Test for RocksDB.
            if !(size == 10000 && div == 1) {
                b.bench_rocksdb(&mut group, "baseline_rocksdb", size, div);
            }

            // Test for fs.
            b.bench_fs(&mut group, "baseline_fs", size, div);

            group.finish();
        }
    }
}

/// Bench test to make sure it can boost very quickly.
fn bench_boost_quickly_for_pictures(c: &mut Criterion) {
    let size = 100;
    let cache = PictureCache::new(size);

    // Init the database
    let database_path = benchmark_path("boost_quickly_for_pictures");

    let mut database = Database::new(&database_path).unwrap();
    for p in &cache.data_pathes {
        let content = fs::read(p).unwrap();
        database.put(&content).unwrap();
    }

    // Start benchmark
    let mut group = c.benchmark_group("boost_quickly_for_pictures");
    group.bench_function("waste_island_database", |b| {
        b.iter(|| {
            let mut database = Database::new(&database_path).unwrap();
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

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_1_put_and_99_reads, bench_boost_quickly_for_pictures,
);
criterion_main!(benches);
