use std::{fs, path::{self, PathBuf}};

use criterion::{criterion_group, criterion_main, Criterion};
use reqwest;
use rand::{self, seq::SliceRandom};

use waste_island::Database;

struct PictureCache {
    pub data_hashes: Vec<String>,
    pub data_pathes: Vec<PathBuf>,
}

fn get_piture_cache() -> PictureCache {
    let cache = path::PathBuf::from("/tmp/waste_island/picture_cache");
    fs::create_dir_all(&cache).unwrap();

    let mut data_pathes = vec![];
    for i in 0..128 {
        let pic_path = cache.join(format!("pic_{i}.jpg"));
        data_pathes.push(pic_path.clone());
        if pic_path.exists() {
            continue;
        }

        let resp =
            reqwest::blocking::get(format!("https://cataas.com/cat/says/random {i}")).unwrap();
        fs::write(pic_path, resp.bytes().unwrap()).unwrap();
    }

    let mut data_hashes = vec![];
    for f in fs::read_dir(&cache).unwrap() {
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

fn bench_1_put_and_99_reads(c: &mut Criterion) {
    // The benchmark in my computer: 221.39 ms.
    // Use one file:                 84.577 ms.
    c.bench_function("1 put and 99 reads", |b| {
        b.iter(|| {
            let mut database =
                Database::create("/tmp/waste_island/benchmark/1_put_and_99_reads").unwrap();
            for i in 0..512 {
                let content = format!("{} {}", i, "just for test".repeat(1024));
                let hash = database.put(content.as_bytes()).unwrap();
                for _ in 0..99 {
                    database.get(&hash).unwrap();
                }
            }
            database.drop().unwrap();
        })
    });
}

fn bench_picture_cache(c: &mut Criterion) {
    let cache = get_piture_cache();

    // Init the database
    let mut database = Database::create("/tmp/waste_island/benchmark/boost_quickly_for_pictures").unwrap();
    for p in &cache.data_pathes {
        let content = fs::read(p).unwrap();
        database.put(&content).unwrap();
    }

    let mut group = c.benchmark_group("boost quickly for pictures");
    group.bench_function("wasteland database", |b| {
        b.iter(|| {
            let mut database =
                Database::open("/tmp/waste_island/benchmark/boost_quickly_for_pictures").unwrap();
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

    database.drop().unwrap();
    group.finish();
}

criterion_group!(benches, bench_1_put_and_99_reads, bench_picture_cache);
criterion_main!(benches);
