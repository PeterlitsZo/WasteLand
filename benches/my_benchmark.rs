use criterion::{black_box, criterion_group, criterion_main, Criterion};
use waste_island::Database;

// The benchmark in my computer: 221.39 ms.
fn benchmark(c: &mut Criterion) {
    c.bench_function("benchmark", |b| {
        b.iter(|| {
            let database = Database::create("/tmp/waste_island.benchmark")
                .unwrap();
            for i in 0..1024 {
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

criterion_group!(benches, benchmark);
criterion_main!(benches);
