use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

const SIZES: [usize; 3] = [100, 1_000, 5_000];

fn keyed_map(size: usize) -> keyed_stream_map::StreamMap<usize, tokio_stream::Pending<()>> {
    let mut map = keyed_stream_map::StreamMap::new();
    for key in 0..size {
        map.insert(key, tokio_stream::pending());
    }
    map
}

fn tokio_map(size: usize) -> tokio_stream::StreamMap<usize, tokio_stream::Pending<()>> {
    let mut map = tokio_stream::StreamMap::new();
    for key in 0..size {
        map.insert(key, tokio_stream::pending());
    }
    map
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    for &size in &SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("keyed", size), &size, |b, &size| {
            b.iter_batched(
                keyed_stream_map::StreamMap::new,
                |mut map| {
                    for key in 0..size {
                        map.insert(key, tokio_stream::pending::<()>());
                    }
                    black_box(map);
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_with_input(BenchmarkId::new("tokio", size), &size, |b, &size| {
            b.iter_batched(
                tokio_stream::StreamMap::new,
                |mut map| {
                    for key in 0..size {
                        map.insert(key, tokio_stream::pending::<()>());
                    }
                    black_box(map);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_contains_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_key_all");

    for &size in &SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("keyed", size), &size, |b, &size| {
            b.iter_batched_ref(
                || keyed_map(size),
                |map| {
                    for key in 0..size {
                        black_box(map.contains_key(&key));
                    }
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_with_input(BenchmarkId::new("tokio", size), &size, |b, &size| {
            b.iter_batched_ref(
                || tokio_map(size),
                |map| {
                    for key in 0..size {
                        black_box(map.contains_key(&key));
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_all");

    for &size in &SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("keyed", size), &size, |b, &size| {
            b.iter_batched_ref(
                || keyed_map(size),
                |map| {
                    for key in 0..size {
                        black_box(map.get(&key));
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("remove_all");

    for &size in &SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("keyed", size), &size, |b, &size| {
            b.iter_batched(
                || keyed_map(size),
                |mut map| {
                    for key in 0..size {
                        black_box(map.remove(&key));
                    }
                    black_box(map);
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_with_input(BenchmarkId::new("tokio", size), &size, |b, &size| {
            b.iter_batched(
                || tokio_map(size),
                |mut map| {
                    for key in 0..size {
                        black_box(map.remove(&key));
                    }
                    black_box(map);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_contains_key,
    bench_get,
    bench_remove
);
criterion_main!(benches);
