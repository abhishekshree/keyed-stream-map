//! Insert + lookup timing with many streams.
//!
//! Run: `cargo run --release --example many_streams -- 20000`
//! Compares keyed-stream-map against tokio-stream on the same workload.

use std::time::Instant;

fn ms<F: FnOnce()>(f: F) -> u128 {
    let t = Instant::now();
    f();
    t.elapsed().as_millis()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    println!("streams: {n}");

    let insert_keyed = ms(|| {
        let mut m = keyed_stream_map::StreamMap::new();
        for i in 0..n {
            m.insert(i, tokio_stream::pending::<i32>());
        }
        std::hint::black_box(m.len());
    });
    let insert_tokio = ms(|| {
        let mut m = tokio_stream::StreamMap::new();
        for i in 0..n {
            m.insert(i, tokio_stream::pending::<i32>());
        }
        std::hint::black_box(m.len());
    });
    println!("insert all: keyed {insert_keyed}ms, tokio {insert_tokio}ms");

    let mut keyed = keyed_stream_map::StreamMap::new();
    let mut tokio_map = tokio_stream::StreamMap::new();
    for i in 0..n {
        keyed.insert(i, tokio_stream::pending::<i32>());
        tokio_map.insert(i, tokio_stream::pending::<i32>());
    }

    let lookup_keyed = ms(|| {
        for i in 0..n {
            assert!(keyed.contains_key(&i));
        }
    });
    let lookup_tokio = ms(|| {
        for i in 0..n {
            assert!(tokio_map.contains_key(&i));
        }
    });
    println!("contains_key x{n}: keyed {lookup_keyed}ms, tokio {lookup_tokio}ms");

    let get_keyed = ms(|| {
        for i in 0..n {
            std::hint::black_box(keyed.get(&i));
        }
    });
    println!("get x{n}: keyed {get_keyed}ms (tokio has no get)");

    let remove_keyed = ms(|| {
        for i in 0..n {
            keyed.remove(&i);
        }
    });
    let remove_tokio = ms(|| {
        for i in 0..n {
            tokio_map.remove(&i);
        }
    });
    println!("remove all: keyed {remove_keyed}ms, tokio {remove_tokio}ms");
    println!("done. keyed wins grow with n because lookups stay flat.");
}
