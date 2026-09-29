# keyed-stream-map

A `StreamMap` with fast key lookups. Same polling as `tokio-stream`, no linear scan on `insert`, `remove`, or `contains_key`.

`tokio-stream::StreamMap` stores entries in a `Vec` and scans it for every keyed op. That is fine for a handful of streams and slow past a few hundred. This crate keeps the `Vec` for polling and adds a `HashMap` from key to position, so keyed ops stay flat as the map grows.

## Install

```toml
[dependencies]
keyed-stream-map = "0.1"
tokio-stream = "0.1"
```

Requires Rust 1.71 or later.

## Use

Swap the import. The rest stays the same.

```rust
use keyed_stream_map::StreamMap;
use tokio_stream::{self as stream, StreamExt};

#[tokio::main]
async fn main() {
    let mut map = StreamMap::new();
    map.insert("a", stream::iter(vec![1, 2]));
    map.insert("b", stream::iter(vec![3]));

    // New here: direct access by key.
    if let Some(s) = map.get(&"a") {
        println!("a holds {} items", s.size_hint().0);
    }

    while let Some((key, val)) = map.next().await {
        println!("{key}: {val}");
    }
}
```

## What changed from tokio-stream

Polling, fairness, `next_many`, `FromIterator`, and `Extend` behave the same. Two additions:

* `get` and `get_mut` for direct access by key.
* `Extend` and `FromIterator` replace duplicates instead of stacking them.

One bound is stricter: keys need `Clone` on `insert` because the index keeps a copy. In practice keys are `u32`, `String`, or similar, so this rarely matters. Polling already required `Clone`.

## Perf

Run the included example with 20k pending streams:

```sh
cargo run --release --example many_streams -- 20000
```

On a typical laptop `contains_key` and `remove` go from seconds to milliseconds. The gap grows with `n`. Polling speed is unchanged.

## License

MIT. See LICENSE.
