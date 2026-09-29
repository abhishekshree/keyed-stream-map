# keyed-stream-map

[![CI](https://github.com/abhishekshree/keyed-stream-map/actions/workflows/ci.yml/badge.svg)](https://github.com/abhishekshree/keyed-stream-map/actions/workflows/ci.yml)

A [`tokio_stream::StreamMap`](https://docs.rs/tokio-stream/latest/tokio_stream/struct.StreamMap.html)
with fast access by key.

The upstream `tokio-stream::StreamMap` currently stores its entries in a `Vec`,
so asking "is key X here?" or removing the stream for key X scans that list.
This crate keeps a hash index alongside the entries, making keyed lookups and
removals expected O(1). It also adds `get` and `get_mut`.

The hash index uses extra memory, and keys must be `Clone` when inserted.
Polling still scans the entries, so this helps when keyed access is the pain,
not when polling many mostly-pending streams is the bottleneck.

## When to use it instead of `select_all`

[`select_all`](https://docs.rs/futures/latest/futures/stream/struct.SelectAll.html)
merges streams but does not let you look up or remove one by key. If your code
has a separate map just to keep streams addressable, `StreamMap` may simplify
that setup. It yields keys with items and is not a drop-in replacement.

Unlike [`FuturesUnordered`](https://docs.rs/futures/latest/futures/stream/struct.FuturesUnordered.html),
this crate scans its entries when polled. Use those notification-driven
alternatives when you need to manage many mostly-pending streams efficiently.

## Install

```toml
[dependencies]
keyed-stream-map = "0.1"
tokio-stream = "0.1"
```

Requires Rust 1.85 or later.

## Example

```rust
use keyed_stream_map::StreamMap;
use tokio_stream::{self as stream, StreamExt};

#[tokio::main]
async fn main() {
    let mut streams = StreamMap::new();
    streams.insert("orders", stream::iter(vec![1, 2]));
    streams.insert("alerts", stream::iter(vec![3]));

    if let Some(order_stream) = streams.get("orders") {
        println!(
            "orders stream has at least {} items",
            order_stream.size_hint().0
        );
    }

    while let Some((key, item)) = streams.next().await {
        println!("{key}: {item}");
    }
}
```

## Compared with `tokio-stream`

The stream polling API and `next_many` follow `tokio-stream`. This crate adds
`get` and `get_mut`, and uses a hash index for keyed operations. There are a few
differences to know about:

- `insert`, `Extend`, and `FromIterator` require keys to implement `Clone`.
- `Extend` replaces an existing stream when a key repeats. Upstream `Extend`
  can store duplicate keys.
- Replacing a key keeps its original entry position and stored key value.
- `iter_mut()` yields `(&K, &mut V)`. You can edit a stream through the
  iterator, but not its key, because keys are part of the index.

## Performance

The example below compares basic keyed operations with `tokio-stream` on your
machine. It is a quick timing demo, not a benchmark suite. Results depend on
the workload, key type, allocator, and hardware. Polling still scans entries.

```sh
cargo run --release --example many_streams -- 20000
```

## Maintenance

This crate is a stopgap. It should become obsolete the day `tokio-stream`
releases a `StreamMap` with indexed keyed operations. Until then, feel free to
use it if fast keyed access is what you need and scan-based polling fits your
workload.

## License

MIT. See [LICENSE](LICENSE).
