//! keyed-stream-map: a `StreamMap` that looks up keys in constant time.
//!
//! Same polling behavior as `tokio_stream::StreamMap`, plus `get` and `get_mut`.
//! Swap the import and keep the rest of your code.

mod rand;
mod stream_map;

pub use stream_map::StreamMap;
