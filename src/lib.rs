//! keyed-stream-map: a `StreamMap` with expected constant-time key lookups.
//!
//! Poll streams with keys attached to their items, with expected constant-time
//! access to the stream for a given key.

mod rand;
mod stream_map;

pub use stream_map::StreamMap;
