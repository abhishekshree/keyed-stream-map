use futures_core::Stream;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::future::poll_fn;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

/// Merge many streams, tagged by key.
///
/// Yields `(key, item)` in arrival order. Insert and remove at any time.
/// Polling starts at a random entry to reduce fixed-order bias. This does not
/// guarantee a maximum wait time for any stream.
///
/// Unlike `tokio_stream::StreamMap`, keyed ops are `O(1)` via a hash index.
/// Polling still scans entries, same as upstream.
///
/// # Example
///
/// ```
/// use keyed_stream_map::StreamMap;
/// use tokio_stream::{self as stream, StreamExt};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let mut map = StreamMap::new();
/// map.insert("a", stream::iter(vec![1]));
/// map.insert("b", stream::iter(vec![2]));
/// assert_eq!(map.len(), 2);
/// assert_eq!(map.get(&"a").is_some(), true);
/// while let Some((k, v)) = map.next().await {
///     println!("{k}: {v}");
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StreamMap<K, V> {
    entries: Vec<(K, V)>,
    // Clone of each key -> position in `entries`. Fixed up on every swap_remove.
    index: HashMap<K, usize>,
}

impl<K, V> StreamMap<K, V> {
    /// Empty map. Allocates on first insert.
    ///
    /// ```
    /// use keyed_stream_map::StreamMap;
    /// use tokio_stream::Pending;
    /// let map: StreamMap<&str, Pending<()>> = StreamMap::new();
    /// assert!(map.is_empty());
    /// ```
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            index: HashMap::new(),
        }
    }

    /// Empty map that can hold `capacity` entries without reallocating.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            index: HashMap::with_capacity(capacity),
        }
    }

    /// Iterate over `(key, stream)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.entries.iter()
    }

    /// Iterate over keys and mutable references to their streams.
    ///
    /// Keys are immutable because the map keeps a separate index of them.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.entries.iter_mut().map(|(key, stream)| (&*key, stream))
    }

    /// All keys.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.iter().map(|(k, _)| k)
    }

    /// All streams.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.iter().map(|(_, v)| v)
    }

    /// All streams, mutably.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.entries.iter_mut().map(|(_, v)| v)
    }

    /// Room for more entries without reallocating. Lower bound.
    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Count of streams in the map.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no streams are stored.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drop all streams. Keeps allocated memory.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.index.clear();
    }

    /// Insert a stream. Returns the old stream when the key existed.
    ///
    /// Replace keeps the entry position, so polling order is stable.
    pub fn insert(&mut self, k: K, stream: V) -> Option<V>
    where
        K: Hash + Eq + Clone,
    {
        if let Some(&pos) = self.index.get(&k) {
            let slot = &mut self.entries[pos].1;
            return Some(std::mem::replace(slot, stream));
        }
        let pos = self.entries.len();
        self.index.insert(k.clone(), pos);
        self.entries.push((k, stream));
        None
    }

    /// Remove by key. Returns the stream when present.
    pub fn remove<Q>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q> + Hash + Eq + Clone,
        Q: Hash + Eq + ?Sized,
    {
        let idx = *self.index.get(k)?;
        self.index.remove(k);
        Some(self.swap_remove_idx(idx).1)
    }

    /// True when the key has a stream.
    pub fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq + ?Sized,
    {
        self.index.contains_key(k)
    }

    /// Reference to the stream for `k`, if present.
    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq + ?Sized,
    {
        let &idx = self.index.get(k)?;
        self.entries.get(idx).map(|(_, v)| v)
    }

    /// Mutable reference to the stream for `k`, if present.
    pub fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq + ?Sized,
    {
        let &idx = self.index.get(k)?;
        self.entries.get_mut(idx).map(|(_, v)| v)
    }
}

impl<K, V> StreamMap<K, V>
where
    K: Clone,
{
    // Remove entries[idx]. Caller already dropped the index row for it.
    // Points the swapped-in entry at its new slot.
    fn swap_remove_idx(&mut self, idx: usize) -> (K, V)
    where
        K: Hash + Eq,
    {
        let out = self.entries.swap_remove(idx);
        if idx < self.entries.len() {
            let moved = self.entries[idx].0.clone();
            self.index.insert(moved, idx);
        }
        out
    }
}

impl<K, V> StreamMap<K, V>
where
    K: Unpin,
    V: Stream + Unpin,
{
    fn poll_next_entry(&mut self, cx: &mut Context<'_>) -> Poll<Option<(usize, V::Item)>>
    where
        K: Hash + Eq + Clone,
    {
        if self.entries.is_empty() {
            return Poll::Ready(None);
        }
        let start = crate::rand::thread_rng_n(self.entries.len() as u32) as usize;
        let mut idx = start;

        for _ in 0..self.entries.len() {
            let (_, stream) = &mut self.entries[idx];
            match Pin::new(stream).poll_next(cx) {
                Poll::Ready(Some(val)) => return Poll::Ready(Some((idx, val))),
                Poll::Ready(None) => {
                    // Stream finished, drop it and keep polling.
                    let key = self.entries[idx].0.clone();
                    self.index.remove(&key);
                    self.swap_remove_idx(idx);
                    if self.entries.is_empty() {
                        return Poll::Ready(None);
                    }
                    if idx == self.entries.len() {
                        idx = 0;
                    } else if idx < start && start <= self.entries.len() {
                        // Swapped-in entry was already polled, skip it.
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
                Poll::Pending => {
                    idx = idx.wrapping_add(1) % self.entries.len();
                }
            }
        }

        if self.entries.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<K, V> Default for StreamMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> StreamMap<K, V>
where
    K: Clone + Unpin,
    V: Stream + Unpin,
{
    /// Pull up to `limit` items into `buffer`. Returns count appended.
    ///
    /// Returns 0 only when the map is empty or `limit` is 0.
    /// Cancel safe: dropping the future loses nothing.
    pub async fn next_many(&mut self, buffer: &mut Vec<(K, V::Item)>, limit: usize) -> usize
    where
        K: Hash + Eq,
    {
        poll_fn(|cx| self.poll_next_many(cx, buffer, limit)).await
    }

    /// Poll version of [`StreamMap::next_many`].
    pub fn poll_next_many(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut Vec<(K, V::Item)>,
        limit: usize,
    ) -> Poll<usize>
    where
        K: Hash + Eq,
    {
        if limit == 0 || self.entries.is_empty() {
            return Poll::Ready(0);
        }

        let mut added = 0;
        let start = crate::rand::thread_rng_n(self.entries.len() as u32) as usize;
        let mut idx = start;

        while added < limit {
            let mut made_progress = false;

            for _ in 0..self.entries.len() {
                if self.entries.is_empty() {
                    break;
                }
                let (_, stream) = &mut self.entries[idx];
                match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(Some(val)) => {
                        added += 1;
                        buffer.push((self.entries[idx].0.clone(), val));
                        made_progress = true;
                        idx = idx.wrapping_add(1) % self.entries.len();
                        if added == limit {
                            break;
                        }
                    }
                    Poll::Ready(None) => {
                        let key = self.entries[idx].0.clone();
                        self.index.remove(&key);
                        self.swap_remove_idx(idx);
                        if self.entries.is_empty() {
                            break;
                        }
                        if idx == self.entries.len() {
                            idx = 0;
                        } else if idx < start && start <= self.entries.len() {
                            idx = idx.wrapping_add(1) % self.entries.len();
                        }
                    }
                    Poll::Pending => {
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
            }

            if !made_progress {
                break;
            }
        }

        if added > 0 {
            Poll::Ready(added)
        } else if self.entries.is_empty() {
            Poll::Ready(0)
        } else {
            Poll::Pending
        }
    }
}

impl<K, V> Stream for StreamMap<K, V>
where
    K: Clone + Unpin,
    V: Stream + Unpin,
    K: Hash + Eq,
{
    type Item = (K, V::Item);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some((idx, val)) = ready!(self.poll_next_entry(cx)) {
            let key = self.entries[idx].0.clone();
            Poll::Ready(Some((key, val)))
        } else {
            Poll::Ready(None)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut lo: usize = 0;
        let mut hi: Option<usize> = Some(0);
        for (_, stream) in &self.entries {
            let (a, b) = stream.size_hint();
            lo = lo.saturating_add(a);
            match (hi, b) {
                (Some(x), Some(y)) => hi = x.checked_add(y),
                (Some(_), None) => hi = None,
                _ => {}
            }
        }
        (lo, hi)
    }
}

impl<K, V> FromIterator<(K, V)> for StreamMap<K, V>
where
    K: Hash + Eq + Clone,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (lo, _) = iter.size_hint();
        let mut map = Self::with_capacity(lo);
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}

impl<K, V> Extend<(K, V)> for StreamMap<K, V>
where
    K: Hash + Eq + Clone,
{
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (K, V)>,
    {
        // Insert per entry so duplicate keys replace instead of doubling.
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}
