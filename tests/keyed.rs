use keyed_stream_map::StreamMap;
use tokio_stream::{Stream, StreamExt, pending};

// get returns the right stream, missing keys give None.
#[test]
fn get_found_and_missing() {
    let mut map = StreamMap::new();
    map.insert("a", tokio_stream::iter(vec![1]));
    assert!(map.get(&"a").is_some());
    assert!(map.get(&"zzz").is_none());
}

// get_mut lets callers poke the stream in place.
#[test]
fn get_mut_sees_same_entry() {
    let mut map = StreamMap::new();
    map.insert(7u32, tokio_stream::iter(vec![1, 2]));
    let len_before = map.get(&7u32).unwrap().size_hint().0;
    assert_eq!(len_before, 2);
    let s = map.get_mut(&7u32).unwrap();
    let _ = s.size_hint();
    assert!(map.contains_key(&7u32));
}

// Borrowed lookup works: String keys, &str queries.
#[test]
fn borrowed_key_lookup() {
    let mut map = StreamMap::new();
    map.insert("foo".to_string(), pending::<i32>());
    assert!(map.contains_key("foo"));
    assert!(map.get("foo").is_some());
    assert!(map.get_mut("foo").is_some());
    assert!(map.remove("foo").is_some());
    assert!(!map.contains_key("foo"));
}

// Re-inserting a key replaces the stream without growing the map.
#[test]
fn insert_replace_keeps_len() {
    let mut map = StreamMap::new();
    assert!(map.insert(1u32, tokio_stream::iter(vec![1])).is_none());
    assert!(map.insert(1u32, tokio_stream::iter(vec![2])).is_some());
    assert_eq!(map.len(), 1);
}

// Removing middle entries keeps the rest reachable (swap_remove fixup).
#[tokio::test]
async fn remove_many_stays_consistent() {
    let mut map = StreamMap::new();
    for i in 0..200u32 {
        map.insert(i, tokio_stream::iter(vec![i]));
    }
    for i in (0..200u32).step_by(2) {
        assert!(map.remove(&i).is_some());
    }
    assert_eq!(map.len(), 100);
    for i in (1..200u32).step_by(2) {
        assert!(map.contains_key(&i), "missing {i}");
        assert!(map.get(&i).is_some(), "get missing {i}");
    }
    let mut seen = 0;
    while let Some((_, _)) = map.next().await {
        seen += 1;
    }
    assert_eq!(seen, 100);
}

// Extend with duplicate keys replaces instead of doubling.
#[test]
fn extend_dedupes() {
    let mut map: StreamMap<u32, _> = StreamMap::new();
    map.extend([(1u32, tokio_stream::iter(vec![1]))]);
    map.extend([(1u32, tokio_stream::iter(vec![2]))]);
    assert_eq!(map.len(), 1);
}

// FromIterator with duplicate keys keeps the last one.
#[test]
fn from_iter_dedupes() {
    let map: StreamMap<u32, _> = [
        (1u32, tokio_stream::iter(vec![1])),
        (1, tokio_stream::iter(vec![2])),
    ]
    .into_iter()
    .collect();
    assert_eq!(map.len(), 1);
}

// Clear drops the key index too.
#[test]
fn clear_drops_index() {
    let mut map = StreamMap::new();
    map.insert("a", pending::<i32>());
    map.insert("b", pending());
    map.clear();
    assert!(map.is_empty());
    assert!(!map.contains_key(&"a"));
    assert!(map.get(&"a").is_none());
    map.insert("a", pending::<i32>());
    assert!(map.contains_key(&"a"));
}

// Scale smoke: 5k keys, all reachable, half removable.
#[test]
fn five_thousand_keys() {
    let mut map = StreamMap::new();
    for i in 0..5000u32 {
        map.insert(i, pending::<i32>());
    }
    assert_eq!(map.len(), 5000);
    for i in [0, 17, 2499, 4999] {
        assert!(map.get(&i).is_some());
    }
    for i in 0..2500u32 {
        map.remove(&i);
    }
    assert_eq!(map.len(), 2500);
    assert!(!map.contains_key(&0));
    assert!(map.contains_key(&4999));
}
