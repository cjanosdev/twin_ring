use super::*;
use std::collections::HashSet;

fn entry(value: &str, expires_at: Instant) -> Entry {
    Entry {
        value: value.to_owned(),
        expires_at,
    }
}

fn assert_consistent(cache: &LruState) {
    let queued: HashSet<&String> = cache.lru_order.iter().collect();
    let stored: HashSet<&String> = cache.entries.keys().collect();
    assert_eq!(
        queued.len(),
        cache.lru_order.len(),
        "duplicate key in queue"
    );
    assert_eq!(queued, stored, "map and queue contain different keys");
    assert!(
        cache.entries.len() <= cache.max_entries,
        "capacity exceeded"
    );
}

#[test]
fn empty_cache_misses_and_removal_is_harmless() {
    let now = Instant::now();
    let mut cache = LruState::new(2);
    assert_eq!(cache.get("missing", now), None);
    assert!(!cache.remove("missing"));
    assert_eq!(cache.live_len(now), 0);
    assert!(cache.evict_expired(now).is_empty());
    assert_consistent(&cache);
}

#[test]
fn insertion_returns_the_value() {
    let now = Instant::now();
    let mut cache = LruState::new(2);
    cache.put("a".into(), entry("hello", now + Duration::from_secs(10)));
    assert_eq!(cache.get("a", now), Some("hello".into()));
    assert_eq!(cache.live_len(now), 1);
    assert_consistent(&cache);
}

#[test]
fn capacity_evicts_the_oldest_unaccessed_key() {
    let now = Instant::now();
    let mut cache = LruState::new(2);
    for key in ["a", "b", "c"] {
        cache.put(key.into(), entry(key, now + Duration::from_secs(10)));
    }
    assert_eq!(cache.get("a", now), None);
    assert_eq!(cache.get("b", now), Some("b".into()));
    assert_eq!(cache.get("c", now), Some("c".into()));
    assert_consistent(&cache);
}

#[test]
fn reading_a_key_changes_eviction_order_but_not_its_expiration() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    let mut cache = LruState::new(2);
    cache.put("a".into(), entry("a", deadline));
    cache.put("b".into(), entry("b", deadline));
    // At t=9, reading A makes B the least recently used key.
    let read_at = now + Duration::from_secs(9);
    assert_eq!(cache.get("a", read_at), Some("a".into()));
    cache.put("c".into(), entry("c", read_at + Duration::from_secs(10)));
    // B is evicted for capacity, even though it has not expired yet.
    assert_eq!(cache.get("b", read_at), None);
    assert_eq!(cache.get("a", read_at), Some("a".into()));
    assert_consistent(&cache);
    // At t=10, A still expires at its ORIGINAL deadline despite those reads.
    assert_eq!(cache.get("a", deadline), None);
    assert_eq!(cache.get("c", deadline), Some("c".into()));
    assert_consistent(&cache);
}

#[test]
fn updating_replaces_value_and_refreshes_recency_without_evicting() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    let mut cache = LruState::new(2);
    cache.put("a".into(), entry("old", deadline));
    cache.put("b".into(), entry("b", deadline));
    cache.put("a".into(), entry("new", deadline));
    assert_eq!(cache.entries.len(), 2);
    assert!(cache.entries.contains_key("b"));
    assert_consistent(&cache);
    // Do not read a before inserting c: the update itself must refresh recency.
    cache.put("c".into(), entry("c", deadline));
    assert_eq!(cache.get("b", now), None);
    assert_eq!(cache.get("a", now), Some("new".into()));
    assert_consistent(&cache);
}

#[test]
fn repeated_reads_do_not_duplicate_queue_entries() {
    let now = Instant::now();
    let mut cache = LruState::new(2);
    cache.put("a".into(), entry("a", now + Duration::from_secs(10)));
    for _ in 0..100 {
        assert_eq!(cache.get("a", now), Some("a".into()));
        assert_consistent(&cache);
    }
}

#[test]
fn missing_reads_do_not_change_eviction_order() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    let mut cache = LruState::new(2);
    cache.put("a".into(), entry("a", deadline));
    cache.put("b".into(), entry("b", deadline));
    assert_eq!(cache.get("missing", now), None);
    cache.put("c".into(), entry("c", deadline));
    assert_eq!(cache.get("a", now), None);
    assert_consistent(&cache);
}

#[test]
fn fixed_ttl_expires_at_the_deadline_despite_reads() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    let mut cache = LruState::new(2);
    cache.put("a".into(), entry("a", deadline));
    assert_eq!(
        cache.get("a", deadline - Duration::from_nanos(1)),
        Some("a".into())
    );
    assert_eq!(cache.get("a", deadline), None);
    assert_eq!(cache.live_len(deadline), 0);
    assert!(cache.entries.is_empty());
    assert_consistent(&cache);
}

#[test]
fn updating_sets_a_new_expiration_deadline() {
    let now = Instant::now();
    let mut cache = LruState::new(1);
    cache.put("a".into(), entry("old", now + Duration::from_secs(1)));
    cache.put("a".into(), entry("new", now + Duration::from_secs(10)));
    assert_eq!(
        cache.get("a", now + Duration::from_secs(1)),
        Some("new".into())
    );
    assert_eq!(cache.get("a", now + Duration::from_secs(10)), None);
    assert_consistent(&cache);
}

#[test]
fn cleanup_removes_only_expired_entries_and_preserves_survivor_order() {
    let now = Instant::now();
    let later = now + Duration::from_secs(10);
    let mut cache = LruState::new(4);
    cache.put("a".into(), entry("a", later));
    cache.put("expired".into(), entry("expired", now));
    cache.put("b".into(), entry("b", later));
    assert_eq!(cache.live_len(now), 2);
    assert_eq!(
        cache.entries.len(),
        3,
        "counting live entries must not mutate storage"
    );
    assert_eq!(cache.evict_expired(now), vec!["expired".to_string()]);
    assert_eq!(
        cache.lru_order,
        VecDeque::from(["a".to_string(), "b".to_string()])
    );
    assert!(cache.evict_expired(now).is_empty());
    assert_eq!(cache.evict_expired(later).len(), 2);
    assert_consistent(&cache);
}

#[test]
fn removal_at_any_position_allows_reinsertion() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    for removed in ["a", "b", "c"] {
        let mut cache = LruState::new(3);
        for key in ["a", "b", "c"] {
            cache.put(key.into(), entry(key, deadline));
        }
        assert!(cache.remove(removed));
        assert!(!cache.remove(removed));
        assert_eq!(cache.get(removed, now), None);
        assert_consistent(&cache);
        cache.put(removed.into(), entry("replacement", deadline));
        assert_eq!(cache.get(removed, now), Some("replacement".into()));
        assert_consistent(&cache);
    }
}

#[test]
fn zero_capacity_stores_nothing() {
    let now = Instant::now();
    let mut cache = LruState::new(0);
    cache.put("a".into(), entry("a", now + Duration::from_secs(10)));
    assert_eq!(cache.get("a", now), None);
    assert_consistent(&cache);
}

#[test]
fn capacity_one_supports_updates_and_replacement() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    let mut cache = LruState::new(1);
    cache.put("a".into(), entry("old", deadline));
    cache.put("a".into(), entry("new", deadline));
    assert_eq!(cache.get("a", now), Some("new".into()));
    cache.put("b".into(), entry("b", deadline));
    assert_eq!(cache.get("a", now), None);
    assert_eq!(cache.get("b", now), Some("b".into()));
    assert_consistent(&cache);
}

// A deliberately different representation: one vector of key/value/deadline
// records in recency order. It trades speed for an easy-to-check specification.
#[derive(Default)]
struct ReferenceCache {
    records: Vec<(String, String, Instant)>,
}

impl ReferenceCache {
    fn get(&mut self, key: &str, now: Instant) -> Option<String> {
        let index = self.records.iter().position(|record| record.0 == key)?;
        let record = self.records.remove(index);
        if record.2 <= now {
            return None;
        }
        let value = record.1.clone();
        self.records.push(record);
        Some(value)
    }

    fn put(&mut self, key: String, value: String, deadline: Instant, capacity: usize) {
        self.records.retain(|record| record.0 != key);
        self.records.push((key, value, deadline));
        if self.records.len() > capacity {
            self.records.remove(0);
        }
    }
}

#[test]
fn mixed_operations_match_a_reference_model() {
    for capacity in [0, 1, 2, 5, 16] {
        for seed in [1_u64, 42, 98765] {
            let mut state = seed;
            let mut now = Instant::now();
            let mut cache = LruState::new(capacity);
            let mut model = ReferenceCache::default();
            for step in 0..2000 {
                // A fixed seed makes a failed sequence reproducible, with no extra dependency.
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = format!("key{}", (state >> 16) % 12);
                let context = format!("capacity={capacity}, seed={seed}, step={step}");
                match (state >> 32) % 6 {
                    0 | 1 => {
                        let value = format!("value{step}");
                        let deadline = now + Duration::from_millis((state >> 48) % 10);
                        cache.put(key.clone(), entry(&value, deadline));
                        model.put(key, value, deadline, capacity);
                    }
                    2 => assert_eq!(cache.get(&key, now), model.get(&key, now), "{context}"),
                    3 => {
                        let before = model.records.len();
                        model.records.retain(|record| record.0 != key);
                        assert_eq!(
                            cache.remove(&key),
                            before != model.records.len(),
                            "{context}"
                        );
                    }
                    4 => now += Duration::from_millis(3),
                    _ => {
                        let mut expected: Vec<String> = model
                            .records
                            .iter()
                            .filter(|record| record.2 <= now)
                            .map(|record| record.0.clone())
                            .collect();
                        model.records.retain(|record| record.2 > now);
                        let mut actual = cache.evict_expired(now);
                        actual.sort();
                        expected.sort();
                        assert_eq!(actual, expected, "{context}");
                    }
                }
                assert_consistent(&cache);
                let actual: Vec<_> = cache
                    .lru_order
                    .iter()
                    .map(|key| {
                        let entry = &cache.entries[key];
                        (key.clone(), entry.value.clone(), entry.expires_at)
                    })
                    .collect();
                assert_eq!(actual, model.records, "{context}");
                assert_eq!(
                    cache.live_len(now),
                    model.records.iter().filter(|r| r.2 > now).count(),
                    "{context}"
                );
            }
        }
    }
}

#[test]
fn public_cache_clones_share_state_and_trait_methods_work() {
    let cache = LruCache::new(Duration::from_secs(60), 2, None);
    let clone = cache.clone();
    let backend: &dyn CacheBackend = &clone;
    backend.put("a".into(), "value".into());
    assert_eq!(cache.get("a"), Some("value".into()));
    assert_eq!(backend.get("a"), Some("value".into()));
    assert_eq!(backend.live_len(), 1);
    backend.evict_expired();
    assert!(cache.delete("a"));
    assert_eq!(backend.get("a"), None);
    assert!(!backend.delete("a"));
}

#[test]
fn public_cache_zero_ttl_never_returns_a_hit() {
    let cache = LruCache::new(Duration::ZERO, 2, None);
    cache.put("a".into(), "value".into());
    assert_eq!(cache.live_len(), 0);
    assert_eq!(cache.get("a"), None);
    cache.evict_expired();
    assert_consistent(&cache.state.lock().unwrap());
}

#[test]
fn concurrent_clones_preserve_values_and_collection_consistency() {
    let cache = LruCache::new(Duration::from_secs(3600), 8, None);
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let handles: Vec<_> = (0..8)
        .map(|worker| {
            let cache = cache.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let key = format!("key{worker}");
                barrier.wait();
                for iteration in 0..100 {
                    let value = format!("value{iteration}");
                    cache.put(key.clone(), value.clone());
                    assert_eq!(cache.get(&key), Some(value));
                    if iteration % 3 == 0 {
                        assert!(cache.delete(&key));
                        assert_eq!(cache.get(&key), None);
                    }
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_consistent(&cache.state.lock().unwrap());
}
