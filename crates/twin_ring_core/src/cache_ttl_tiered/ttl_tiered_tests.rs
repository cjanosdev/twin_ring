use super::*;
use std::collections::HashSet;

fn assert_consistent(cache: &TtlTieredState) {
    let queued: HashSet<_> = cache.lru_order.iter().collect();
    let stored: HashSet<_> = cache.entries.keys().collect();
    assert_eq!(queued.len(), cache.lru_order.len(), "duplicate queue keys");
    assert_eq!(queued, stored, "map and queue disagree");
    assert!(cache.entries.len() <= cache.max_entries);
}

#[test]
fn a_new_fill_is_cold() {
    let start = Instant::now();
    let ttl = Duration::from_secs(10);
    let entry = TieredEntry::new("value".into(), start, ttl);
    assert_eq!(entry.value, "value");
    assert_eq!(entry.hit_count, 0);
    assert_eq!(entry.filled_at, start);
    assert_eq!(entry.expires_at, start + ttl);
}

#[test]
fn only_the_second_and_eighth_hits_extend_the_deadline() {
    let start = Instant::now();
    let ttl = Duration::from_secs(10);
    let mut entry = TieredEntry::new("value".into(), start, ttl);
    for hit in 1..=20 {
        entry.record_hit(ttl);
        let lifetime = match hit {
            1 => 10,
            2..=7 => 20,
            _ => 40,
        };
        assert_eq!(entry.hit_count, hit);
        assert_eq!(entry.filled_at, start);
        assert_eq!(
            entry.expires_at,
            start + Duration::from_secs(lifetime),
            "hit {hit}"
        );
    }
}

#[test]
fn hit_counter_saturates_instead_of_wrapping_to_cold() {
    let start = Instant::now();
    let ttl = Duration::from_secs(10);
    let mut entry = TieredEntry::new("value".into(), start, ttl);
    entry.hit_count = u32::MAX;
    entry.expires_at = start + ttl * 4;
    entry.record_hit(ttl);
    assert_eq!(entry.hit_count, u32::MAX);
    assert_eq!(entry.expires_at, start + ttl * 4);
}

#[test]
fn a_single_read_does_not_restart_cold_ttl() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(2, Duration::from_secs(10));
    cache.put("a".into(), "value".into(), start);
    assert_eq!(
        cache.get("a", start + Duration::from_secs(9)),
        Some("value".into())
    );
    assert_eq!(cache.get("a", start + Duration::from_secs(10)), None);
    assert_consistent(&cache);
}

#[test]
fn warm_promotion_uses_fill_time_not_read_time() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(2, Duration::from_secs(10));
    cache.put("a".into(), "value".into(), start);
    assert_eq!(
        cache.get("a", start + Duration::from_secs(8)),
        Some("value".into())
    );
    assert_eq!(
        cache.get("a", start + Duration::from_secs(9)),
        Some("value".into())
    );
    assert_eq!(
        cache.entries["a"].expires_at,
        start + Duration::from_secs(20)
    );
    assert_eq!(
        cache.get("a", start + Duration::from_secs(19)),
        Some("value".into())
    );
    assert_eq!(cache.get("a", start + Duration::from_secs(20)), None);
    assert_consistent(&cache);
}

#[test]
fn the_eighth_hit_promotes_warm_to_hot_but_continuous_reads_cannot_extend_past_four_ttl() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(2, Duration::from_secs(10));
    cache.put("a".into(), "value".into(), start);
    for _ in 0..7 {
        assert!(cache.get("a", start + Duration::from_secs(1)).is_some());
    }
    assert_eq!(
        cache.entries["a"].expires_at,
        start + Duration::from_secs(20)
    );
    assert!(cache.get("a", start + Duration::from_secs(19)).is_some());
    assert_eq!(
        cache.entries["a"].expires_at,
        start + Duration::from_secs(40)
    );
    for second in 20..40 {
        assert!(cache
            .get("a", start + Duration::from_secs(second))
            .is_some());
    }
    assert_eq!(cache.get("a", start + Duration::from_secs(40)), None);
    assert_consistent(&cache);
}

#[test]
fn an_expired_entry_cannot_be_promoted_at_either_threshold() {
    let start = Instant::now();
    for (prior_hits, deadline_secs) in [(1, 10), (7, 20)] {
        let mut cache = TtlTieredState::new(2, Duration::from_secs(10));
        cache.put("a".into(), "value".into(), start);
        for _ in 0..prior_hits {
            assert!(cache.get("a", start).is_some());
        }
        assert_eq!(
            cache.get("a", start + Duration::from_secs(deadline_secs)),
            None
        );
        assert!(cache.entries.is_empty());
        assert_consistent(&cache);
    }
}

#[test]
fn a_hot_key_can_be_evicted_by_capacity() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(2, Duration::from_secs(10));
    cache.put("hot".into(), "value".into(), start);
    for _ in 0..8 {
        assert!(cache.get("hot", start).is_some());
    }
    cache.put("b".into(), "b".into(), start);
    cache.put("c".into(), "c".into(), start);
    assert_eq!(cache.get("hot", start), None);
    assert_eq!(cache.get("b", start), Some("b".into()));
    assert_eq!(cache.get("c", start), Some("c".into()));
    assert_consistent(&cache);
}

#[test]
fn reads_refresh_lru_order_across_the_shared_store() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(2, Duration::from_secs(10));
    cache.put("a".into(), "a".into(), start);
    cache.put("b".into(), "b".into(), start);
    assert!(cache.get("a", start).is_some());
    assert_eq!(cache.get("missing", start), None);
    cache.put("c".into(), "c".into(), start);
    assert_eq!(cache.get("b", start), None);
    assert!(cache.get("a", start).is_some());
    assert_consistent(&cache);
}

#[test]
fn replacement_resets_popularity_and_does_not_evict_another_key() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(2, Duration::from_secs(10));
    cache.put("a".into(), "old".into(), start);
    for _ in 0..8 {
        assert!(cache.get("a", start).is_some());
    }
    cache.put("b".into(), "b".into(), start);
    let refill = start + Duration::from_secs(5);
    cache.put("a".into(), "new".into(), refill);
    assert_eq!(cache.entries["a"].hit_count, 0);
    assert_eq!(cache.entries["a"].filled_at, refill);
    assert_eq!(
        cache.entries["a"].expires_at,
        refill + Duration::from_secs(10)
    );
    assert!(cache.entries.contains_key("b"));
    assert_eq!(cache.get("a", refill), Some("new".into()));
    assert_consistent(&cache);
}

#[test]
fn refill_after_expiration_starts_cold() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(2, Duration::from_secs(10));
    cache.put("a".into(), "old".into(), start);
    for _ in 0..8 {
        assert!(cache.get("a", start).is_some());
    }
    let refill = start + Duration::from_secs(40);
    assert_eq!(cache.get("a", refill), None);
    cache.put("a".into(), "new".into(), refill);
    assert_eq!(cache.entries["a"].hit_count, 0);
    assert_eq!(
        cache.entries["a"].expires_at,
        refill + Duration::from_secs(10)
    );
    assert_consistent(&cache);
}

#[test]
fn cleanup_obeys_each_class_deadline_without_resetting_survivors() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(3, Duration::from_secs(10));
    for (key, hits) in [("cold", 0), ("warm", 2), ("hot", 8)] {
        cache.put(key.into(), key.into(), start);
        for _ in 0..hits {
            assert!(cache.get(key, start).is_some());
        }
    }
    for (second, expected_live) in [(10, 2), (20, 1), (40, 0)] {
        let now = start + Duration::from_secs(second);
        assert_eq!(cache.live_len(now), expected_live);
        cache.evict_expired(now);
        assert_eq!(cache.entries.len(), expected_live);
        assert_consistent(&cache);
    }
}

#[test]
fn deletion_and_reinsertion_do_not_duplicate_order_entries() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(3, Duration::from_secs(10));
    for key in ["a", "b", "c"] {
        cache.put(key.into(), key.into(), start);
    }
    for key in ["b", "a", "c"] {
        assert!(cache.delete(key));
        assert!(!cache.delete(key));
        cache.put(key.into(), "new".into(), start);
        assert_consistent(&cache);
    }
}

#[test]
fn zero_and_one_capacity_respect_the_single_budget() {
    let start = Instant::now();
    for capacity in [0, 1] {
        let mut cache = TtlTieredState::new(capacity, Duration::from_secs(10));
        cache.put("a".into(), "a".into(), start);
        cache.put("b".into(), "b".into(), start);
        assert_eq!(cache.get("a", start), None);
        assert_eq!(
            cache.get("b", start),
            (capacity == 1).then(|| "b".to_string())
        );
        assert_consistent(&cache);
    }
}

#[test]
fn zero_ttl_expires_immediately() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(1, Duration::ZERO);
    cache.put("a".into(), "a".into(), start);
    assert_eq!(cache.live_len(start), 0);
    assert_eq!(cache.get("a", start), None);
    assert_consistent(&cache);
}

#[test]
fn public_clones_and_trait_methods_share_one_cache() {
    let cache = TtlTieredCache::new(Duration::from_secs(3600), 2);
    let clone = cache.clone();
    let backend: &dyn CacheBackend = &clone;
    cache.put("a".into(), "value".into());
    assert_eq!(backend.get("a"), Some("value".into()));
    assert_eq!(backend.live_len(), 1);
    backend.evict_expired();
    assert!(backend.delete("a"));
    assert_eq!(cache.get("a"), None);
    assert_consistent(&cache.state.lock().unwrap());
}
