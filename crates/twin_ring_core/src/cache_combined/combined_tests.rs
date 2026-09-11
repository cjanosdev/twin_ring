//! Combined strategy unit tests.
//!
//! Pure Combined L1 tests. No Cassandra, HTTP, Docker, lease, or L2 behavior.

use super::{CombinedInner, CombinedLookup};
use std::time::{Duration, Instant};

#[test]
fn l1_second_and_eighth_reads_promote_ttl_from_the_original_fill_time() {
    let start = Instant::now();
    let mut cache = CombinedInner::new(2, Duration::from_secs(10));
    cache.put_at("key".into(), "value".into(), start);

    cache.get_at("key", start + Duration::from_secs(1));
    assert_eq!(
        cache.entries["key"].expires_at,
        start + Duration::from_secs(10)
    );
    cache.get_at("key", start + Duration::from_secs(2));
    assert_eq!(
        cache.entries["key"].expires_at,
        start + Duration::from_secs(20)
    );

    for second in 3..=8 {
        cache.get_at("key", start + Duration::from_secs(second));
    }
    assert_eq!(
        cache.entries["key"].expires_at,
        start + Duration::from_secs(40)
    );
}

#[test]
fn l1_reads_update_lru_order_and_capacity_evicts_the_oldest_key() {
    let start = Instant::now();
    let mut cache = CombinedInner::new(2, Duration::from_secs(10));
    cache.put_at("a".into(), "a".into(), start);
    cache.put_at("b".into(), "b".into(), start);
    cache.get_at("a", start + Duration::from_millis(1));
    cache.put_at("c".into(), "c".into(), start + Duration::from_millis(2));

    assert!(cache.entries.contains_key("a"));
    assert!(!cache.entries.contains_key("b"));
    assert!(cache.entries.contains_key("c"));
}

#[test]
fn l1_read_does_not_extend_an_entry_before_a_promotion_threshold() {
    let start = Instant::now();
    let mut cache = CombinedInner::new(1, Duration::from_secs(10));
    cache.put_at("key".into(), "value".into(), start);

    assert_eq!(
        cache.get_at("key", start + Duration::from_secs(9)),
        Some("value".into())
    );
    assert_eq!(cache.get_at("key", start + Duration::from_secs(10)), None);
}

#[test]
fn first_miss_gets_a_lease_and_a_second_miss_is_held() {
    let start = Instant::now();
    let mut cache = CombinedInner::new(1, Duration::from_secs(10));
    assert!(matches!(
        cache.lookup_or_acquire_at("key", start),
        CombinedLookup::LeaseGranted(_)
    ));
    assert_eq!(
        cache.lookup_or_acquire_at("key", start),
        CombinedLookup::LeaseHeld
    );
}

#[test]
fn only_the_current_lease_can_fill_and_then_read_as_a_hit() {
    let start = Instant::now();
    let mut cache = CombinedInner::new(1, Duration::from_secs(10));
    let CombinedLookup::LeaseGranted(lease) = cache.lookup_or_acquire_at("key", start) else {
        panic!("first miss grants")
    };
    assert!(cache.complete_fill_at(&lease, "value".into(), start + Duration::from_millis(1)));
    assert_eq!(
        cache.lookup_or_acquire_at("key", start + Duration::from_millis(2)),
        CombinedLookup::Hit("value".into())
    );
}

#[test]
fn expired_lease_can_be_replaced_but_cannot_fill_afterwards() {
    let start = Instant::now();
    let mut cache = CombinedInner::new(1, Duration::from_secs(10));
    let CombinedLookup::LeaseGranted(old) = cache.lookup_or_acquire_at("key", start) else {
        panic!("first miss grants")
    };
    let later = start + Duration::from_millis(501);
    let CombinedLookup::LeaseGranted(new) = cache.lookup_or_acquire_at("key", later) else {
        panic!("expired lease is replaced")
    };
    assert!(!cache.complete_fill_at(&old, "old".into(), later));
    assert!(cache.complete_fill_at(&new, "new".into(), later));
}
