use super::*;

fn granted(result: LeaseAcquisition) -> Lease {
    match result {
        LeaseAcquisition::Granted(lease) => lease,
        LeaseAcquisition::Held => panic!("expected lease to be granted"),
    }
}

#[test]
fn lookup_outcomes_are_distinct() {
    assert_eq!(
        LeaseLookup::Hit("value".into()),
        LeaseLookup::Hit("value".into())
    );
    let lease = Lease {
        key: "key".into(),
        id: 1,
    };
    assert_ne!(
        LeaseLookup::Hit("value".into()),
        LeaseLookup::LeaseGranted(lease.clone())
    );
    assert_ne!(LeaseLookup::LeaseGranted(lease), LeaseLookup::LeaseHeld);
}

#[test]
fn first_request_gets_a_lease_and_second_request_for_same_key_is_held() {
    let now = Instant::now();
    let mut registry = LeaseRegistry::new(Duration::from_secs(10));

    let first = granted(registry.acquire("profile:42", now));
    assert_eq!(first.key, "profile:42");
    assert_eq!(registry.acquire("profile:42", now), LeaseAcquisition::Held);
}

#[test]
fn different_keys_can_hold_leases_independently() {
    let now = Instant::now();
    let mut registry = LeaseRegistry::new(Duration::from_secs(10));

    let first = granted(registry.acquire("profile:42", now));
    let second = granted(registry.acquire("profile:99", now));

    assert_ne!(first.id, second.id);
    assert_eq!(registry.active.len(), 2);
}

#[test]
fn an_expired_lease_can_be_replaced() {
    let start = Instant::now();
    let mut registry = LeaseRegistry::new(Duration::from_secs(10));

    let first = granted(registry.acquire("profile:42", start));
    assert_eq!(
        registry.acquire("profile:42", start + Duration::from_secs(9)),
        LeaseAcquisition::Held
    );

    let replacement = granted(registry.acquire("profile:42", start + Duration::from_secs(10)));
    assert_ne!(first.id, replacement.id);
}

#[test]
fn an_old_lease_cannot_clear_its_replacement() {
    let start = Instant::now();
    let mut registry = LeaseRegistry::new(Duration::from_secs(10));

    let old = granted(registry.acquire("profile:42", start));
    let replacement = granted(registry.acquire("profile:42", start + Duration::from_secs(10)));

    assert!(!registry.release(&old));
    assert_eq!(registry.active["profile:42"].id, replacement.id);
    assert!(registry.release(&replacement));
    assert!(registry.active.is_empty());
}

#[test]
fn releasing_the_current_lease_immediately_allows_another_request() {
    let now = Instant::now();
    let mut registry = LeaseRegistry::new(Duration::from_secs(10));

    let first = granted(registry.acquire("profile:42", now));
    assert!(registry.release(&first));
    assert!(matches!(
        registry.acquire("profile:42", now),
        LeaseAcquisition::Granted(_)
    ));
}

fn leased_state() -> LeasedState {
    LeasedState::new(60, Duration::from_secs(10), Duration::from_secs(5))
}

#[test]
fn lookup_returns_a_cached_value_before_consulting_leases() {
    let now = Instant::now();
    let mut state = leased_state();
    state.cache.put("profile:42".into(), "cached".into());
    let lease = granted(state.leases.acquire("profile:42", now));

    assert_eq!(
        state.lookup_or_acquire("profile:42", now),
        LeaseLookup::Hit("cached".into())
    );
    assert_eq!(state.leases.active["profile:42"].id, lease.id);
}

#[test]
fn first_miss_is_granted_and_the_next_same_key_miss_is_held() {
    let now = Instant::now();
    let mut state = leased_state();

    let first = state.lookup_or_acquire("profile:42", now);
    let LeaseLookup::LeaseGranted(lease) = first else {
        panic!("first cache miss should receive a lease");
    };
    assert_eq!(lease.key, "profile:42");
    assert_eq!(
        state.lookup_or_acquire("profile:42", now),
        LeaseLookup::LeaseHeld
    );
}

#[test]
fn different_missing_keys_are_granted_independent_leases() {
    let now = Instant::now();
    let mut state = leased_state();

    let LeaseLookup::LeaseGranted(first) = state.lookup_or_acquire("profile:42", now) else {
        panic!("first key should receive a lease");
    };
    let LeaseLookup::LeaseGranted(second) = state.lookup_or_acquire("profile:99", now) else {
        panic!("second key should receive a lease");
    };
    assert_ne!(first.id, second.id);
}

#[test]
fn an_expired_lease_is_replaced_by_a_new_lookup() {
    let start = Instant::now();
    let mut state = leased_state();

    let LeaseLookup::LeaseGranted(first) = state.lookup_or_acquire("profile:42", start) else {
        panic!("first key should receive a lease");
    };
    assert_eq!(
        state.lookup_or_acquire("profile:42", start + Duration::from_secs(4)),
        LeaseLookup::LeaseHeld
    );
    let LeaseLookup::LeaseGranted(replacement) =
        state.lookup_or_acquire("profile:42", start + Duration::from_secs(5))
    else {
        panic!("expired lease should be replaced");
    };
    assert_ne!(first.id, replacement.id);
}

#[test]
fn completing_a_current_lease_fills_the_cache_and_clears_the_lease() {
    let start = Instant::now();
    let mut state = leased_state();
    let LeaseLookup::LeaseGranted(lease) = state.lookup_or_acquire("profile:42", start) else {
        panic!("first cache miss should receive a lease");
    };

    assert!(state.complete_fill(&lease, "fetched".into(), start + Duration::from_secs(1)));
    assert!(state.leases.active.is_empty());
    assert_eq!(
        state.lookup_or_acquire("profile:42", start + Duration::from_secs(1)),
        LeaseLookup::Hit("fetched".into())
    );
}

#[test]
fn an_expired_lease_cannot_fill_the_cache() {
    let start = Instant::now();
    let mut state = leased_state();
    let LeaseLookup::LeaseGranted(lease) = state.lookup_or_acquire("profile:42", start) else {
        panic!("first cache miss should receive a lease");
    };

    assert!(!state.complete_fill(&lease, "late result".into(), start + Duration::from_secs(5)));
    assert!(state.cache.map.is_empty());
    assert!(matches!(
        state.lookup_or_acquire("profile:42", start + Duration::from_secs(5)),
        LeaseLookup::LeaseGranted(_)
    ));
}

#[test]
fn an_old_lease_cannot_overwrite_a_newer_fill() {
    let start = Instant::now();
    let mut state = leased_state();
    let LeaseLookup::LeaseGranted(old) = state.lookup_or_acquire("profile:42", start) else {
        panic!("first cache miss should receive a lease");
    };
    let LeaseLookup::LeaseGranted(current) =
        state.lookup_or_acquire("profile:42", start + Duration::from_secs(5))
    else {
        panic!("expired lease should be replaced");
    };

    assert!(state.complete_fill(
        &current,
        "new result".into(),
        start + Duration::from_secs(6)
    ));
    assert!(!state.complete_fill(&old, "old result".into(), start + Duration::from_secs(6)));
    assert_eq!(
        state.lookup_or_acquire("profile:42", start + Duration::from_secs(6)),
        LeaseLookup::Hit("new result".into())
    );
}

#[test]
fn abandoning_a_failed_fill_releases_only_its_own_lease() {
    let start = Instant::now();
    let mut state = leased_state();
    let LeaseLookup::LeaseGranted(lease) = state.lookup_or_acquire("profile:42", start) else {
        panic!("first cache miss should receive a lease");
    };

    assert!(state.abandon_fill(&lease));
    assert!(matches!(
        state.lookup_or_acquire("profile:42", start),
        LeaseLookup::LeaseGranted(_)
    ));
}

#[test]
fn an_old_failed_fill_cannot_release_a_replacement_lease() {
    let start = Instant::now();
    let mut state = leased_state();
    let LeaseLookup::LeaseGranted(old) = state.lookup_or_acquire("profile:42", start) else {
        panic!("first cache miss should receive a lease");
    };
    let LeaseLookup::LeaseGranted(replacement) =
        state.lookup_or_acquire("profile:42", start + Duration::from_secs(5))
    else {
        panic!("expired lease should be replaced");
    };

    assert!(!state.abandon_fill(&old));
    assert_eq!(state.leases.active["profile:42"].id, replacement.id);
}

#[test]
fn public_cache_uses_the_configured_lease_duration_and_shares_state_across_clones() {
    let cache =
        LeasedCache::with_lease_duration(Duration::from_secs(60), 2, Duration::from_millis(5));
    let clone = cache.clone();

    let LeaseLookup::LeaseGranted(lease) = cache.lookup_or_acquire("profile:42") else {
        panic!("first cache miss should receive a lease");
    };
    assert_eq!(
        clone.lookup_or_acquire("profile:42"),
        LeaseLookup::LeaseHeld
    );
    assert!(clone.complete_fill(&lease, "fetched".into()));
    assert_eq!(
        cache.lookup_or_acquire("profile:42"),
        LeaseLookup::Hit("fetched".into())
    );
}
