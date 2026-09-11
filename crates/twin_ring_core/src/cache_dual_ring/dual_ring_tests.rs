//! Dual-ring strategy unit tests.
//!
//! These tests cover only deterministic key placement. They do not need
//! Cassandra, HTTP, Docker, or a Tokio runtime.

use super::{DualRing, DualRingInner, KeyPlacement, ReplicateEntry, SnapshotVersion};
use std::time::{Duration, Instant};

/// Test convenience only. Production code constructs one `DualRing` when the
/// membership view changes and reuses it for every lookup.
fn placement_for(key: &str, server_count: usize) -> Option<KeyPlacement> {
    DualRing::new(0..server_count).map(|ring| ring.placement_for(key))
}

fn three_node_ring() -> DualRing {
    DualRing::new([0, 1, 2]).expect("three servers can form two rings")
}

fn version(sequence: u64) -> SnapshotVersion {
    SnapshotVersion {
        epoch_ms: 1,
        sequence,
    }
}

#[test]
fn the_same_key_always_has_the_same_primary_and_backup() {
    let first = placement_for("account:42", 3).expect("three servers can form two rings");
    let second = placement_for("account:42", 3).expect("three servers can form two rings");

    assert_eq!(first, second);
}

#[test]
fn primary_and_backup_are_both_real_servers_and_are_distinct() {
    for key_number in 0..1_000 {
        let key = format!("key{key_number}");
        let placement = placement_for(&key, 3).expect("three servers can form two rings");

        assert!(placement.primary < 3);
        assert!(placement.backup < 3);
        assert_ne!(placement.primary, placement.backup);
    }
}

#[test]
fn placement_works_for_keys_that_do_not_follow_the_experiment_key_name() {
    let placement =
        placement_for("customer/42/preferences", 3).expect("three servers can form two rings");

    assert!(placement.primary < 3);
    assert!(placement.backup < 3);
}

#[test]
fn a_backup_requires_at_least_two_servers() {
    assert_eq!(placement_for("key42", 0), None);
    assert_eq!(placement_for("key42", 1), None);
}

#[test]
fn membership_order_does_not_change_the_ring() {
    let ordered = DualRing::new([0, 1, 2]).expect("three members");
    let reordered = DualRing::new([2, 0, 1]).expect("three members");

    assert_eq!(ordered.members(), &[0, 1, 2]);
    assert_eq!(ordered.members(), reordered.members());
    for key_number in 0..1_000 {
        let key = format!("key{key_number}");
        assert_eq!(ordered.placement_for(&key), reordered.placement_for(&key));
    }
}

#[test]
fn removing_a_server_preserves_l1_owners_for_keys_it_did_not_own() {
    let before = DualRing::new([0, 1, 2]).expect("three members");
    let after = DualRing::new([0, 2]).expect("two remaining members");
    let mut unaffected = 0;
    let mut moved_from_removed_server = 0;

    for key_number in 0..10_000 {
        let key = format!("key{key_number}");
        let old = before.placement_for(&key);
        let new = after.placement_for(&key);
        if old.primary == 1 {
            moved_from_removed_server += 1;
            assert!(matches!(new.primary, 0 | 2));
        } else {
            unaffected += 1;
            assert_eq!(new.primary, old.primary, "unrelated key {key} moved");
        }
    }

    assert!(unaffected > 0);
    assert!(moved_from_removed_server > 0);
}

#[test]
fn l1_read_updates_hotness_and_recency_but_not_the_ttl() {
    let start = Instant::now();
    let mut cache = DualRingInner::new(2, Duration::from_secs(10));
    cache.put_at("a".to_string(), "value-a".to_string(), start);
    cache.put_at("b".to_string(), "value-b".to_string(), start);

    assert_eq!(
        cache.main_cache_get_at("a", start + Duration::from_secs(9)),
        Some("value-a".to_string())
    );
    assert_eq!(cache.main_map["a"].access_count, 1);
    assert_eq!(
        cache.main_order.iter().collect::<Vec<_>>(),
        vec![&"b", &"a"]
    );

    assert_eq!(
        cache.main_cache_get_at("a", start + Duration::from_secs(10)),
        None
    );
}

#[test]
fn l1_capacity_evicts_the_least_recently_used_entry() {
    let start = Instant::now();
    let mut cache = DualRingInner::new(2, Duration::from_secs(10));
    cache.put_at("a".to_string(), "value-a".to_string(), start);
    cache.put_at("b".to_string(), "value-b".to_string(), start);
    cache.main_cache_get_at("a", start + Duration::from_millis(1));

    cache.put_at(
        "c".to_string(),
        "value-c".to_string(),
        start + Duration::from_millis(2),
    );

    assert!(cache.main_map.contains_key("a"));
    assert!(!cache.main_map.contains_key("b"));
    assert!(cache.main_map.contains_key("c"));
}

#[test]
fn hot_key_selection_uses_live_entries_and_a_deterministic_tie_breaker() {
    let start = Instant::now();
    let mut cache = DualRingInner::new(4, Duration::from_secs(10));
    for key in ["c", "a", "b"] {
        cache.put_at(key.to_string(), format!("value-{key}"), start);
    }

    cache.main_cache_get_at("b", start + Duration::from_millis(1));
    cache.main_cache_get_at("b", start + Duration::from_millis(2));
    cache.main_cache_get_at("a", start + Duration::from_millis(3));

    assert_eq!(
        cache.top_k_hot_at(3, start + Duration::from_secs(1)),
        vec![
            ("b".to_string(), "value-b".to_string()),
            ("a".to_string(), "value-a".to_string()),
            ("c".to_string(), "value-c".to_string()),
        ]
    );
    assert!(cache
        .top_k_hot_at(3, start + Duration::from_secs(10))
        .is_empty());
}

#[test]
fn a_replication_snapshot_replaces_only_that_primarys_previous_snapshot() {
    let mut cache = DualRingInner::new(4, Duration::from_secs(10));
    cache.receive_replicate(
        1,
        version(1),
        vec![ReplicateEntry {
            key: "old-hot".to_string(),
            value: "old-value".to_string(),
        }],
    );
    cache.receive_replicate(
        2,
        version(1),
        vec![ReplicateEntry {
            key: "other-primary-key".to_string(),
            value: "other-value".to_string(),
        }],
    );

    cache.receive_replicate(
        1,
        version(2),
        vec![ReplicateEntry {
            key: "new-hot".to_string(),
            value: "new-value".to_string(),
        }],
    );

    assert_eq!(cache.hot_store.get_from(1, "old-hot"), None);
    assert_eq!(
        cache.hot_store.get_from(1, "new-hot"),
        Some("new-value".to_string())
    );
    assert_eq!(
        cache.hot_store.get_from(2, "other-primary-key"),
        Some("other-value".to_string())
    );
}

#[test]
fn promoting_a_replica_copies_only_the_requested_key_into_l1() {
    let start = Instant::now();
    let mut cache = DualRingInner::new(3, Duration::from_secs(10));
    cache.put_at("native".to_string(), "native-value".to_string(), start);
    cache.receive_replicate(
        1,
        version(1),
        vec![
            ReplicateEntry {
                key: "hot-a".to_string(),
                value: "replica-a".to_string(),
            },
            ReplicateEntry {
                key: "hot-b".to_string(),
                value: "replica-b".to_string(),
            },
        ],
    );

    assert_eq!(
        cache.promote_requested_replica(1, "hot-a", start + Duration::from_secs(1)),
        Some("replica-a".to_string())
    );
    assert!(cache.main_map.contains_key("native"));
    assert!(cache.main_map.contains_key("hot-a"));
    assert!(!cache.main_map.contains_key("hot-b"));
    assert_eq!(cache.main_map["hot-a"].access_count, 1);
    assert_eq!(
        cache.hot_store.get_from(1, "hot-b"),
        Some("replica-b".to_string())
    );
}

#[test]
fn only_the_designated_l2_backup_can_promote_a_replica() {
    let start = Instant::now();
    let key = "customer/42/preferences";
    let ring = three_node_ring();
    let placement = ring.placement_for(key);

    let mut backup_cache = DualRingInner::new(3, Duration::from_secs(10));
    backup_cache.receive_replicate(
        placement.primary,
        version(1),
        vec![ReplicateEntry {
            key: key.to_string(),
            value: "replica-value".to_string(),
        }],
    );

    assert_eq!(
        backup_cache.l2_lookup_at(key, placement.backup, &ring, start),
        Some("replica-value".to_string())
    );

    let mut primary_cache = DualRingInner::new(3, Duration::from_secs(10));
    primary_cache.receive_replicate(
        placement.primary,
        version(1),
        vec![ReplicateEntry {
            key: key.to_string(),
            value: "replica-value".to_string(),
        }],
    );
    assert_eq!(
        primary_cache.l2_lookup_at(key, placement.primary, &ring, start),
        None
    );
    assert!(!primary_cache.main_map.contains_key(key));

    let other_server = (0..3)
        .find(|server| *server != placement.primary && *server != placement.backup)
        .expect("three servers leave one uninvolved server");
    let mut other_cache = DualRingInner::new(3, Duration::from_secs(10));
    other_cache.receive_replicate(
        placement.primary,
        version(1),
        vec![ReplicateEntry {
            key: key.to_string(),
            value: "replica-value".to_string(),
        }],
    );
    assert_eq!(
        other_cache.l2_lookup_at(key, other_server, &ring, start),
        None
    );
    assert!(!other_cache.main_map.contains_key(key));
}

#[test]
fn replication_sends_only_owned_hot_keys_to_their_designated_backups() {
    let start = Instant::now();
    let ring = three_node_ring();
    let mut owned_keys = Vec::new();
    let mut foreign_key = None;
    for key_number in 0..1_000 {
        let key = format!("key{key_number}");
        let placement = placement_for(&key, 3).expect("three servers can form two rings");
        if placement.primary == 0 && owned_keys.len() < 2 {
            owned_keys.push(key.clone());
        }
        if placement.primary != 0 && foreign_key.is_none() {
            foreign_key = Some(key);
        }
        if owned_keys.len() == 2 && foreign_key.is_some() {
            break;
        }
    }
    let foreign_key = foreign_key.expect("a three-server ring has foreign keys");

    let mut cache = DualRingInner::new(4, Duration::from_secs(10));
    for key in owned_keys.iter().chain(std::iter::once(&foreign_key)) {
        cache.put_at(key.clone(), format!("value-{key}"), start);
    }

    // This foreign replica is more popular locally, but it must not consume one
    // of primary 0's replication slots.
    for _ in 0..10 {
        cache.main_cache_get_at(&foreign_key, start + Duration::from_millis(1));
    }
    for _ in 0..3 {
        cache.main_cache_get_at(&owned_keys[1], start + Duration::from_millis(2));
    }
    cache.main_cache_get_at(&owned_keys[0], start + Duration::from_millis(3));

    let batches = cache.replication_batches_at(2, 0, &ring, start + Duration::from_secs(1));
    let replicated: Vec<&ReplicateEntry> = batches
        .iter()
        .flat_map(|batch| batch.entries.iter())
        .collect();

    assert_eq!(replicated.len(), 2);
    assert!(replicated.iter().all(|entry| entry.key != foreign_key));
    for batch in &batches {
        for entry in &batch.entries {
            let placement = placement_for(&entry.key, 3).expect("three servers can form two rings");
            assert_eq!(batch.target, placement.backup);
            assert_ne!(batch.target, 0);
        }
    }
}

#[test]
fn an_empty_snapshot_clears_replicas_that_are_no_longer_hot() {
    let start = Instant::now();
    let ring = three_node_ring();
    let (key, placement) = (0..1_000)
        .map(|key_number| {
            let key = format!("key{key_number}");
            let placement = placement_for(&key, 3).expect("three servers can form two rings");
            (key, placement)
        })
        .find(|(_, placement)| placement.primary == 0)
        .expect("a three-server ring has keys owned by server 0");

    let primary = DualRingInner::new(3, Duration::from_secs(10));
    // No owned L1 values are selected, so every peer receives an empty snapshot.
    let batches = primary.replication_batches_at(10, 0, &ring, start);
    let empty_batch = batches
        .iter()
        .find(|batch| batch.target == placement.backup)
        .expect("the designated backup receives a snapshot each interval");
    assert!(empty_batch.entries.is_empty());

    let mut backup = DualRingInner::new(3, Duration::from_secs(10));
    backup.receive_replicate(
        placement.primary,
        version(1),
        vec![ReplicateEntry {
            key: key.clone(),
            value: "stale-replica".to_string(),
        }],
    );
    assert_eq!(
        backup.hot_store.get_from(placement.primary, &key),
        Some("stale-replica".to_string())
    );

    backup.receive_replicate(placement.primary, version(2), empty_batch.entries.clone());
    assert_eq!(backup.hot_store.get_from(placement.primary, &key), None);
}

#[test]
fn an_older_delayed_snapshot_cannot_restore_stale_replicas() {
    let mut backup = DualRingInner::new(3, Duration::from_secs(10));
    let primary = 0;

    assert!(backup.receive_replicate(
        primary,
        version(2),
        vec![ReplicateEntry {
            key: "new-hot".to_string(),
            value: "new-value".to_string(),
        }],
    ));
    assert!(!backup.receive_replicate(
        primary,
        version(1),
        vec![ReplicateEntry {
            key: "old-hot".to_string(),
            value: "old-value".to_string(),
        }],
    ));

    assert_eq!(
        backup.hot_store.get_from(primary, "new-hot"),
        Some("new-value".to_string())
    );
    assert_eq!(backup.hot_store.get_from(primary, "old-hot"), None);
}

#[test]
fn a_newer_process_epoch_can_replace_a_pre_restart_snapshot() {
    let mut backup = DualRingInner::new(3, Duration::from_secs(10));
    let primary = 0;
    let before_restart = SnapshotVersion {
        epoch_ms: 10,
        sequence: 99,
    };
    let after_restart = SnapshotVersion {
        epoch_ms: 11,
        sequence: 1,
    };

    assert!(backup.receive_replicate(
        primary,
        before_restart,
        vec![ReplicateEntry {
            key: "old-hot".to_string(),
            value: "old-value".to_string(),
        }],
    ));
    assert!(backup.receive_replicate(
        primary,
        after_restart,
        vec![ReplicateEntry {
            key: "new-hot".to_string(),
            value: "new-value".to_string(),
        }],
    ));

    assert_eq!(backup.hot_store.get_from(primary, "old-hot"), None);
    assert_eq!(
        backup.hot_store.get_from(primary, "new-hot"),
        Some("new-value".to_string())
    );
}
