/// Dual-ring hash cache with proactive hot-key replication.
///
/// Architecture:
/// - **L1 ring (main cache)**: LRU+TTL store for keys this node owns.
///   Access counts are tracked per entry for hot-key identification.
/// - **L2 ring (hot store)**: A separate in-RAM store of replicated hot keys
///   received from peer nodes. Not subject to TTL or LRU — replaced wholesale
///   on each replication interval by the owning node.
///
/// On node failure:
///   Client gets a connection error from the L1 node, calculates the L2 backup
///   with `placement_for`, and calls that node's `/backup/{key}` endpoint.
///   The backup promotes the requested replica into L1, then serves it without
///   sending a Cassandra query.
///
/// Replication (background task, configurable interval):
///   1. Scan main cache, pick top-K entries by access_count.
///   2. For each hot key, compute its L2 backup node (second hash).
///   3. POST the key+value list to each backup node's /replicate endpoint.
///   4. Backup node replaces its hot store for the sending node atomically.
///
/// Future extension points (not implemented):
/// - TODO: quorum check before promotion (false-positive guard)
/// - TODO: ML-based hot-key selection (access_count is the raw signal)
/// - TODO: bloom filter per replication batch for client-side absence hints
use crate::cache_backend::CacheBackend;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod placement;
pub use placement::{DualRing, KeyPlacement, ServerIndex};

// ── Types ─────────────────────────────────────────────────────────────────────

/// Zero-based identity of the source primary for one replica snapshot.
type NodeId = ServerIndex;

/// A main-cache entry: value, expiry, and a hit counter for hot-key selection.
struct MainEntry {
    value: String,
    expires_at: Instant,
    access_count: u64,
}

/// The per-node hot store: keyed by source NodeId → their replicated hot items.
/// Replaced wholesale on each replication update — no TTL, no LRU.
#[derive(Default)]
struct HotStore {
    by_node: HashMap<NodeId, ReplicaSnapshot>,
}

struct ReplicaSnapshot {
    version: SnapshotVersion,
    entries: HashMap<String, String>,
}

impl HotStore {
    /// Replace the complete replica snapshot received from one L1 primary.
    ///
    /// Replication is a snapshot rather than a merge: keys that have fallen out
    /// of a primary's top-K list must also disappear from this backup store.
    fn replace_from(
        &mut self,
        primary: NodeId,
        version: SnapshotVersion,
        entries: Vec<ReplicateEntry>,
    ) -> bool {
        if self
            .by_node
            .get(&primary)
            .is_some_and(|current| version <= current.version)
        {
            return false;
        }

        let entries = entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect();
        self.by_node
            .insert(primary, ReplicaSnapshot { version, entries });
        true
    }

    /// Return one replica owned by `primary`.
    ///
    /// The clone gives the caller an owned value, so it can promote that value
    /// into L1 after this immutable borrow of the replica store has ended.
    fn get_from(&self, primary: NodeId, key: &str) -> Option<String> {
        self.by_node.get(&primary)?.entries.get(key).cloned()
    }
}

struct DualRingInner {
    // L1: main LRU+TTL cache
    main_map: HashMap<String, MainEntry>,
    main_order: VecDeque<String>, // front=LRU, back=MRU
    max_entries: usize,
    default_ttl: Duration,

    // L2: hot store (replication targets from peer nodes)
    hot_store: HotStore,
}

pub struct DualRingCache {
    inner: Arc<Mutex<DualRingInner>>,
    top_k: usize,
    server_index: ServerIndex,
    ring: Arc<RwLock<Arc<DualRing>>>,
}

// ── Wire types for /replicate endpoint ────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicateEntry {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize)]
pub struct ReplicatePayload {
    pub from_node: NodeId,
    pub version: SnapshotVersion,
    pub entries: Vec<ReplicateEntry>,
}

/// Monotonically ordered identity for one primary's complete replica snapshot.
///
/// `epoch_ms` changes when a primary process restarts; `sequence` increases for
/// every snapshot created within that process. Backups use the pair to reject a
/// delayed older snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SnapshotVersion {
    pub epoch_ms: u64,
    pub sequence: u64,
}

/// One outbound replica snapshot batch for a designated level-2 backup.
#[derive(Debug, PartialEq, Eq)]
struct ReplicationBatch {
    target: ServerIndex,
    entries: Vec<ReplicateEntry>,
}

/// Diagnostic snapshot returned by `GET /replicate-stats`.
#[derive(Serialize)]
pub struct ReplicateStats {
    pub hot_store_total: usize,
    pub hot_store_by_node: HashMap<NodeId, usize>,
    pub top5_main_hot_keys: Vec<(String, u64)>,
    pub main_cache_live: usize,
}

// ── DualRingInner methods ──────────────────────────────────────────────────────

impl DualRingInner {
    fn new(max_entries: usize, default_ttl: Duration) -> Self {
        DualRingInner {
            main_map: HashMap::with_capacity(max_entries + 1),
            main_order: VecDeque::with_capacity(max_entries + 1),
            max_entries,
            default_ttl,
            hot_store: HotStore::default(),
        }
    }

    /// Check only L1 main cache using the real clock.
    ///
    /// Production reads come through here. Tests use `main_cache_get_at` below
    /// so expiration can be checked at exact, deterministic moments.
    fn main_cache_get(&mut self, key: &str) -> Option<String> {
        self.main_cache_get_at(key, Instant::now())
    }

    /// Check only L1 main cache at `now`.
    ///
    /// A valid read increments the hotness count and becomes most recently used.
    /// It deliberately does not change `expires_at`: L1 uses fixed TTL.
    fn main_cache_get_at(&mut self, key: &str, now: Instant) -> Option<String> {
        match self.main_map.get_mut(key) {
            None => None,
            Some(entry) if entry.expires_at <= now => {
                self.main_map.remove(key);
                if let Some(pos) = self.main_order.iter().position(|k| k == key) {
                    self.main_order.remove(pos);
                }
                None
            }
            Some(entry) => {
                entry.access_count += 1;
                let value = entry.value.clone();
                if let Some(pos) = self.main_order.iter().position(|k| k == key) {
                    self.main_order.remove(pos);
                }
                self.main_order.push_back(key.to_string());
                Some(value)
            }
        }
    }

    /// Insert into L1 using the real clock.
    fn put(&mut self, key: String, value: String) {
        self.put_at(key, value, Instant::now());
    }

    /// Insert into L1 at `now`.
    ///
    /// A new fill starts with zero observed cache hits. Replacing a value also
    /// resets that count because it is a new version of the cached object.
    fn put_at(&mut self, key: String, value: String, now: Instant) {
        if self.main_map.contains_key(&key) {
            if let Some(pos) = self.main_order.iter().position(|k| k == &key) {
                self.main_order.remove(pos);
            }
        }
        self.main_map.insert(
            key.clone(),
            MainEntry {
                value,
                expires_at: now + self.default_ttl,
                access_count: 0,
            },
        );
        self.main_order.push_back(key);

        while self.main_map.len() > self.max_entries {
            if let Some(lru_key) = self.main_order.pop_front() {
                self.main_map.remove(&lru_key);
            }
        }
    }

    fn delete(&mut self, key: &str) -> bool {
        let existed = self.main_map.remove(key).is_some();
        if existed {
            if let Some(pos) = self.main_order.iter().position(|k| k == key) {
                self.main_order.remove(pos);
            }
        }
        existed
    }

    fn live_len(&self) -> usize {
        let now = Instant::now();
        self.main_map
            .values()
            .filter(|e| e.expires_at > now)
            .count()
    }

    fn evict_expired(&mut self) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .main_map
            .iter()
            .filter(|(_, e)| e.expires_at <= now)
            .map(|(k, _)| k.clone())
            .collect();
        for key in &expired {
            self.main_map.remove(key);
            if let Some(pos) = self.main_order.iter().position(|k| k == key) {
                self.main_order.remove(pos);
            }
        }
    }

    /// Test-only view of the hot-key ranking, independent of network batching.
    #[cfg(test)]
    fn top_k_hot_at(&self, k: usize, now: Instant) -> Vec<(String, String)> {
        let mut entries: Vec<(&String, &MainEntry)> = self
            .main_map
            .iter()
            .filter(|(_, entry)| entry.expires_at > now)
            .collect();
        entries.sort_unstable_by(|a, b| {
            b.1.access_count
                .cmp(&a.1.access_count)
                .then_with(|| a.0.cmp(b.0))
        });
        entries.truncate(k);
        entries
            .into_iter()
            .map(|(key, entry)| (key.clone(), entry.value.clone()))
            .collect()
    }

    /// Select and group this primary's hottest owned entries for L2 replication.
    ///
    /// Foreign keys can exist in L1 after an L2 promotion. They are useful local
    /// cache entries, but this server is not their primary and must not replicate
    /// them onward as though it owned them. Ownership is filtered before taking
    /// the top-K entries so foreign promotions cannot crowd out owned keys.
    fn replication_batches_at(
        &self,
        top_k: usize,
        sender: ServerIndex,
        ring: &DualRing,
        now: Instant,
    ) -> Vec<ReplicationBatch> {
        let mut owned: Vec<(&String, &MainEntry)> = self
            .main_map
            .iter()
            .filter(|(_, entry)| entry.expires_at > now)
            .filter(|(key, _)| ring.placement_for(key).primary == sender)
            .collect();
        owned.sort_unstable_by(|a, b| {
            b.1.access_count
                .cmp(&a.1.access_count)
                .then_with(|| a.0.cmp(b.0))
        });
        owned.truncate(top_k);

        // Every peer receives a complete replacement snapshot from this primary.
        // An empty batch is meaningful: it clears replicas that were hot in an
        // earlier interval but are no longer selected now.
        let mut by_backup: BTreeMap<ServerIndex, Vec<ReplicateEntry>> = ring
            .members()
            .iter()
            .copied()
            .filter(|backup| *backup != sender)
            .map(|backup| (backup, Vec::new()))
            .collect();
        for (key, entry) in owned {
            let placement = ring.placement_for(key);
            debug_assert_ne!(placement.backup, sender);
            by_backup
                .entry(placement.backup)
                .or_default()
                .push(ReplicateEntry {
                    key: key.clone(),
                    value: entry.value.clone(),
                });
        }

        by_backup
            .into_iter()
            .map(|(target, entries)| ReplicationBatch { target, entries })
            .collect()
    }

    /// Diagnostic snapshot: hot store counts, top-5 hot keys, and live cache size.
    fn replicate_stats(&self) -> ReplicateStats {
        let now = Instant::now();
        let hot_store_total: usize = self
            .hot_store
            .by_node
            .values()
            .map(|snapshot| snapshot.entries.len())
            .sum();
        let hot_store_by_node: HashMap<NodeId, usize> = self
            .hot_store
            .by_node
            .iter()
            .map(|(id, snapshot)| (*id, snapshot.entries.len()))
            .collect();

        let mut entries: Vec<(&String, u64)> = self
            .main_map
            .iter()
            .filter(|(_, e)| e.expires_at > now)
            .map(|(k, e)| (k, e.access_count))
            .collect();
        entries.sort_unstable_by(|a, b| b.1.cmp(&a.1));
        let top5: Vec<(String, u64)> = entries
            .iter()
            .take(5)
            .map(|(k, c)| ((*k).clone(), *c))
            .collect();

        let main_cache_live = self
            .main_map
            .values()
            .filter(|e| e.expires_at > now)
            .count();

        ReplicateStats {
            hot_store_total,
            hot_store_by_node,
            top5_main_hot_keys: top5,
            main_cache_live,
        }
    }

    /// Receive a replication snapshot from `from_node`.
    pub fn receive_replicate(
        &mut self,
        from_node: NodeId,
        version: SnapshotVersion,
        entries: Vec<ReplicateEntry>,
    ) -> bool {
        self.hot_store.replace_from(from_node, version, entries)
    }

    /// Promote exactly the requested L2 replica into L1, then serve it.
    ///
    /// A backup is useful because it preserves selected high-value objects; it
    /// should not copy an entire peer snapshot into scarce L1 memory on the
    /// first fallback request. The final L1 read records this request as a hit.
    fn promote_requested_replica(
        &mut self,
        primary: NodeId,
        key: &str,
        now: Instant,
    ) -> Option<String> {
        let value = self.hot_store.get_from(primary, key)?;
        self.put_at(key.to_owned(), value, now);
        self.main_cache_get_at(key, now)
    }

    /// Serve a key through this server's level-2 replica store.
    ///
    /// A caller supplies its own zero-based server index because placement is a
    /// client-visible rule. Only the server selected by the L2 ring may promote
    /// the requested replica. The primary and uninvolved servers return a miss
    /// here, even if they happen to hold a replica from some other source.
    fn l2_lookup_at(
        &mut self,
        key: &str,
        this_server: ServerIndex,
        ring: &DualRing,
        now: Instant,
    ) -> Option<String> {
        let placement = ring.placement_for(key);
        if placement.backup != this_server {
            return None;
        }

        self.promote_requested_replica(placement.primary as NodeId, key, now)
    }
}

// ── DualRingCache public API ───────────────────────────────────────────────────

impl DualRingCache {
    /// Create a new DualRingCache.
    ///
    /// `top_k`: how many hot keys to replicate per interval.
    /// `server_index`: this node's zero-based position in the cluster list.
    /// `ring`: shared level-1 and level-2 member placement.
    ///
    /// Peer URLs are read from env vars at replication time:
    ///   PEER_URLS — comma-separated list of peer base URLs, e.g.
    ///               "http://node2:8002,http://node3:8003"
    pub fn new(
        default_ttl: Duration,
        max_entries: usize,
        top_k: usize,
        server_index: ServerIndex,
        ring: DualRing,
    ) -> Self {
        assert!(
            ring.members().contains(&server_index),
            "server index is not in the dual-ring membership"
        );
        DualRingCache {
            inner: Arc::new(Mutex::new(DualRingInner::new(max_entries, default_ttl))),
            top_k,
            server_index,
            ring: Arc::new(RwLock::new(Arc::new(ring))),
        }
    }

    /// Spawn the background replication loop.
    /// Must be called from an async context (tokio runtime must be running).
    pub fn spawn_replication_task(self: &Arc<Self>, interval: Duration) {
        let inner = Arc::clone(&self.inner);
        let top_k = self.top_k;
        let server_index = self.server_index;
        let ring = Arc::clone(&self.ring);
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let epoch_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let mut sequence = 0_u64;
            loop {
                tokio::time::sleep(interval).await;
                sequence = sequence.saturating_add(1);
                let version = SnapshotVersion { epoch_ms, sequence };

                // Build batches while briefly holding the cache lock, then
                // release it before HTTP calls.
                let batches = {
                    let g = inner.lock().unwrap();
                    let ring = ring.read().unwrap();
                    g.replication_batches_at(top_k, server_index, &ring, Instant::now())
                };
                if batches.is_empty() {
                    continue;
                }

                let peer_urls: Vec<String> = std::env::var("PEER_URLS")
                    .unwrap_or_default()
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.trim().to_string())
                    .collect();

                if peer_urls.is_empty() {
                    continue;
                }

                // POST each batch to the appropriate peer
                for batch in batches {
                    // `PEER_URLS` lists every other configured server in
                    // ascending server-index order. It remains a full topology
                    // map while the active ring shrinks, so a member can still
                    // address (for example) server 2 after server 0 is removed.
                    let url_idx = if batch.target < server_index {
                        batch.target
                    } else {
                        batch.target - 1
                    };
                    if let Some(base_url) = peer_urls.get(url_idx) {
                        let payload = ReplicatePayload {
                            from_node: server_index,
                            version,
                            entries: batch.entries,
                        };
                        let url = format!("{}/replicate", base_url);
                        // Best-effort: ignore errors (peer may be down)
                        let _ = client.post(&url).json(&payload).send().await;
                    }
                }
            }
        });
    }

    /// Handle a `/replicate` POST from a peer node.
    /// Replaces the hot store for `from_node` only when this snapshot is newer.
    pub fn receive_replicate(&self, payload: ReplicatePayload) {
        self.inner.lock().unwrap().receive_replicate(
            payload.from_node,
            payload.version,
            payload.entries,
        );
    }

    /// Return a diagnostic snapshot of the hot store and hot key rankings.
    pub fn replicate_stats(&self) -> ReplicateStats {
        self.inner.lock().unwrap().replicate_stats()
    }

    /// Return replicas received from one primary, ordered by key for a
    /// deterministic controller-driven reconfiguration handoff.
    pub fn replicas_from(&self, primary: ServerIndex) -> Vec<ReplicateEntry> {
        let mut entries: Vec<_> = self
            .inner
            .lock()
            .unwrap()
            .hot_store
            .by_node
            .get(&primary)
            .map(|snapshot| {
                snapshot
                    .entries
                    .iter()
                    .map(|(key, value)| ReplicateEntry {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        entries
    }

    /// Check only this node's level-1 main cache.
    pub fn get_l1(&self, key: &str) -> Option<String> {
        self.inner.lock().unwrap().main_cache_get(key)
    }

    /// Check this node's level-2 replica store and promote a replica only when
    /// this node is the designated backup for `key`.
    pub fn get_from_backup(&self, key: &str) -> Option<String> {
        let ring = self.ring.read().unwrap();
        self.inner
            .lock()
            .unwrap()
            .l2_lookup_at(key, self.server_index, &ring, Instant::now())
    }

    /// Replace this node's membership view after a coordinated reconfiguration.
    ///
    /// The caller must update every participating client and healthy cache node
    /// to the same member list before it sends reconfigured traffic.
    pub fn replace_ring(&self, ring: DualRing) -> Result<(), &'static str> {
        if !ring.members().contains(&self.server_index) {
            return Err("this cache server is not in the proposed membership");
        }
        *self.ring.write().unwrap() = Arc::new(ring);
        Ok(())
    }
}

impl Clone for DualRingCache {
    fn clone(&self) -> Self {
        DualRingCache {
            inner: Arc::clone(&self.inner),
            top_k: self.top_k,
            server_index: self.server_index,
            ring: Arc::clone(&self.ring),
        }
    }
}

impl CacheBackend for DualRingCache {
    fn get(&self, key: &str) -> Option<String> {
        self.get_l1(key)
    }

    fn put(&self, key: String, value: String) {
        self.inner.lock().unwrap().put(key, value);
    }

    fn delete(&self, key: &str) -> bool {
        self.inner.lock().unwrap().delete(key)
    }

    fn live_len(&self) -> usize {
        self.inner.lock().unwrap().live_len()
    }

    fn evict_expired(&self) {
        self.inner.lock().unwrap().evict_expired();
    }
}

#[cfg(test)]
mod dual_ring_tests;
