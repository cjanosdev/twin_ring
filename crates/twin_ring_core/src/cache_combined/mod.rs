/// Combined cache: LRU+TTL with dual-ring hot-key replication (L2 hot store for fallback promotion),
/// and access-count-based hot-key identification (TTL-tier signal).
///
/// **Research question:** Does composing all three mitigations together produce
/// better metastability resistance than any single strategy alone?
///
/// **Composition design:**
/// - Main store  → `LeasedCache`'s LRU + TTL storage
/// - L2 hot store → `HotStore` (dual-ring replication, separate from main cache)
/// - Access counters → `HashMap<String, u64>` (lightweight, not a full tier store)
///
/// On `get`:
///   1. Check main LRU+TTL store (fast path)
///   2. On miss, check L2 hot store — if found, promote ALL hot items from that
///      source node into the main cache (bulk warm-up, same as dual-ring)
///   3. Increment access counter on any hit — used for top-K hot-key selection
///
/// On replication tick:
///   Pick top-K entries by access_count, POST to backup node's /replicate.
///
/// Lease behavior is intentionally disabled until this strategy receives its
/// own rewrite around the explicit `LeaseLookup` protocol.
///
/// No TTL tier data duplication.
///   Tiering is expressed as access_count → hot_store promotion, not separate
///   TierBucket stores. Hot-store entries have no TTL (replaced wholesale on each
///   replication tick). This avoids 3× memory and 3× lock contention.
///
/// Future extension points (not implemented):
/// - TODO: quorum guard before promotion (false-positive prevention)
/// - TODO: ML-based hot-key selection (access_count is the raw signal)
/// - TODO: bloom filter per replication batch (client-side absence hints)
use crate::cache_backend::CacheBackend;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

// ── Wire types (shared with dual-ring's /replicate endpoint) ──────────────────

pub use crate::cache_dual_ring::ReplicatePayload;

// ── NodeId ────────────────────────────────────────────────────────────────────

type NodeId = usize;

// ── Hot store — same semantics as DualRingCache's HotStore ────────────────────

#[derive(Default)]
struct HotStore {
    by_node: HashMap<NodeId, ReplicaSnapshot>,
}

struct ReplicaSnapshot {
    version: crate::cache_dual_ring::SnapshotVersion,
    entries: HashMap<String, String>,
}

impl HotStore {
    fn get_from(&self, primary: NodeId, key: &str) -> Option<String> {
        self.by_node.get(&primary)?.entries.get(key).cloned()
    }
    fn receive(
        &mut self,
        from_node: NodeId,
        version: crate::cache_dual_ring::SnapshotVersion,
        entries: Vec<crate::cache_dual_ring::ReplicateEntry>,
    ) -> bool {
        if self
            .by_node
            .get(&from_node)
            .is_some_and(|current| version <= current.version)
        {
            return false;
        }
        let map: HashMap<String, String> = entries.into_iter().map(|e| (e.key, e.value)).collect();
        self.by_node.insert(
            from_node,
            ReplicaSnapshot {
                version,
                entries: map,
            },
        );
        true
    }
}

// ── CombinedInner — shared mutable state ─────────────────────────────────────

struct CombinedInner {
    entries: HashMap<String, CombinedEntry>,
    lru_order: VecDeque<String>,
    max_entries: usize,
    default_ttl: Duration,
    leases: HashMap<String, ActiveLease>,
    next_lease_token: u64,
    lease_duration: Duration,
    hot_store: HotStore,
}

/// Ownership token for one in-flight backing-store fill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CombinedLease {
    key: String,
    token: u64,
}

/// Result of a normal Combined L1 lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CombinedLookup {
    Hit(String),
    LeaseGranted(CombinedLease),
    LeaseHeld,
}

struct CombinedEntry {
    value: String,
    filled_at: std::time::Instant,
    expires_at: std::time::Instant,
    access_count: u64,
}

impl CombinedInner {
    fn new(max_entries: usize, default_ttl: Duration) -> Self {
        CombinedInner {
            entries: HashMap::with_capacity(max_entries + 1),
            lru_order: VecDeque::with_capacity(max_entries + 1),
            max_entries,
            default_ttl,
            leases: HashMap::new(),
            next_lease_token: 0,
            lease_duration: Duration::from_millis(500),
            hot_store: HotStore::default(),
        }
    }

    fn remove_from_order(&mut self, key: &str) {
        if let Some(position) = self.lru_order.iter().position(|stored| stored == key) {
            self.lru_order.remove(position);
        }
    }

    fn get_at(&mut self, key: &str, now: std::time::Instant) -> Option<String> {
        let entry = self.entries.get_mut(key)?;
        if entry.expires_at <= now {
            self.entries.remove(key);
            self.remove_from_order(key);
            return None;
        }

        entry.access_count = entry.access_count.saturating_add(1);
        if entry.access_count == 2 {
            entry.expires_at = entry.filled_at + self.default_ttl * 2;
        } else if entry.access_count == 8 {
            entry.expires_at = entry.filled_at + self.default_ttl * 4;
        }
        let value = entry.value.clone();
        self.remove_from_order(key);
        self.lru_order.push_back(key.to_owned());
        Some(value)
    }

    fn get(&mut self, key: &str) -> Option<String> {
        self.get_at(key, std::time::Instant::now())
    }

    fn lookup_or_acquire_at(&mut self, key: &str, now: std::time::Instant) -> CombinedLookup {
        if let Some(value) = self.get_at(key, now) {
            return CombinedLookup::Hit(value);
        }
        if self
            .leases
            .get(key)
            .is_some_and(|lease| lease.expires_at > now)
        {
            return CombinedLookup::LeaseHeld;
        }
        self.next_lease_token = self.next_lease_token.saturating_add(1);
        let lease = CombinedLease {
            key: key.to_owned(),
            token: self.next_lease_token,
        };
        self.leases.insert(
            key.to_owned(),
            ActiveLease {
                token: lease.token,
                expires_at: now + self.lease_duration,
            },
        );
        CombinedLookup::LeaseGranted(lease)
    }

    fn complete_fill_at(
        &mut self,
        lease: &CombinedLease,
        value: String,
        now: std::time::Instant,
    ) -> bool {
        if self
            .leases
            .get(&lease.key)
            .is_none_or(|active| active.token != lease.token || active.expires_at <= now)
        {
            return false;
        }
        self.leases.remove(&lease.key);
        self.put_at(lease.key.clone(), value, now);
        true
    }

    fn abandon_fill(&mut self, lease: &CombinedLease) {
        if self
            .leases
            .get(&lease.key)
            .is_some_and(|active| active.token == lease.token)
        {
            self.leases.remove(&lease.key);
        }
    }

    fn l2_lookup_at(
        &mut self,
        key: &str,
        this_server: NodeId,
        ring: &crate::cache_dual_ring::DualRing,
        now: std::time::Instant,
    ) -> Option<String> {
        let placement = ring.placement_for(key);
        if placement.backup != this_server {
            return None;
        }
        let value = self.hot_store.get_from(placement.primary, key)?;
        self.put_at(key.to_owned(), value, now);
        self.get_at(key, now)
    }

    fn put_at(&mut self, key: String, value: String, now: std::time::Instant) {
        self.entries.remove(&key);
        self.remove_from_order(&key);
        self.entries.insert(
            key.clone(),
            CombinedEntry {
                value,
                filled_at: now,
                expires_at: now + self.default_ttl,
                access_count: 0,
            },
        );
        self.lru_order.push_back(key);
        while self.entries.len() > self.max_entries {
            if let Some(lru) = self.lru_order.pop_front() {
                self.entries.remove(&lru);
            }
        }
    }

    fn put(&mut self, key: String, value: String) {
        self.put_at(key, value, std::time::Instant::now());
    }

    fn top_k_hot(
        &self,
        k: usize,
        owner: NodeId,
        ring: &crate::cache_dual_ring::DualRing,
    ) -> Vec<crate::cache_dual_ring::ReplicateEntry> {
        let now = std::time::Instant::now();
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .filter(|(key, entry)| {
                entry.expires_at > now && ring.placement_for(key).primary == owner
            })
            .collect();
        entries.sort_unstable_by(|left, right| {
            right
                .1
                .access_count
                .cmp(&left.1.access_count)
                .then_with(|| left.0.cmp(right.0))
        });
        entries.truncate(k);
        entries
            .into_iter()
            .map(|(key, entry)| crate::cache_dual_ring::ReplicateEntry {
                key: key.clone(),
                value: entry.value.clone(),
            })
            .collect()
    }
}

struct ActiveLease {
    token: u64,
    expires_at: std::time::Instant,
}

// ── CombinedCache ─────────────────────────────────────────────────────────────

pub struct CombinedCache {
    inner: Arc<Mutex<CombinedInner>>,
    top_k: usize,
    default_ttl: Duration,
    server_index: NodeId,
    ring: Arc<RwLock<Arc<crate::cache_dual_ring::DualRing>>>,
}

impl CombinedCache {
    /// Create a new CombinedCache.
    ///
    /// `top_k`: how many hot keys to replicate per interval.
    /// Node identity and peer URLs are read from env vars at replication time:
    ///   `NODE_INDEX` — this node's zero-based index (0, 1, or 2)
    ///   `PEER_URLS` — comma-separated list of peer base URLs
    pub fn new(
        default_ttl: Duration,
        max_entries: usize,
        top_k: usize,
        server_index: NodeId,
        ring: crate::cache_dual_ring::DualRing,
    ) -> Self {
        CombinedCache {
            inner: Arc::new(Mutex::new(CombinedInner::new(max_entries, default_ttl))),
            top_k,
            default_ttl,
            server_index,
            ring: Arc::new(RwLock::new(Arc::new(ring))),
        }
    }

    /// Handle a `/replicate` POST from a peer node.
    pub fn receive_replicate(&self, payload: ReplicatePayload) {
        self.inner.lock().unwrap().hot_store.receive(
            payload.from_node,
            payload.version,
            payload.entries,
        );
    }

    /// Acquire the right to fill a missing L1 key, or observe its current state.
    pub fn lookup_or_acquire(&self, key: &str) -> CombinedLookup {
        self.inner
            .lock()
            .unwrap()
            .lookup_or_acquire_at(key, std::time::Instant::now())
    }

    pub fn complete_fill(&self, lease: &CombinedLease, value: String) -> bool {
        self.inner
            .lock()
            .unwrap()
            .complete_fill_at(lease, value, std::time::Instant::now())
    }

    pub fn abandon_fill(&self, lease: &CombinedLease) {
        self.inner.lock().unwrap().abandon_fill(lease);
    }

    pub fn get_from_backup(&self, key: &str) -> Option<String> {
        let ring = self.ring.read().unwrap();
        self.inner.lock().unwrap().l2_lookup_at(
            key,
            self.server_index,
            &ring,
            std::time::Instant::now(),
        )
    }

    pub fn replace_ring(&self, ring: crate::cache_dual_ring::DualRing) -> Result<(), &'static str> {
        if !ring.members().contains(&self.server_index) {
            return Err("this cache server is not in the proposed membership");
        }
        *self.ring.write().unwrap() = Arc::new(ring);
        Ok(())
    }

    pub fn replicas_from(&self, primary: NodeId) -> Vec<crate::cache_dual_ring::ReplicateEntry> {
        self.inner
            .lock()
            .unwrap()
            .hot_store
            .by_node
            .get(&primary)
            .map(|snapshot| {
                snapshot
                    .entries
                    .iter()
                    .map(|(key, value)| crate::cache_dual_ring::ReplicateEntry {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Spawn the background replication loop.
    /// Must be called from a tokio async context.
    pub fn spawn_replication_task(self: &Arc<Self>, interval: Duration) {
        let inner = Arc::clone(&self.inner);
        let top_k = self.top_k;
        let my_node_id = self.server_index;
        let ring = Arc::clone(&self.ring);

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let epoch_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let mut sequence = 0_u64;
            loop {
                tokio::time::sleep(interval).await;
                sequence = sequence.saturating_add(1);
                let version = crate::cache_dual_ring::SnapshotVersion { epoch_ms, sequence };

                // Collect top-K hot key names (brief lock)
                let entries = {
                    let ring = ring.read().unwrap();
                    inner.lock().unwrap().top_k_hot(top_k, my_node_id, &ring)
                };
                if entries.is_empty() {
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
                let ring = Arc::clone(&ring.read().unwrap());

                // Group owned hot keys by their designated L2 backup.
                let mut batches: HashMap<usize, Vec<crate::cache_dual_ring::ReplicateEntry>> =
                    HashMap::new();
                for entry in &entries {
                    let l2_node = ring.placement_for(&entry.key).backup;
                    batches.entry(l2_node).or_default().push(
                        crate::cache_dual_ring::ReplicateEntry {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                        },
                    );
                }

                for (peer_idx, batch_entries) in batches {
                    let url_idx = if peer_idx >= my_node_id {
                        peer_idx - 1
                    } else {
                        peer_idx
                    };
                    if let Some(base_url) = peer_urls.get(url_idx % peer_urls.len()) {
                        let payload = crate::cache_dual_ring::ReplicatePayload {
                            from_node: my_node_id,
                            version,
                            entries: batch_entries,
                        };
                        let url = format!("{}/replicate", base_url);
                        let _ = client.post(&url).json(&payload).send().await;
                    }
                }
            }
        });
    }

    /// Look up a key, falling back to the L2 hot store on a miss.
    /// On hot-store hit: promotes ALL hot items from the source node into the main cache.
    fn get_internal(&self, key: &str) -> Option<String> {
        // Fast path: main LRU+TTL cache
        if let Some(val) = self.inner.lock().unwrap().get(key) {
            return Some(val);
        }

        None
    }
}

impl Clone for CombinedCache {
    fn clone(&self) -> Self {
        CombinedCache {
            inner: Arc::clone(&self.inner),
            top_k: self.top_k,
            default_ttl: self.default_ttl,
            server_index: self.server_index,
            ring: Arc::clone(&self.ring),
        }
    }
}

impl CacheBackend for CombinedCache {
    fn get(&self, key: &str) -> Option<String> {
        self.get_internal(key)
    }

    fn put(&self, key: String, value: String) {
        self.inner.lock().unwrap().put(key, value);
    }

    fn delete(&self, key: &str) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let existed = inner.entries.remove(key).is_some();
        inner.remove_from_order(key);
        existed
    }

    fn live_len(&self) -> usize {
        let now = std::time::Instant::now();
        self.inner
            .lock()
            .unwrap()
            .entries
            .values()
            .filter(|entry| entry.expires_at > now)
            .count()
    }

    fn evict_expired(&self) {
        let now = std::time::Instant::now();
        let mut inner = self.inner.lock().unwrap();
        let expired: Vec<String> = inner
            .entries
            .iter()
            .filter(|(_, entry)| entry.expires_at <= now)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            inner.entries.remove(&key);
            inner.remove_from_order(&key);
        }
    }
}

#[cfg(test)]
mod combined_tests;
