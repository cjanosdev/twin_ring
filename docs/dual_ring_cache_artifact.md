# Dual-ring cache artifact

Sources: [crates/twin_ring_core/src/cache_dual_ring/placement.rs](../crates/twin_ring_core/src/cache_dual_ring/placement.rs), [crates/twin_ring_core/src/cache_dual_ring/mod.rs](../crates/twin_ring_core/src/cache_dual_ring/mod.rs).

Tests: [dual_ring_tests.rs](../crates/twin_ring_core/src/cache_dual_ring/dual_ring_tests.rs).

Read in order: [Data structures](#data-structures), [Implementations](#implementations), [Functions](#functions), [Strategy goal](#strategy-goal), and [Testing](#testing). Each item shows its complete source code before its explanation. Full `impl` blocks are intentionally repeated as individual functions in the next section so both the type-level grouping and each operation can be studied on their own. Code is copied from the current source, including attributes and attached comments; these are excerpts, not standalone compilable files.

## Data structures

### `ServerIndex`

```rust
/// Index of one cache server in the client-visible cluster list.
pub type ServerIndex = usize;
```

**Types and what:** `ServerIndex` is an alias for `usize`, the zero-based identity used in the client-visible server list. It is not a newtype with automatic validation.

**Why:** Using one name connects placement, membership, and replication targets without conversion between different integer types.

### `VIRTUAL_NODES_PER_SERVER`

```rust
const VIRTUAL_NODES_PER_SERVER: u32 = 128;
```

**Types and what:** Each physical member contributes 128 virtual points to each ring; the loop index uses `u32`.

**Why:** Multiple positions aim to spread keys across a small cluster. This parameter is a design choice, not a demonstrated guarantee of balanced workload.

### `KeyPlacement`

```rust
/// The two locations a client needs for one key.
///
/// `primary` is the normal level-1 cache location. `backup` is the level-2
/// replica location that a client tries only when it cannot reach `primary`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyPlacement {
    pub primary: ServerIndex,
    pub backup: ServerIndex,
}
```

**Types and what:** Two `ServerIndex` values identify the normal primary and the distinct fallback backup. Copy, clone, equality, and debug derives make this small result easy to pass and test.

**Why:** Clients and nodes need the same pair so replication destinations and backup reads agree.

### `RingPoint`

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RingPoint {
    hash: u64,
    owner: ServerIndex,
}
```

**Types and what:** A `u64` hash locates a virtual point; `ServerIndex` identifies its owning server. Copy and comparison-related derives support working with these small records.

**Why:** Several points per physical server spread ownership across the ring, instead of assigning one contiguous region per server.

### `HashRing`

```rust
/// One immutable consistent-hash ring.
///
/// A server appears at many virtual points, which gives a small cluster a more
/// even key distribution than placing each server at just one point.
#[derive(Clone, Debug)]
struct HashRing {
    points: Vec<RingPoint>,
}
```

**Types and what:** A sorted `Vec<RingPoint>` represents one immutable ring; cloning copies the vector. Sorting is by hash and then owner.

**Why:** A sorted vector permits binary-search-style successor lookup and deterministic clockwise traversal. This keeps placement independent of process-local map iteration.

### `DualRing`

```rust
/// Two independent consistent-hash rings over one member set.
///
/// The L1 ring locates the normal owner. The L2 ring independently chooses a
/// replica destination, then skips the L1 owner if it happens to select it.
/// Keeping `DualRing` immutable means a request sees one coherent membership
/// snapshot. To reconfigure the experiment, construct and distribute a new
/// ring from the new member list.
#[derive(Clone, Debug)]
pub struct DualRing {
    members: Vec<ServerIndex>,
    l1: HashRing,
    l2: HashRing,
}
```

**Types and what:** A sorted, deduplicated vector of server IDs and two `HashRing` values describe the same membership with different seeds. `Clone` copies this immutable placement value.

**Why:** Independent L1/L2 placement plus skipping the primary produces a distinct backup. Coordinated replacement gives clients and cache nodes a common new membership snapshot.

### `NodeId`

```rust
/// Zero-based identity of the source primary for one replica snapshot.
type NodeId = ServerIndex;
```

**Types and what:** This local alias names the source-primary identity used as the key of the replica map. Dual-ring aliases `ServerIndex`; combined aliases `usize`, which is also the underlying server-index type.

**Why:** The name distinguishes node identities from entry counts while preserving compatibility with shared replication payloads.

### `MainEntry`

```rust
/// A main-cache entry: value, expiry, and a hit counter for hot-key selection.
struct MainEntry {
    value: String,
    expires_at: Instant,
    access_count: u64,
}
```

**Types and what:** `String` owns an L1 value, `Instant` is its fixed expiry, and `u64` counts successful reads for hot-key ranking.

**Why:** Dual-ring uses popularity to select replicas, while L1 lifetime stays fixed. It therefore does not need the original fill timestamp used by TTL promotion.

### `HotStore`

```rust
/// The per-node hot store: keyed by source NodeId → their replicated hot items.
/// Replaced wholesale on each replication update — no TTL, no LRU.
#[derive(Default)]
struct HotStore {
    by_node: HashMap<NodeId, ReplicaSnapshot>,
}
```

**Types and what:** `by_node` maps each source primary ID to its own `ReplicaSnapshot`; derived `Default` creates an empty map. These L2 replicas have no local TTL or LRU ordering.

**Why:** Separate source namespaces allow one primary to replace its snapshot without deleting another primary’s replicas. L2 memory is outside the L1 capacity budget.

### `ReplicaSnapshot`

```rust
struct ReplicaSnapshot {
    version: SnapshotVersion,
    entries: HashMap<String, String>,
}
```

**Types and what:** The version identifies the accepted snapshot; `HashMap<String, String>` owns its key/value pairs. Combined uses the same `SnapshotVersion` type exported by dual-ring.

**Why:** Keeping the version even for an empty snapshot lets the receiver reject delayed older data after stale keys have been cleared.

### `DualRingInner`

```rust
struct DualRingInner {
    // L1: main LRU+TTL cache
    main_map: HashMap<String, MainEntry>,
    main_order: VecDeque<String>, // front=LRU, back=MRU
    max_entries: usize,
    default_ttl: Duration,

    // L2: hot store (replication targets from peer nodes)
    hot_store: HotStore,
}
```

**Types and what:** `main_map` and `main_order` implement L1 storage and LRU eviction under `max_entries`; `default_ttl` is the fixed lifetime. `hot_store` is the separate L2 replica map.

**Why:** The stores serve different purposes: L1 handles ordinary cache traffic, while L2 preserves selected peer values for explicit fallback. L1 maintenance does not erase L2.

### `DualRingCache`

```rust
pub struct DualRingCache {
    inner: Arc<Mutex<DualRingInner>>,
    top_k: usize,
    server_index: ServerIndex,
    ring: Arc<RwLock<Arc<DualRing>>>,
}
```

**Types and what:** The shared `inner` mutex protects storage; `top_k` controls replication selection; `server_index` is this node’s identity. The nested `Arc` and `RwLock` hold a shared, replaceable immutable `DualRing`.

**Why:** The request API, clones, and replication task need one cache and membership view. Ring replacement changes placement without rebuilding stored entries.

### `ReplicateEntry`

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicateEntry {
    pub key: String,
    pub value: String,
}
```

**Types and what:** Two owned strings form one transferable key/value pair. `Serialize` and `Deserialize` support the HTTP JSON payload; clone, debug, and equality derives support copying and assertions.

**Why:** The wire format carries the replicated value without exporting local expiry or hit-count state. Promotion creates fresh L1 bookkeeping on the receiving node.

### `ReplicatePayload`

```rust
#[derive(Serialize, Deserialize)]
pub struct ReplicatePayload {
    pub from_node: NodeId,
    pub version: SnapshotVersion,
    pub entries: Vec<ReplicateEntry>,
}
```

**Types and what:** A source `NodeId`, ordered `SnapshotVersion`, and `Vec<ReplicateEntry>` describe the complete replacement snapshot from that source to one destination. Serde derives encode and decode it.

**Why:** The receiver needs both source identity and ordering metadata to replace the correct snapshot and reject obsolete arrivals. An empty vector is a meaningful clear operation.

### `SnapshotVersion`

```rust
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
```

**Types and what:** Two `u64` fields represent a process-start epoch in milliseconds and a sequence number within that task. Derived ordering compares `epoch_ms` first, then `sequence`; serialization sends both to peers.

**Why:** Sequence ordering rejects delayed snapshots. A greater restart epoch can replace the previous process’s data even with a smaller sequence. The wall clock does not guarantee a greater epoch under clock rollback or same-millisecond restarts.

### `ReplicationBatch`

```rust
/// One outbound replica snapshot batch for a designated level-2 backup.
#[derive(Debug, PartialEq, Eq)]
struct ReplicationBatch {
    target: ServerIndex,
    entries: Vec<ReplicateEntry>,
}
```

**Types and what:** A target `ServerIndex` and vector of `ReplicateEntry` values form one outbound destination batch. Debug and equality derives make batch contents inspectable in tests.

**Why:** Grouping by designated backup makes network sending independent of selection and allows empty batches to clear each peer’s previous snapshot.

### `ReplicateStats`

```rust
/// Diagnostic snapshot returned by `GET /replicate-stats`.
#[derive(Serialize)]
pub struct ReplicateStats {
    pub hot_store_total: usize,
    pub hot_store_by_node: HashMap<NodeId, usize>,
    pub top5_main_hot_keys: Vec<(String, u64)>,
    pub main_cache_live: usize,
}
```

**Types and what:** Serializable counts describe total and per-source L2 entries, the five hottest live L1 keys as `(String, u64)` pairs, and the live L1 count. Counts use `usize`.

**Why:** Diagnostics expose what is stored and selected without handing out references into locked maps. These are a point-in-time view, not proof of recovery performance.

## Implementations

An inherent `impl Type` defines operations on that type. `impl Trait for Type` supplies a shared interface. `Self` means the implementing type, `&self` borrows it, and `&mut self` permits mutation. Public wrappers use locks for mutation through a shared reference. Derives shown on data structures generate their trait implementations; they have no handwritten implementation block to reproduce.

### `impl HashRing`

```rust
impl HashRing {
    fn new(ring_seed: u8, members: &[ServerIndex]) -> Self {
        let mut points = Vec::with_capacity(members.len() * VIRTUAL_NODES_PER_SERVER as usize);
        for &member in members {
            for virtual_node in 0..VIRTUAL_NODES_PER_SERVER {
                points.push(RingPoint {
                    hash: point_hash(ring_seed, member, virtual_node),
                    owner: member,
                });
            }
        }
        points.sort_unstable_by_key(|point| (point.hash, point.owner));
        Self { points }
    }

    fn owner_for_hash(&self, hash: u64) -> ServerIndex {
        let index = self.points.partition_point(|point| point.hash < hash);
        self.points[index % self.points.len()].owner
    }

    /// Find the first distinct server encountered clockwise from `hash`.
    fn first_owner_other_than(&self, hash: u64, excluded: ServerIndex) -> ServerIndex {
        let start = self.points.partition_point(|point| point.hash < hash) % self.points.len();
        for offset in 0..self.points.len() {
            let owner = self.points[(start + offset) % self.points.len()].owner;
            if owner != excluded {
                return owner;
            }
        }
        unreachable!("a dual ring always has at least two distinct members")
    }
}
```

**Types and what:** These methods construct sorted virtual points and find a clockwise owner, optionally skipping an excluded server. Slices borrow membership and numeric hashes use `u64`.

**Why:** A reusable immutable ring avoids rebuilding placement for every key. `DualRing::new` supplies the at-least-two-distinct-members precondition.

### `impl DualRing`

```rust
impl DualRing {
    /// Build both rings from distinct, zero-based server IDs.
    ///
    /// At least two servers are required because a replica cannot share the
    /// primary's machine. Sorting makes `[2, 0, 1]` describe the same cluster
    /// as `[0, 1, 2]`.
    pub fn new<I>(members: I) -> Option<Self>
    where
        I: IntoIterator<Item = ServerIndex>,
    {
        let mut members: Vec<_> = members.into_iter().collect();
        members.sort_unstable();
        members.dedup();
        if members.len() < 2 {
            return None;
        }

        Some(Self {
            l1: HashRing::new(0, &members),
            l2: HashRing::new(1, &members),
            members,
        })
    }

    /// Return the member IDs used to construct this ring, in sorted order.
    pub fn members(&self) -> &[ServerIndex] {
        &self.members
    }

    /// Calculate the L1 primary and distinct L2 backup for one key.
    pub fn placement_for(&self, key: &str) -> KeyPlacement {
        let primary = self.l1.owner_for_hash(key_hash(0, key));
        let backup = self.l2.first_owner_other_than(key_hash(1, key), primary);
        KeyPlacement { primary, backup }
    }
}
```

**Types and what:** The generic constructor accepts `IntoIterator<Item = ServerIndex>`, validates and normalizes membership, and builds both rings. Other methods expose a borrowed member slice and owned `KeyPlacement`.

**Why:** One deterministic placement API is shared by clients, senders, and backups, so all three agree about ownership.

### `impl HotStore`

```rust
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
```

**Types and what:** The replica methods read one source’s entries and accept only a strictly newer complete snapshot. `Option<String>` owns a result; `bool` indicates whether replacement was accepted.

**Why:** Reading by source and replacing atomically preserves primary isolation and prevents delayed delivery from rolling a snapshot backward.

### `impl DualRingInner`

```rust
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
```

**Types and what:** The internal methods implement fixed-TTL L1 storage, hot-key ranking, snapshot batching, receipt, and requested-key L2 promotion. Time-sensitive `_at` methods accept `Instant` explicitly.

**Why:** Keeping network I/O outside this state machine makes placement and replication contents directly testable without a server or database.

### `impl DualRingCache`

```rust
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
```

**Types and what:** The public wrapper validates initial membership, locks state for reads and maintenance, exposes replica diagnostics and ring replacement, and starts a Tokio replication task.

**Why:** Callers get thread-safe operations while the background task copies outbound data before awaiting HTTP. Membership coordination across processes remains the caller’s responsibility.

### `impl Clone for DualRingCache`

```rust
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
```

**Types and what:** This trait implementation supplies `clone(&self) -> Self` by sharing the cache and ring `Arc` handles while copying scalar configuration.

**Why:** Each cloned handle must observe the same entries and membership. Cloning does not replicate data into a new cache or spawn a new task.

### `impl CacheBackend for DualRingCache`

```rust
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
```

**Types and what:** This implements the shared `CacheBackend: Send + Sync` interface: owned optional reads, owned inserts, boolean deletion, a live count, and expiry cleanup. Interior mutability through a mutex permits mutations behind `&self`.

**Why:** Callers can use a common cache interface while each strategy preserves its own storage policy. For combined and dual-ring, this trait only exposes L1 operations; explicit strategy APIs handle leases or backup lookup.

## Functions

`String` is owned text and `&str` is a borrowed view. `Option<T>` distinguishes a result from absence; `Result<T, E>` distinguishes success from an error. `Duration` is a time span, while `Instant` is a monotonic time point. `usize` is used for counts and indices; `u32` and `u64` have fixed unsigned widths. `Vec` is a growable sequence, `HashMap` associates keys with values, and `VecDeque` supplies the LRU queue. `Arc` shares ownership, `Mutex` grants exclusive access, and `RwLock` separates shared reads from exclusive replacement. Lock calls using `unwrap()` panic on poisoning rather than recovering.

### `HashRing::new`

```rust
fn new(ring_seed: u8, members: &[ServerIndex]) -> Self {
    let mut points = Vec::with_capacity(members.len() * VIRTUAL_NODES_PER_SERVER as usize);
    for &member in members {
        for virtual_node in 0..VIRTUAL_NODES_PER_SERVER {
            points.push(RingPoint {
                hash: point_hash(ring_seed, member, virtual_node),
                owner: member,
            });
        }
    }
    points.sort_unstable_by_key(|point| (point.hash, point.owner));
    Self { points }
}
```

**Types and what:** For each member and each of 128 virtual-node indices, creates a seeded `u64` point hash and owner record, sorts the vector by `(hash, owner)`, and returns `Self`.

**Why:** All processes using the same membership and seed build identical sorted points; the owner tie-break keeps construction deterministic even for equal hashes.

### `HashRing::owner_for_hash`

```rust
fn owner_for_hash(&self, hash: u64) -> ServerIndex {
    let index = self.points.partition_point(|point| point.hash < hash);
    self.points[index % self.points.len()].owner
}
```

**Types and what:** Uses `partition_point` to find the first point whose hash is at least the supplied `u64`, then indexes modulo vector length to wrap past the end.

**Why:** Clockwise successor ownership is the consistent-hash rule. The ring must be nonempty, as ensured by its use inside a valid `DualRing`.

### `HashRing::first_owner_other_than`

```rust
/// Find the first distinct server encountered clockwise from `hash`.
fn first_owner_other_than(&self, hash: u64, excluded: ServerIndex) -> ServerIndex {
    let start = self.points.partition_point(|point| point.hash < hash) % self.points.len();
    for offset in 0..self.points.len() {
        let owner = self.points[(start + offset) % self.points.len()].owner;
        if owner != excluded {
            return owner;
        }
    }
    unreachable!("a dual ring always has at least two distinct members")
}
```

**Types and what:** Finds the clockwise start for a `u64` hash, scans with wraparound, and returns the first `ServerIndex` different from `excluded`. `unreachable!` covers violation of the distinct-members precondition.

**Why:** Independent L2 hashing can still select the primary, so explicit skipping guarantees a different physical backup.

### `DualRing::new`

```rust
/// Build both rings from distinct, zero-based server IDs.
///
/// At least two servers are required because a replica cannot share the
/// primary's machine. Sorting makes `[2, 0, 1]` describe the same cluster
/// as `[0, 1, 2]`.
pub fn new<I>(members: I) -> Option<Self>
where
    I: IntoIterator<Item = ServerIndex>,
{
    let mut members: Vec<_> = members.into_iter().collect();
    members.sort_unstable();
    members.dedup();
    if members.len() < 2 {
        return None;
    }

    Some(Self {
        l1: HashRing::new(0, &members),
        l2: HashRing::new(1, &members),
        members,
    })
}
```

**Types and what:** The generic `I: IntoIterator<Item = ServerIndex>` accepts arrays, ranges, or other member iterables. It sorts and deduplicates IDs, returns `None` for fewer than two, and constructs seed-0 L1 and seed-1 L2 rings in `Some(Self)`.

**Why:** Equivalent member sets must produce the same placement regardless of input order, and a valid backup needs two distinct servers.

### `DualRing::members`

```rust
/// Return the member IDs used to construct this ring, in sorted order.
pub fn members(&self) -> &[ServerIndex] {
    &self.members
}
```

**Types and what:** Returns `&[ServerIndex]`, a borrowed slice of normalized membership, without allocation or mutation.

**Why:** Callers can validate local identity and enumerate replication peers while the ring remains immutable.

### `DualRing::placement_for`

```rust
/// Calculate the L1 primary and distinct L2 backup for one key.
pub fn placement_for(&self, key: &str) -> KeyPlacement {
    let primary = self.l1.owner_for_hash(key_hash(0, key));
    let backup = self.l2.first_owner_other_than(key_hash(1, key), primary);
    KeyPlacement { primary, backup }
}
```

**Types and what:** Hashes the borrowed key separately for each seed, finds its L1 primary, skips that server in the L2 successor search, and returns an owned `KeyPlacement`.

**Why:** The same deterministic function aligns ordinary request routing, replica destinations, and explicit backup validation.

### `key_hash`

```rust
/// Stable FNV-1a hash of a key for one ring.
fn key_hash(ring_seed: u8, key: &str) -> u64 {
    stable_hash(&[b"key", &[ring_seed], key.as_bytes()])
}
```

**Types and what:** Passes the domain bytes `key`, a one-byte ring seed, and UTF-8 key bytes as borrowed byte slices to `stable_hash`, returning `u64`.

**Why:** Domain and seed inputs distinguish key hashing from virtual-point hashing and distinguish the two rings.

### `point_hash`

```rust
/// Stable FNV-1a hash of a virtual point for one ring.
fn point_hash(ring_seed: u8, member: ServerIndex, virtual_node: u32) -> u64 {
    stable_hash(&[
        b"point",
        &[ring_seed],
        &(member as u64).to_le_bytes(),
        &virtual_node.to_le_bytes(),
    ])
}
```

**Types and what:** Hashes the `point` domain, one-byte ring seed, member converted to little-endian `u64` bytes, and virtual-node `u32` in little-endian form.

**Why:** Explicit numeric encoding avoids machine-dependent byte order and gives each member/virtual-node pair reproducible point positions.

### `stable_hash`

```rust
/// A tiny stable hash for experimental placement.
///
/// `DefaultHasher` is fine for maps inside one process, but its algorithm is
/// not a protocol that cache nodes and clients can safely share. Separators make
/// the byte sequences unambiguous (for example, `("ab", "c")` versus
/// `("a", "bc")`).
fn stable_hash(parts: &[&[u8]]) -> u64 {
    const OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
    const PRIME: u64 = 1_099_511_628_211;

    let mut hash = OFFSET_BASIS;
    for part in parts {
        for byte in *part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
```

**Types and what:** Takes a slice of byte slices, initializes the FNV offset basis, XORs each byte, and uses wrapping multiplication by the FNV prime. It also mixes `0xff` after each part and returns a `u64`.

**Why:** A specified deterministic algorithm supports cross-process placement, unlike relying on the standard map hasher’s unspecified protocol behavior. Separators distinguish common concatenation ambiguities; this finite noncryptographic hash can still collide.

### `HotStore::replace_from`

```rust
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
```

**Types and what:** Rejects versions less than or equal to the stored version. It consumes the entry vector into a new `HashMap<String, String>` and replaces only that primary’s snapshot, returning whether it accepted it. Duplicate keys in a payload collapse to the last inserted value.

**Why:** Complete replacement drops keys no longer selected; an empty snapshot clears entries while retaining the version fence. Other primaries remain untouched.

### `HotStore::get_from`

```rust
/// Return one replica owned by `primary`.
///
/// The clone gives the caller an owned value, so it can promote that value
/// into L1 after this immutable borrow of the replica store has ended.
fn get_from(&self, primary: NodeId, key: &str) -> Option<String> {
    self.by_node.get(&primary)?.entries.get(key).cloned()
}
```

**Types and what:** Uses `primary: NodeId` to find its snapshot, then a borrowed `&str` to find the value. `?` returns `None` for an absent source; `cloned` returns an owned `String` on a hit.

**Why:** A fallback must use the intended primary’s replica. Ownership lets the caller mutate L1 after the immutable L2 borrow ends.

### `DualRingInner::new`

```rust
fn new(max_entries: usize, default_ttl: Duration) -> Self {
    DualRingInner {
        main_map: HashMap::with_capacity(max_entries + 1),
        main_order: VecDeque::with_capacity(max_entries + 1),
        max_entries,
        default_ttl,
        hot_store: HotStore::default(),
    }
}
```

**Types and what:** Creates empty L1 map/queue allocations sized for `max_entries + 1`, stores the fixed TTL and limit, and constructs an empty L2 hot store.

**Why:** L1 insertion briefly exceeds capacity before eviction; L2 snapshots remain independent of that L1 entry budget.

### `DualRingInner::main_cache_get`

```rust
/// Check only L1 main cache using the real clock.
///
/// Production reads come through here. Tests use `main_cache_get_at` below
/// so expiration can be checked at exact, deterministic moments.
fn main_cache_get(&mut self, key: &str) -> Option<String> {
    self.main_cache_get_at(key, Instant::now())
}
```

**Types and what:** Forwards a borrowed key to `main_cache_get_at` with `Instant::now()` and returns `Option<String>`.

**Why:** One implementation serves both real-time public reads and explicit-time tests.

### `DualRingInner::main_cache_get_at`

```rust
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
```

**Types and what:** Matches absence, expiry at/before `now`, or a live entry. Expired records leave map and queue; live reads increment `u64` access count, clone the value, and move its key to MRU without changing expiry.

**Why:** Dual-ring ranks hot keys for replication but preserves fixed L1 TTL. Its counter uses ordinary `+= 1`, not saturation, so it has no explicit overflow protection.

### `DualRingInner::put`

```rust
/// Insert into L1 using the real clock.
fn put(&mut self, key: String, value: String) {
    self.put_at(key, value, Instant::now());
}
```

**Types and what:** Consumes two `String` values and delegates insertion to `put_at` using the current monotonic clock.

**Why:** The production API uses real time without duplicating the insertion logic used by deterministic tests.

### `DualRingInner::put_at`

```rust
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
```

**Types and what:** Consumes key/value, removes any old queue position, replaces the map entry with count zero and deadline `now + T`, pushes MRU, and evicts deque-front entries until capacity holds.

**Why:** A replacement is a fresh fill and cannot leave duplicate queue entries. Capacity zero is handled by inserting and immediately evicting.

### `DualRingInner::delete`

```rust
fn delete(&mut self, key: &str) -> bool {
    let existed = self.main_map.remove(key).is_some();
    if existed {
        if let Some(pos) = self.main_order.iter().position(|k| k == key) {
            self.main_order.remove(pos);
        }
    }
    existed
}
```

**Types and what:** Removes the key from the L1 map and, if present, removes its queue record. The `bool` result reports stored presence, including an expired but not yet cleaned entry.

**Why:** Deleting both records preserves the invariant. This is local L1 deletion; dual-ring’s L2 snapshots are not invalidated.

### `DualRingInner::live_len`

```rust
fn live_len(&self) -> usize {
    let now = Instant::now();
    self.main_map
        .values()
        .filter(|e| e.expires_at > now)
        .count()
}
```

**Types and what:** Reads `Instant::now()` and returns a `usize` count of L1 entries whose deadlines are still strictly in the future.

**Why:** The count reports usable L1 values without mutating recency or including L2 replicas or expired stored entries.

### `DualRingInner::evict_expired`

```rust
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
```

**Types and what:** Uses the real clock to collect expired L1 keys into `Vec<String>`, then removes each from both map and queue.

**Why:** Collect-then-remove avoids borrow conflicts and reclaims expired L1 storage without changing L2 snapshots.

### `DualRingInner::top_k_hot_at`

```rust
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
```

**Types and what:** This `#[cfg(test)]` helper filters live entries at an explicit `Instant`, sorts by descending count with key ordering for ties, truncates to `k`, and returns owned `(String, String)` pairs.

**Why:** It exposes ranking without network batches for unit tests. It does not filter primary ownership and is not the production replication selector.

### `DualRingInner::replication_batches_at`

```rust
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
```

**Types and what:** Filters live L1 entries to those owned by `sender`, ranks by descending hits then key, and takes a global `top_k`. A `BTreeMap` creates an empty vector for every other ring member before selected wire entries are grouped by designated backup; it returns ordered `ReplicationBatch` values.

**Why:** Ownership filtering prevents foreign promotions from being re-replicated. Every peer gets a complete snapshot, including an empty batch when necessary to remove formerly hot replicas; deterministic ordering simplifies tests.

### `DualRingInner::replicate_stats`

```rust
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
```

**Types and what:** Under the caller’s lock, counts all L2 entries and each source’s entries, finds up to five live L1 keys by descending count, and counts live L1 values at one `Instant`. It returns an owned `ReplicateStats`.

**Why:** Diagnostics can be serialized after releasing the lock. Unlike replication ranking, the stats sort has no explicit key tie-breaker and includes all live L1 keys regardless of ownership.

### `DualRingInner::receive_replicate`

```rust
/// Receive a replication snapshot from `from_node`.
pub fn receive_replicate(
    &mut self,
    from_node: NodeId,
    version: SnapshotVersion,
    entries: Vec<ReplicateEntry>,
) -> bool {
    self.hot_store.replace_from(from_node, version, entries)
}
```

**Types and what:** Consumes one primary’s versioned vector and forwards it to `HotStore::replace_from`, returning its acceptance boolean.

**Why:** A narrow state-level entry point lets tests exercise snapshot replacement and ordering directly without HTTP.

### `DualRingInner::promote_requested_replica`

```rust
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
```

**Types and what:** Clones only `key` from the named primary’s L2 snapshot, inserts it into L1 at `now`, and performs a read at the same time to record the served hit.

**Why:** One-key promotion preserves scarce L1 space instead of copying a whole snapshot. L2 retains its copy; the L1 insertion starts a fresh fixed TTL, and capacity zero or zero TTL can still yield a miss.

### `DualRingInner::l2_lookup_at`

```rust
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
```

**Types and what:** Computes `KeyPlacement` using the supplied ring, returns `None` unless `this_server` is the backup, and then promotes the requested key from the calculated primary.

**Why:** A cached replica is served only through its designated fallback location; primary and uninvolved servers must not treat arbitrary L2 contents as valid fallback.

### `DualRingCache::new`

```rust
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
```

**Types and what:** Accepts TTL, capacity, top-K, explicit server index, and owned `DualRing`. It asserts this server is a member before allocating shared state and a shared replaceable ring.

**Why:** Failing immediately on invalid local membership prevents creating a cache with no legitimate place in its supplied topology.

### `DualRingCache::spawn_replication_task`

```rust
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
```

**Types and what:** From `&Arc<Self>`, captures shared handles and spawns a Tokio loop. Each iteration sleeps, creates an epoch/sequence version, builds all batches under short locks, releases those guards, reads `PEER_URLS`, and POSTs each batch using one reqwest client. The peer list is the full configured topology excluding this server, in ascending index order.

**Why:** This proactively copies hot values and sends empty snapshots to clear obsolete selections. No cache lock spans HTTP awaits; missing URL slots are skipped and send errors are ignored. The task has no returned shutdown handle, and a successful send is not checked for a successful HTTP status.

### `DualRingCache::receive_replicate`

```rust
/// Handle a `/replicate` POST from a peer node.
/// Replaces the hot store for `from_node` only when this snapshot is newer.
pub fn receive_replicate(&self, payload: ReplicatePayload) {
    self.inner.lock().unwrap().receive_replicate(
        payload.from_node,
        payload.version,
        payload.entries,
    );
}
```

**Types and what:** Consumes `ReplicatePayload`, locks shared state, and passes its source, version, and vector to the snapshot receiver. The public return is unit, so it discards the receiver’s acceptance boolean.

**Why:** The source snapshot is replaced atomically with respect to cache operations. Callers cannot infer acceptance from this wrapper’s return value.

### `DualRingCache::replicate_stats`

```rust
/// Return a diagnostic snapshot of the hot store and hot key rankings.
pub fn replicate_stats(&self) -> ReplicateStats {
    self.inner.lock().unwrap().replicate_stats()
}
```

**Types and what:** Locks shared state and returns its owned diagnostic `ReplicateStats` snapshot.

**Why:** The response can be serialized without retaining a mutex guard or exposing mutable internal collections.

### `DualRingCache::replicas_from`

```rust
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
```

**Types and what:** Clones one primary’s L2 entries under the mutex, uses an empty vector if absent, then sorts by ascending key before returning `Vec<ReplicateEntry>`.

**Why:** A deterministic owned snapshot supports controller-driven reconfiguration handoff independently of hash-map iteration order.

### `DualRingCache::get_l1`

```rust
/// Check only this node's level-1 main cache.
pub fn get_l1(&self, key: &str) -> Option<String> {
    self.inner.lock().unwrap().main_cache_get(key)
}
```

**Types and what:** Locks the shared state and calls the real-clock L1 lookup with a borrowed key, returning `Option<String>`.

**Why:** The normal path remains an L1 lookup; selecting a backup is a distinct client/request decision.

### `DualRingCache::get_from_backup`

```rust
/// Check this node's level-2 replica store and promote a replica only when
/// this node is the designated backup for `key`.
pub fn get_from_backup(&self, key: &str) -> Option<String> {
    let ring = self.ring.read().unwrap();
    self.inner
        .lock()
        .unwrap()
        .l2_lookup_at(key, self.server_index, &ring, Instant::now())
}
```

**Types and what:** Holds the ring read guard, then locks state and calls `l2_lookup_at` with this server’s ID, the borrowed ring, key, and current `Instant`. The result owns an optional value.

**Why:** Fallback validates the designated backup against a coherent local ring view. Calling this API is explicit; a normal L1 miss does not invoke it automatically.

### `DualRingCache::replace_ring`

```rust
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
```

**Types and what:** Returns `Result<(), &'static str>`: a static error message if this server is absent from the proposed membership; otherwise replaces the shared ring with a new `Arc` and returns `Ok(())`.

**Why:** The guard prevents excluding a cache from its own new view. This updates only local placement, so callers still need coordinated membership distribution and any data handoff.

### `<DualRingCache as Clone>::clone`

```rust
fn clone(&self) -> Self {
    DualRingCache {
        inner: Arc::clone(&self.inner),
        top_k: self.top_k,
        server_index: self.server_index,
        ring: Arc::clone(&self.ring),
    }
}
```

**Types and what:** Returns `Self` with cloned `Arc` handles and copied scalar configuration. Both storage and the replaceable ring remain shared.

**Why:** A clone must observe the same cache and membership changes; it does not create independent entries or start another replication loop.

### `<DualRingCache as CacheBackend>::get`

```rust
fn get(&self, key: &str) -> Option<String> {
    self.get_l1(key)
}
```

**Types and what:** Delegates the borrowed key to `get_l1`, returning an owned `Option<String>` from L1 only.

**Why:** The common storage trait cannot represent a lease grant and does not decide when a client should attempt a backup.

### `<DualRingCache as CacheBackend>::put`

```rust
fn put(&self, key: String, value: String) {
    self.inner.lock().unwrap().put(key, value);
}
```

**Types and what:** Locks shared state and transfers owned key/value strings to the real-clock put method, returning unit.

**Why:** All inserts follow the same LRU and fresh-fill rules. In combined, this generic put does not validate or clear a lease; coordinated fills use `complete_fill`.

### `<DualRingCache as CacheBackend>::delete`

```rust
fn delete(&self, key: &str) -> bool {
    self.inner.lock().unwrap().delete(key)
}
```

**Types and what:** Under the shared mutex, removes the L1 key and its recency record and returns whether a stored L1 value existed.

**Why:** Local deletion maintains LRU consistency. It does not clear L2 replicas.

### `<DualRingCache as CacheBackend>::live_len`

```rust
fn live_len(&self) -> usize {
    self.inner.lock().unwrap().live_len()
}
```

**Types and what:** Counts unexpired L1 entries and returns `usize` by delegating under the lock to the real-clock state method.

**Why:** This excludes L2 and expired stored entries.

### `<DualRingCache as CacheBackend>::evict_expired`

```rust
fn evict_expired(&self) {
    self.inner.lock().unwrap().evict_expired();
}
```

**Types and what:** Removes expired L1 entries and queue records by delegating to the state cleanup under its mutex.

**Why:** This reclaims L1 memory without modifying L2.

## Strategy goal

The goal is to keep selected popular values available on a distinct backup before a primary fails, reducing the backing-store load caused by loss of that primary’s L1 cache. Clients and nodes share deterministic placement: L1 chooses the normal primary, L2 chooses a distinct backup. The cache itself does not detect network failure; the caller decides when to use the explicit backup API.

L1 uses fixed TTL and one LRU capacity budget. Valid reads update recency and the hit counter without extending expiry. Replication selects up to top-K live entries actually owned by the sender, groups them by designated backup, and sends complete versioned snapshots to every other ring member. Empty batches clear old selections. Fallback copies only the requested value into L1 with a fresh local TTL and records a hit, leaving the source L2 snapshot intact.

L2 is separate memory with no TTL or LRU budget. If a primary stops sending, its last accepted replicas can remain indefinitely; version ordering prevents older delivery from replacing newer snapshots but is not a database freshness guarantee. Wall-clock epochs are not guaranteed unique or monotonic across restarts. HTTP replication is best-effort, and peer addressing assumes the documented full topology ordering. Consistent membership replacement must be coordinated externally.

The current lock acquisition order also deserves attention before concurrency stress claims: the replication batch collection takes the inner mutex then the ring read lock, while backup lookup takes the ring read lock then the inner mutex. With a waiting ring writer, progress can depend on the platform’s RwLock scheduling. The current in-memory tests do not establish freedom from such contention. These implementation limits do not change the intended research question: only controlled failure/recovery experiments can establish whether replication improves metastability resistance.

## Testing

The current strategy test module contains **16 tests**. These are pure in-memory tests: they do not start Cassandra, HTTP servers, Docker, or a Tokio replication loop. Most time-sensitive checks capture one `Instant` and supply offsets to state methods rather than sleeping. The code below includes every current test and its helper functions.

Run from the repository root:

```sh
cargo test -p twin_ring_core --offline --locked cache_dual_ring::
```

For the whole core suite and workspace compatibility check:

```sh
cargo test -p twin_ring_core --offline --locked
cargo check --workspace --offline --locked
```

The sixteen tests cover placement, member removal, fixed-TTL L1 behavior, LRU capacity, ranking, replica replacement, designated-backup promotion, owned batching, empty clears, and snapshot ordering. They do not run the HTTP sender, coordinated reconfiguration, concurrent locking, or failure recovery. Equal-count ranking ties and duplicate snapshot versions are implemented but not directly asserted by these tests. The file’s opening comment describing placement-only coverage is stale.

### `placement_for`

```rust
/// Test convenience only. Production code constructs one `DualRing` when the
/// membership view changes and reuses it for every lookup.
fn placement_for(key: &str, server_count: usize) -> Option<KeyPlacement> {
    DualRing::new(0..server_count).map(|ring| ring.placement_for(key))
}
```

**Types and what:** Builds a ring from the `0..server_count` range and maps a successful construction to one `KeyPlacement`.

**Why:** Tests can express small topologies tersely; production reuses a ring instead of rebuilding it for every key.

### `three_node_ring`

```rust
fn three_node_ring() -> DualRing {
    DualRing::new([0, 1, 2]).expect("three servers can form two rings")
}
```

**Types and what:** Constructs membership `[0, 1, 2]` and unwraps its expected valid `DualRing`.

**Why:** A common deterministic topology keeps replica tests focused on their state transitions.

### `version`

```rust
fn version(sequence: u64) -> SnapshotVersion {
    SnapshotVersion {
        epoch_ms: 1,
        sequence,
    }
}
```

**Types and what:** Returns a `SnapshotVersion` with fixed epoch 1 and a supplied `u64` sequence.

**Why:** Tests isolate delivery ordering within one simulated process epoch.

### `the_same_key_always_has_the_same_primary_and_backup`

```rust
#[test]
fn the_same_key_always_has_the_same_primary_and_backup() {
    let first = placement_for("account:42", 3).expect("three servers can form two rings");
    let second = placement_for("account:42", 3).expect("three servers can form two rings");

    assert_eq!(first, second);
}
```

**Types and what:** Constructs placement twice for `account:42` and asserts identical primary/backup pairs.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `primary_and_backup_are_both_real_servers_and_are_distinct`

```rust
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
```

**Types and what:** Samples 1,000 keys in a three-node topology and checks both indices are valid and unequal.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `placement_works_for_keys_that_do_not_follow_the_experiment_key_name`

```rust
#[test]
fn placement_works_for_keys_that_do_not_follow_the_experiment_key_name() {
    let placement =
        placement_for("customer/42/preferences", 3).expect("three servers can form two rings");

    assert!(placement.primary < 3);
    assert!(placement.backup < 3);
}
```

**Types and what:** Checks valid placement for a slash-separated application key instead of the experiment’s numbered-key pattern.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `a_backup_requires_at_least_two_servers`

```rust
#[test]
fn a_backup_requires_at_least_two_servers() {
    assert_eq!(placement_for("key42", 0), None);
    assert_eq!(placement_for("key42", 1), None);
}
```

**Types and what:** Asserts that zero- and one-server constructions return `None`.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `membership_order_does_not_change_the_ring`

```rust
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
```

**Types and what:** Builds ordered and permuted membership, checks normalized member slices, and compares placement for 1,000 keys.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `removing_a_server_preserves_l1_owners_for_keys_it_did_not_own`

```rust
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
```

**Types and what:** Compares 10,000 keys before and after removing server 1: keys formerly owned by it move to survivors, while other L1 owners stay unchanged; both populations must be nonempty.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `l1_read_updates_hotness_and_recency_but_not_the_ttl`

```rust
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
```

**Types and what:** Reads a key at second 9, checks hit count one and MRU order, then verifies a miss at the unchanged second-10 deadline.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `l1_capacity_evicts_the_least_recently_used_entry`

```rust
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
```

**Types and what:** Reads one of two stored keys before a third insertion and asserts eviction of the untouched key.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `hot_key_selection_uses_live_entries_and_a_deterministic_tie_breaker`

```rust
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
```

**Types and what:** Builds distinct hit counts for three keys, asserts descending ranking, and checks that no entries qualify at their expiry time. Despite the name, it does not directly exercise equal-count key ordering.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `a_replication_snapshot_replaces_only_that_primarys_previous_snapshot`

```rust
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
```

**Types and what:** Installs snapshots from two primaries, updates one, and asserts removal of that source’s old key while preserving the other source.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `promoting_a_replica_copies_only_the_requested_key_into_l1`

```rust
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
```

**Types and what:** Installs two peer replicas alongside a native L1 value, promotes one, and checks native survival, one-key promotion, hit count one, and retention of the unrequested L2 key.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `only_the_designated_l2_backup_can_promote_a_replica`

```rust
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
```

**Types and what:** Provides the same source replica to backup, primary, and uninvolved states; only the designated backup returns the value and promotes it.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `replication_sends_only_owned_hot_keys_to_their_designated_backups`

```rust
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
```

**Types and what:** Makes a foreign key more popular than two owned keys, then verifies a top-two selection includes only owned values and targets each designated backup.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `an_empty_snapshot_clears_replicas_that_are_no_longer_hot`

```rust
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
```

**Types and what:** Builds batches from an empty primary, finds the designated peer’s empty batch, delivers it with a newer version, and checks removal of an earlier replica.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `an_older_delayed_snapshot_cannot_restore_stale_replicas`

```rust
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
```

**Types and what:** Accepts version 2 then rejects version 1, asserting the newer value remains and the old key is absent.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `a_newer_process_epoch_can_replace_a_pre_restart_snapshot`

```rust
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
```

**Types and what:** Accepts epoch 10/sequence 99 then epoch 11/sequence 1 and checks the newer epoch replaces the previous keys despite its lower sequence.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

