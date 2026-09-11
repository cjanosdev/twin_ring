# Combined cache artifact

Sources: [crates/twin_ring_core/src/cache_combined/mod.rs](../crates/twin_ring_core/src/cache_combined/mod.rs).

Tests: [combined_tests.rs](../crates/twin_ring_core/src/cache_combined/combined_tests.rs).

Read in order: [Data structures](#data-structures), [Implementations](#implementations), [Functions](#functions), [Strategy goal](#strategy-goal), and [Testing](#testing). Each item shows its complete source code before its explanation. Full `impl` blocks are intentionally repeated as individual functions in the next section so both the type-level grouping and each operation can be studied on their own. Code is copied from the current source, including attributes and attached comments; these are excerpts, not standalone compilable files.

## Data structures

### `NodeId`

```rust
type NodeId = usize;
```

**Types and what:** This local alias names the source-primary identity used as the key of the replica map. Dual-ring aliases `ServerIndex`; combined aliases `usize`, which is also the underlying server-index type.

**Why:** The name distinguishes node identities from entry counts while preserving compatibility with shared replication payloads.

### `HotStore`

```rust
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
    version: crate::cache_dual_ring::SnapshotVersion,
    entries: HashMap<String, String>,
}
```

**Types and what:** The version identifies the accepted snapshot; `HashMap<String, String>` owns its key/value pairs. Combined uses the same `SnapshotVersion` type exported by dual-ring.

**Why:** Keeping the version even for an empty snapshot lets the receiver reject delayed older data after stale keys have been cleared.

### `CombinedInner`

```rust
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
```

**Types and what:** `entries` and `lru_order` hold one capacity-limited L1 cache. `default_ttl` is T; `leases`, `next_lease_token`, and `lease_duration` coordinate fills. `hot_store` holds separately budgeted peer replicas. Counts and capacity use `u64` and `usize` respectively.

**Why:** One mutex can make L1 lookup, lease acquisition, and completion atomic relative to each other. Keeping L2 separate preserves replicas when local L1 entries expire or are evicted.

### `CombinedLease`

```rust
/// Ownership token for one in-flight backing-store fill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CombinedLease {
    key: String,
    token: u64,
}
```

**Types and what:** An owned `String` key and private `u64` token identify one backing-store fill. The derives support cloning, debug output, and equality; cloning preserves the same capability, not a new acquisition.

**Why:** A stale requester must be distinguishable from a replacement holder for the same key. Private fields prevent callers from constructing arbitrary tokens through the public API.

### `CombinedLookup`

```rust
/// Result of a normal Combined L1 lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CombinedLookup {
    Hit(String),
    LeaseGranted(CombinedLease),
    LeaseHeld,
}
```

**Types and what:** `Hit(String)` returns an owned cached value, `LeaseGranted(CombinedLease)` authorizes a fill, and `LeaseHeld` reports an existing live holder. Equality and debug derives make the outcomes easy to assert in tests.

**Why:** An ordinary `Option<String>` miss cannot tell the caller whether it may query the backing store. This enum exposes that coordination decision explicitly.

### `CombinedEntry`

```rust
struct CombinedEntry {
    value: String,
    filled_at: std::time::Instant,
    expires_at: std::time::Instant,
    access_count: u64,
}
```

**Types and what:** An owned value, two monotonic `Instant` timestamps, and a `u64` access count describe one L1 fill. The counter drives both TTL promotion and replication ranking.

**Why:** Recording fill time separately from expiry allows the second and eighth successful reads to extend lifetime from the original fill. A replacement starts a new popularity history.

### `ActiveLease`

```rust
struct ActiveLease {
    token: u64,
    expires_at: std::time::Instant,
}
```

**Types and what:** `token: u64` records the current holder and `expires_at: Instant` records when its authority to complete a fill ends.

**Why:** The cache needs expiry state independently of the capability handed to a caller, so a stalled fill can be superseded without accepting its late result.

### `CombinedCache`

```rust
pub struct CombinedCache {
    inner: Arc<Mutex<CombinedInner>>,
    top_k: usize,
    default_ttl: Duration,
    server_index: NodeId,
    ring: Arc<RwLock<Arc<crate::cache_dual_ring::DualRing>>>,
}
```

**Types and what:** `Arc<Mutex<CombinedInner>>` shares cache state. `top_k` limits replication selection, `default_ttl` retains configuration, and `server_index` identifies this server. `Arc<RwLock<Arc<DualRing>>>` shares a replaceable immutable membership view.

**Why:** Clones and the background task must observe shared storage and membership. The inner `Arc` lets an immutable ring snapshot outlive a read lock.

### Shared replication types

Combined re-exports `ReplicatePayload` and uses dual-ring’s `ReplicateEntry` and `SnapshotVersion` directly. Their full definitions are repeated here so the wire types can be understood without leaving this guide.

#### `ReplicateEntry`

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicateEntry {
    pub key: String,
    pub value: String,
}
```

**Types and what:** Two owned strings form one transferable key/value pair. `Serialize` and `Deserialize` support the HTTP JSON payload; clone, debug, and equality derives support copying and assertions.

**Why:** The wire format carries the replicated value without exporting local expiry or hit-count state. Promotion creates fresh L1 bookkeeping on the receiving node.

#### `SnapshotVersion`

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

#### `ReplicatePayload`

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

The shared `DualRing` placement structures, complete implementations, and hash functions are documented in the [dual-ring artifact](dual_ring_cache_artifact.md#data-structures).

## Implementations

An inherent `impl Type` defines operations on that type. `impl Trait for Type` supplies a shared interface. `Self` means the implementing type, `&self` borrows it, and `&mut self` permits mutation. Public wrappers use locks for mutation through a shared reference. Derives shown on data structures generate their trait implementations; they have no handwritten implementation block to reproduce.

### `impl HotStore`

```rust
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
```

**Types and what:** The replica methods read one source’s entries and accept only a strictly newer complete snapshot. `Option<String>` owns a result; `bool` indicates whether replacement was accepted.

**Why:** Reading by source and replacing atomically preserves primary isolation and prevents delayed delivery from rolling a snapshot backward.

### `impl CombinedInner`

```rust
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
```

**Types and what:** These methods implement L1 TTL promotion and LRU, per-key lease coordination, explicit L2 promotion, and owned hot-key selection. The `_at` methods accept a monotonic time for deterministic transitions.

**Why:** Combining these transitions under one outer mutex lets a cache lookup and lease decision happen together. Database work belongs to the caller after the lookup returns.

### `impl CombinedCache`

```rust
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
```

**Types and what:** The public methods lock shared state, manage the replaceable ring, expose lease operations and explicit backup reads, and start asynchronous replication. `self: &Arc<Self>` lets the task capture shared handles.

**Why:** The wrapper connects the synchronous state machine to concurrent requests and network replication. Normal reads, coordinated fills, and backup reads remain distinct entry points; some copied source comments describing disabled leases or bulk promotion are stale.

### `impl Clone for CombinedCache`

```rust
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
```

**Types and what:** This trait implementation supplies `clone(&self) -> Self` by sharing the cache and ring `Arc` handles while copying scalar configuration.

**Why:** Each cloned handle must observe the same entries and membership. Cloning does not replicate data into a new cache or spawn a new task.

### `impl CacheBackend for CombinedCache`

```rust
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
```

**Types and what:** This implements the shared `CacheBackend: Send + Sync` interface: owned optional reads, owned inserts, boolean deletion, a live count, and expiry cleanup. Interior mutability through a mutex permits mutations behind `&self`.

**Why:** Callers can use a common cache interface while each strategy preserves its own storage policy. For combined and dual-ring, this trait only exposes L1 operations; explicit strategy APIs handle leases or backup lookup.

## Functions

`String` is owned text and `&str` is a borrowed view. `Option<T>` distinguishes a result from absence; `Result<T, E>` distinguishes success from an error. `Duration` is a time span, while `Instant` is a monotonic time point. `usize` is used for counts and indices; `u32` and `u64` have fixed unsigned widths. `Vec` is a growable sequence, `HashMap` associates keys with values, and `VecDeque` supplies the LRU queue. `Arc` shares ownership, `Mutex` grants exclusive access, and `RwLock` separates shared reads from exclusive replacement. Lock calls using `unwrap()` panic on poisoning rather than recovering.

### `HotStore::get_from`

```rust
fn get_from(&self, primary: NodeId, key: &str) -> Option<String> {
    self.by_node.get(&primary)?.entries.get(key).cloned()
}
```

**Types and what:** Uses `primary: NodeId` to find its snapshot, then a borrowed `&str` to find the value. `?` returns `None` for an absent source; `cloned` returns an owned `String` on a hit.

**Why:** A fallback must use the intended primary’s replica. Ownership lets the caller mutate L1 after the immutable L2 borrow ends.

### `HotStore::receive`

```rust
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
```

**Types and what:** Rejects versions less than or equal to the stored version. It consumes the entry vector into a new `HashMap<String, String>` and replaces only that primary’s snapshot, returning whether it accepted it. Duplicate keys in a payload collapse to the last inserted value.

**Why:** Complete replacement drops keys no longer selected; an empty snapshot clears entries while retaining the version fence. Other primaries remain untouched.

### `CombinedInner::new`

```rust
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
```

**Types and what:** Reserves map/queue space for `max_entries + 1`, records T and the L1 limit, starts an empty lease table with token zero and a 500 ms lease duration, and initializes L2 empty.

**Why:** Insertion may briefly exceed capacity before eviction. The lease deadline allows another requester to retry a fill that has stalled.

### `CombinedInner::remove_from_order`

```rust
fn remove_from_order(&mut self, key: &str) {
    if let Some(position) = self.lru_order.iter().position(|stored| stored == key) {
        self.lru_order.remove(position);
    }
}
```

**Types and what:** Searches the `VecDeque<String>` for a borrowed key and removes its position if found. The iterator’s `position` returns `Option<usize>`.

**Why:** A single helper keeps recency updates, replacement, and deletion consistent. The search/removal costs O(n), an intentional simplicity tradeoff.

### `CombinedInner::get_at`

```rust
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
```

**Types and what:** Returns `Option<String>` at an explicit `Instant`. It removes expired L1 entries before counting, saturates the `u64` access count, promotes at hits 2 and 8 using `filled_at`, clones a live value, and moves its key to MRU.

**Why:** The same successful-read history drives bounded TTL and hot-key ranking. Expiry-first ordering prevents promotion from reviving an expired value.

### `CombinedInner::get`

```rust
fn get(&mut self, key: &str) -> Option<String> {
    self.get_at(key, std::time::Instant::now())
}
```

**Types and what:** Calls `get_at` with the borrowed key and `Instant::now()`, returning its owned optional value.

**Why:** Production gets real-time behavior while tests can exercise the underlying method at exact times.

### `CombinedInner::lookup_or_acquire_at`

```rust
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
```

**Types and what:** First tries L1 at `now`; a hit returns `CombinedLookup::Hit`. A lease with expiry greater than `now` returns `LeaseHeld`; otherwise it increments the `u64` token with saturation and stores a new lease deadline.

**Why:** This atomic decision permits one same-key filler while a lease is live. It does not consult L2. Saturation means tokens cease to be unique at `u64::MAX`, so uniqueness is not mathematically unbounded.

### `CombinedInner::complete_fill_at`

```rust
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
```

**Types and what:** Borrows a `CombinedLease`, consumes the value, and returns false if no matching unexpired token exists at `now`. Otherwise it removes the lease, performs a fresh L1 put, and returns true.

**Why:** Validation prevents a late expired holder from overwriting a replacement fill. True reports acceptance, not guaranteed residency: zero capacity or zero TTL can still make the value immediately unavailable.

### `CombinedInner::abandon_fill`

```rust
fn abandon_fill(&mut self, lease: &CombinedLease) {
    if self
        .leases
        .get(&lease.key)
        .is_some_and(|active| active.token == lease.token)
    {
        self.leases.remove(&lease.key);
    }
}
```

**Types and what:** Checks the borrowed lease’s key and token, removing the registry entry only if they match. It returns unit and does not check expiry.

**Why:** A failing filler can release its own lease without removing a newer holder’s lease. Matching expired records can also be cleared safely before replacement.

### `CombinedInner::l2_lookup_at`

```rust
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
```

**Types and what:** Calculates placement from the borrowed `DualRing`, rejects a server that is not the designated backup, looks up only the primary’s requested replica, then puts and reads that single key in L1 at `now`.

**Why:** Requested-key promotion avoids bulk eviction of local L1 contents. The final read records hit one, while zero capacity or zero TTL can make the result `None`; L2 itself is retained.

### `CombinedInner::put_at`

```rust
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
```

**Types and what:** Removes existing map/queue records, consumes key/value into a fresh entry with count zero and expiry `now + T`, then pops LRU keys until the `usize` capacity limit holds.

**Why:** Every fill resets TTL and popularity. Inserting before the eviction loop also handles capacity zero, where the just-inserted value is removed.

### `CombinedInner::put`

```rust
fn put(&mut self, key: String, value: String) {
    self.put_at(key, value, std::time::Instant::now());
}
```

**Types and what:** Consumes two `String` values and delegates insertion to `put_at` using the current monotonic clock.

**Why:** The production API uses real time without duplicating the insertion logic used by deterministic tests.

### `CombinedInner::top_k_hot`

```rust
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
```

**Types and what:** At the real clock, filters live entries whose ring primary equals `owner: NodeId`, sorts by descending `u64` access count then ascending key, truncates to `k: usize`, and clones them into shared wire entries.

**Why:** Filtering ownership before ranking keeps promoted foreign replicas from consuming this primary’s top-K allowance. Sorting all eligible entries costs O(n log n).

### `CombinedCache::new`

```rust
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
```

**Types and what:** Builds shared combined state from TTL/capacity and retains top-K, explicit `server_index`, and a supplied `DualRing`. It returns `Self` and does not validate that the server belongs to the ring.

**Why:** One configuration powers request handling and replication. Despite its copied comment, node identity comes from this argument; only peer URLs are read from the environment later.

### `CombinedCache::receive_replicate`

```rust
/// Handle a `/replicate` POST from a peer node.
pub fn receive_replicate(&self, payload: ReplicatePayload) {
    self.inner.lock().unwrap().hot_store.receive(
        payload.from_node,
        payload.version,
        payload.entries,
    );
}
```

**Types and what:** Consumes `ReplicatePayload`, locks shared state, and passes its source, version, and vector to the snapshot receiver. The public return is unit, so it discards the receiver’s acceptance boolean.

**Why:** The source snapshot is replaced atomically with respect to cache operations. Callers cannot infer acceptance from this wrapper’s return value.

### `CombinedCache::lookup_or_acquire`

```rust
/// Acquire the right to fill a missing L1 key, or observe its current state.
pub fn lookup_or_acquire(&self, key: &str) -> CombinedLookup {
    self.inner
        .lock()
        .unwrap()
        .lookup_or_acquire_at(key, std::time::Instant::now())
}
```

**Types and what:** Locks shared state and calls `lookup_or_acquire_at` with a borrowed key and the real monotonic clock, returning `CombinedLookup`.

**Why:** The mutex spans L1 lookup and lease acquisition, then is released before the caller performs backing-store I/O.

### `CombinedCache::complete_fill`

```rust
pub fn complete_fill(&self, lease: &CombinedLease, value: String) -> bool {
    self.inner
        .lock()
        .unwrap()
        .complete_fill_at(lease, value, std::time::Instant::now())
}
```

**Types and what:** Locks shared state and forwards the borrowed lease, owned value, and current time to the validating fill method, returning its boolean.

**Why:** Completion and token validation occur within one protected transition, so a concurrent replacement cannot slip between validation and insertion.

### `CombinedCache::abandon_fill`

```rust
pub fn abandon_fill(&self, lease: &CombinedLease) {
    self.inner.lock().unwrap().abandon_fill(lease);
}
```

**Types and what:** Locks the state and delegates release of the borrowed `CombinedLease`; the result is unit.

**Why:** A caller handling a failed backing-store read can relinquish its matching token without exposing the lease registry.

### `CombinedCache::get_from_backup`

```rust
pub fn get_from_backup(&self, key: &str) -> Option<String> {
    let ring = self.ring.read().unwrap();
    self.inner.lock().unwrap().l2_lookup_at(
        key,
        self.server_index,
        &ring,
        std::time::Instant::now(),
    )
}
```

**Types and what:** Holds the ring read guard, then locks state and calls `l2_lookup_at` with this server’s ID, the borrowed ring, key, and current `Instant`. The result owns an optional value.

**Why:** Fallback validates the designated backup against a coherent local ring view. Calling this API is explicit; a normal L1 miss does not invoke it automatically.

### `CombinedCache::replace_ring`

```rust
pub fn replace_ring(&self, ring: crate::cache_dual_ring::DualRing) -> Result<(), &'static str> {
    if !ring.members().contains(&self.server_index) {
        return Err("this cache server is not in the proposed membership");
    }
    *self.ring.write().unwrap() = Arc::new(ring);
    Ok(())
}
```

**Types and what:** Returns `Result<(), &'static str>`: a static error message if this server is absent from the proposed membership; otherwise replaces the shared ring with a new `Arc` and returns `Ok(())`.

**Why:** The guard prevents excluding a cache from its own new view. This updates only local placement, so callers still need coordinated membership distribution and any data handoff.

### `CombinedCache::replicas_from`

```rust
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
```

**Types and what:** Locks L2, clones one primary’s snapshot into `Vec<ReplicateEntry>`, or returns an empty vector when absent. The iteration order is unspecified because the source is a `HashMap`.

**Why:** This exports owned values for callers such as handoff code without keeping the state lock or sharing internal map references.

### `CombinedCache::spawn_replication_task`

```rust
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
```

**Types and what:** Captures shared handles from `&Arc<Self>` and spawns a Tokio loop. After each sleep it increments a saturating sequence, selects owned live top-K entries, reads `PEER_URLS`, groups entries by designated backup, and POSTs versioned snapshots with a reusable reqwest client. Cache/ring guards used for selection end before HTTP awaits.

**Why:** Periodic copies aim to preserve hot values away from their primary. The task is best-effort and discards send results. It skips empty selection and absent destination groups, so it does not send the empty snapshots needed to clear those peers. Peer indexing uses modulo fallback, and selection and grouping take separate ring snapshots; these are current limitations, not guarantees inherited from dual-ring.

### `CombinedCache::get_internal`

```rust
/// Look up a key, falling back to the L2 hot store on a miss.
/// On hot-store hit: promotes ALL hot items from the source node into the main cache.
fn get_internal(&self, key: &str) -> Option<String> {
    // Fast path: main LRU+TTL cache
    if let Some(val) = self.inner.lock().unwrap().get(key) {
        return Some(val);
    }

    None
}
```

**Types and what:** Locks and queries only L1 through `CombinedInner::get`; it returns the hit or `None` without consulting L2 or acquiring a lease.

**Why:** This implements the shared storage get path. Its source comment describing bulk L2 promotion is stale; explicit backup and lease APIs implement those separate decisions.

### `<CombinedCache as Clone>::clone`

```rust
fn clone(&self) -> Self {
    CombinedCache {
        inner: Arc::clone(&self.inner),
        top_k: self.top_k,
        default_ttl: self.default_ttl,
        server_index: self.server_index,
        ring: Arc::clone(&self.ring),
    }
}
```

**Types and what:** Returns `Self` with cloned `Arc` handles and copied scalar configuration. Both storage and the replaceable ring remain shared.

**Why:** A clone must observe the same cache and membership changes; it does not create independent entries or start another replication loop.

### `<CombinedCache as CacheBackend>::get`

```rust
fn get(&self, key: &str) -> Option<String> {
    self.get_internal(key)
}
```

**Types and what:** Delegates the borrowed key to `get_internal`, returning an owned `Option<String>` from L1 only.

**Why:** The common storage trait cannot represent a lease grant and does not decide when a client should attempt a backup.

### `<CombinedCache as CacheBackend>::put`

```rust
fn put(&self, key: String, value: String) {
    self.inner.lock().unwrap().put(key, value);
}
```

**Types and what:** Locks shared state and transfers owned key/value strings to the real-clock put method, returning unit.

**Why:** All inserts follow the same LRU and fresh-fill rules. In combined, this generic put does not validate or clear a lease; coordinated fills use `complete_fill`.

### `<CombinedCache as CacheBackend>::delete`

```rust
fn delete(&self, key: &str) -> bool {
    let mut inner = self.inner.lock().unwrap();
    let existed = inner.entries.remove(key).is_some();
    inner.remove_from_order(key);
    existed
}
```

**Types and what:** Under the shared mutex, removes the L1 key and its recency record and returns whether a stored L1 value existed.

**Why:** Local deletion maintains LRU consistency. It does not clear L2 replicas or active combined leases.

### `<CombinedCache as CacheBackend>::live_len`

```rust
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
```

**Types and what:** Counts unexpired L1 entries and returns `usize` using a timestamp sampled before locking.

**Why:** This excludes L2 and expired stored entries. Lock waiting can make combined’s sampled live count slightly older than the protected state observation.

### `<CombinedCache as CacheBackend>::evict_expired`

```rust
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
```

**Types and what:** Removes expired L1 entries and queue records using a timestamp sampled before locking and a collect-then-remove pass.

**Why:** This reclaims L1 memory without modifying L2 or sweeping the lease registry; records expiring during lock waiting may survive until the next operation.

## Strategy goal

The goal is to combine three mitigations: bounded TTL promotion retains popular L1 values; a per-key lease selects one backing-store filler on a coordinated miss; and proactive hot-key replication provides an explicit L2 fallback on another server. Whether the combination improves metastability resistance beyond individual strategies remains an experimental question.

The current code implements T/2T/4T promotion at hits 2 and 8 from the original fill time. A successful coordinated lookup also counts as a hit. L1 misses can grant a 500 ms lease through `lookup_or_acquire`; callers perform I/O outside the cache lock and complete or abandon that lease. The common `CacheBackend::get` only checks L1 and never grants a lease. `get_from_backup` is separate, validates placement, and promotes just the requested replica.

L1 has one capacity limit. Leases and L2 snapshots are additional memory outside that limit; L2 has no expiry. The replication sender currently skips empty selections and peers without selected keys, so stale replicas can persist even while replication continues elsewhere. The receiver can accept an empty snapshot, but the sender does not generate all necessary clears. Snapshot ordering rejects older arrivals but does not establish database freshness. Generic puts and deletes do not invalidate outstanding leases or peer replicas, and token saturation eventually stops producing distinct lease identities.

Source comments claiming leases are disabled, access counts live in a separate map, or fallback promotes all replicas describe older designs. The complete code shown above is preserved verbatim; the explanations describe the actual bodies. The combined test file’s opening comment also predates its lease tests.

## Testing

The current strategy test module contains **6 tests**. These are pure in-memory tests: they do not start Cassandra, HTTP servers, Docker, or a Tokio replication loop. Most time-sensitive checks capture one `Instant` and supply offsets to state methods rather than sleeping. The code below includes every current test and its helper functions.

Run from the repository root:

```sh
cargo test -p twin_ring_core --offline --locked cache_combined::
```

For the whole core suite and workspace compatibility check:

```sh
cargo test -p twin_ring_core --offline --locked
cargo check --workspace --offline --locked
```

The six tests cover three L1 behaviors and three lease behaviors. They do not currently exercise L2 promotion, combined snapshot ordering or batching, abandonment, generic-put/lease interactions, counter exhaustion, zero capacity/TTL, public clone/trait behavior, or concurrent requests. Dual-ring’s tests cannot be treated as coverage of combined’s separate sender and state implementation.

### `l1_second_and_eighth_reads_promote_ttl_from_the_original_fill_time`

```rust
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
```

**Types and what:** Fills at a controlled start and checks combined’s deadlines after the first, second, and eighth reads: 10, 20, and 40 seconds from that fill.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `l1_reads_update_lru_order_and_capacity_evicts_the_oldest_key`

```rust
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
```

**Types and what:** Fills two keys, reads the first, inserts a third, and verifies that the unread second key alone was evicted.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `l1_read_does_not_extend_an_entry_before_a_promotion_threshold`

```rust
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
```

**Types and what:** Confirms a first read at second 9 still expires at second 10, before reaching the second-hit promotion.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `first_miss_gets_a_lease_and_a_second_miss_is_held`

```rust
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
```

**Types and what:** Performs two misses for the same key at one `Instant`; the first grants a token and the second reports `LeaseHeld`.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `only_the_current_lease_can_fill_and_then_read_as_a_hit`

```rust
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
```

**Types and what:** Acquires a token, completes at +1 ms, and observes the filled value at +2 ms through coordinated lookup. This test exercises successful completion; stale-token rejection is covered by the next test.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `expired_lease_can_be_replaced_but_cannot_fill_afterwards`

```rust
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
```

**Types and what:** Reacquires at +501 ms, beyond the 500 ms duration, then checks rejection of the old token and acceptance of its replacement.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

