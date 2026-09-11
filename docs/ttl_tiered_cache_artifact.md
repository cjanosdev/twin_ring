# TTL-tiered cache artifact

Sources: [crates/twin_ring_core/src/cache_ttl_tiered/mod.rs](../crates/twin_ring_core/src/cache_ttl_tiered/mod.rs).

Tests: [ttl_tiered_tests.rs](../crates/twin_ring_core/src/cache_ttl_tiered/ttl_tiered_tests.rs).

Read in order: [Data structures](#data-structures), [Implementations](#implementations), [Functions](#functions), [Strategy goal](#strategy-goal), and [Testing](#testing). Each item shows its complete source code before its explanation. Full `impl` blocks are intentionally repeated as individual functions in the next section so both the type-level grouping and each operation can be studied on their own. Code is copied from the current source, including attributes and attached comments; these are excerpts, not standalone compilable files.

## Data structures

### `WARM_HITS`

```rust
const WARM_HITS: u32 = 2;
```

**Types and what:** The `u32` constant is 2, matching the entry hit counter and the warm-promotion match arm.

**Why:** Naming the threshold makes the experimental policy visible separately from the storage mechanism.

### `HOT_HITS`

```rust
const HOT_HITS: u32 = 8;
```

**Types and what:** The `u32` constant is 8, marking the read that promotes a live warm entry to lifetime 4T.

**Why:** A fixed second threshold bounds promotion rather than extending the deadline on all subsequent reads.

### `TieredEntry`

```rust
/// One value and the history of its current fill.
struct TieredEntry {
    value: String,
    filled_at: Instant,
    expires_at: Instant,
    hit_count: u32,
}
```

**Types and what:** `String` owns the value; `Instant` records the last fill and its current deadline; `u32` counts successful reads of this fill. New entries are cold, and no separate tier enum or tier stores are needed.

**Why:** Keeping the original fill time makes promotion bounded: the deadline can become fill + 2T or fill + 4T, rather than sliding forward on every read.

### `TtlTieredState`

```rust
/// One shared capacity budget and least-to-most recently used ordering.
/// Every stored key appears exactly once in the queue. Expired entries count
/// toward capacity until lookup, eviction, deletion, or cleanup removes them.
struct TtlTieredState {
    entries: HashMap<String, TieredEntry>,
    lru_order: VecDeque<String>,
    max_entries: usize,
    base_ttl: Duration,
}
```

**Types and what:** `HashMap<String, TieredEntry>` stores values, while `VecDeque<String>` orders keys from least to most recently used. `usize` sets the entry budget and `Duration` sets T. Every stored key has exactly one queue record; expired entries occupy capacity until removed.

**Why:** All popularity classes must compete for the same memory budget. Updating the map and queue together prevents duplicate recency records and incorrect eviction.

### `TtlTieredCache`

```rust
/// Thread-safe TTL-tiered cache. Clones share state; it contains no DB client.
#[derive(Clone)]
pub struct TtlTieredCache {
    state: Arc<Mutex<TtlTieredState>>,
}
```

**Types and what:** `Arc<Mutex<TtlTieredState>>` is a reference-counted handle to one locked state. Derived `Clone` clones the handle and shares the cache rather than copying entries.

**Why:** Reads change hit counts and recency, so they also require exclusive access. The public wrapper provides thread-safe access without owning a database client.

## Implementations

An inherent `impl Type` defines operations on that type. `impl Trait for Type` supplies a shared interface. `Self` means the implementing type, `&self` borrows it, and `&mut self` permits mutation. Public wrappers use locks for mutation through a shared reference. Derives shown on data structures generate their trait implementations; they have no handwritten implementation block to reproduce.

### `impl TieredEntry`

```rust
impl TieredEntry {
    fn new(value: String, filled_at: Instant, base_ttl: Duration) -> Self {
        Self {
            value,
            filled_at,
            expires_at: filled_at + base_ttl,
            hit_count: 0,
        }
    }

    /// Called only after checking that this entry is still live.
    fn record_hit(&mut self, base_ttl: Duration) {
        self.hit_count = self.hit_count.saturating_add(1);
        match self.hit_count {
            WARM_HITS => self.expires_at = self.filled_at + base_ttl * 2,
            HOT_HITS => self.expires_at = self.filled_at + base_ttl * 4,
            _ => {}
        }
    }
}
```

**Types and what:** These inherent methods construct one entry and update its popularity. `Self` means `TieredEntry`; `&mut self` allows updating the counter and deadline.

**Why:** Keeping promotion here isolates the T/2T/4T policy. The state lookup remains responsible for checking expiry before calling `record_hit`.

### `impl TtlTieredState`

```rust
impl TtlTieredState {
    fn new(max_entries: usize, base_ttl: Duration) -> Self {
        Self {
            entries: HashMap::with_capacity(max_entries),
            lru_order: VecDeque::with_capacity(max_entries),
            max_entries,
            base_ttl,
        }
    }

    fn get(&mut self, key: &str, now: Instant) -> Option<String> {
        let entry = self.entries.get(key)?;
        // Expiration is checked BEFORE incrementing hits: reads cannot revive
        // expired data by promoting it into a longer-lived class.
        if entry.expires_at <= now {
            self.delete(key);
            return None;
        }

        let entry = self.entries.get_mut(key).unwrap();
        entry.record_hit(self.base_ttl);
        let value = entry.value.clone();
        self.remove_from_order(key);
        self.lru_order.push_back(key.to_owned());
        Some(value)
    }

    /// A put is a fresh fill, including replacement of an existing value.
    fn put(&mut self, key: String, value: String, now: Instant) {
        self.delete(&key);
        if self.max_entries == 0 {
            return;
        }
        if self.entries.len() == self.max_entries {
            let oldest = self
                .lru_order
                .pop_front()
                .expect("stored keys have LRU order");
            self.entries.remove(&oldest);
        }
        let entry = TieredEntry::new(value, now, self.base_ttl);
        self.entries.insert(key.clone(), entry);
        self.lru_order.push_back(key);
    }

    fn remove_from_order(&mut self, key: &str) {
        if let Some(position) = self.lru_order.iter().position(|candidate| candidate == key) {
            self.lru_order.remove(position);
        }
    }

    fn delete(&mut self, key: &str) -> bool {
        let removed = self.entries.remove(key).is_some();
        if removed {
            self.remove_from_order(key);
        }
        removed
    }

    fn live_len(&self, now: Instant) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.expires_at > now)
            .count()
    }

    fn evict_expired(&mut self, now: Instant) {
        let expired: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.expires_at <= now)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            self.delete(&key);
        }
    }
}
```

**Types and what:** These inherent methods operate on the map, queue, and TTL policy with explicitly supplied `Instant` values. Mutating operations take `&mut self`; `live_len` only borrows the state.

**Why:** Separating state transitions from locking and the real clock makes expiry boundaries deterministic to test and keeps one shared LRU invariant.

### `impl TtlTieredCache`

```rust
impl TtlTieredCache {
    pub fn new(default_ttl: Duration, max_entries: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(TtlTieredState::new(max_entries, default_ttl))),
        }
    }
}
```

**Types and what:** The public constructor wraps `TtlTieredState` in a mutex and reference-counted handle. `Self` denotes the public cache.

**Why:** This is the boundary between the deterministic in-memory state and the thread-safe cache used by callers.

### `impl CacheBackend for TtlTieredCache`

```rust
impl CacheBackend for TtlTieredCache {
    fn get(&self, key: &str) -> Option<String> {
        let mut state = self.state.lock().unwrap();
        state.get(key, Instant::now())
    }

    fn put(&self, key: String, value: String) {
        let mut state = self.state.lock().unwrap();
        // Fill time starts after acquiring the lock, so lock waiting does not
        // consume the lifetime of a value that has not been inserted yet.
        state.put(key, value, Instant::now());
    }

    fn delete(&self, key: &str) -> bool {
        self.state.lock().unwrap().delete(key)
    }

    fn live_len(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.live_len(Instant::now())
    }

    fn evict_expired(&self) {
        let mut state = self.state.lock().unwrap();
        state.evict_expired(Instant::now());
    }
}
```

**Types and what:** This implements the shared `CacheBackend: Send + Sync` interface: owned optional reads, owned inserts, boolean deletion, a live count, and expiry cleanup. Interior mutability through a mutex permits mutations behind `&self`.

**Why:** Callers can use a common cache interface while each strategy preserves its own storage policy. For combined and dual-ring, this trait only exposes L1 operations; explicit strategy APIs handle leases or backup lookup.

## Functions

`String` is owned text and `&str` is a borrowed view. `Option<T>` distinguishes a result from absence; `Result<T, E>` distinguishes success from an error. `Duration` is a time span, while `Instant` is a monotonic time point. `usize` is used for counts and indices; `u32` and `u64` have fixed unsigned widths. `Vec` is a growable sequence, `HashMap` associates keys with values, and `VecDeque` supplies the LRU queue. `Arc` shares ownership, `Mutex` grants exclusive access, and `RwLock` separates shared reads from exclusive replacement. Lock calls using `unwrap()` panic on poisoning rather than recovering.

### `TieredEntry::new`

```rust
fn new(value: String, filled_at: Instant, base_ttl: Duration) -> Self {
    Self {
        value,
        filled_at,
        expires_at: filled_at + base_ttl,
        hit_count: 0,
    }
}
```

**Types and what:** Consumes the owned value, records `filled_at: Instant`, sets expiry to fill + `base_ttl: Duration`, and initializes the `u32` hit count to zero. `Self` is the new entry.

**Why:** Every insertion and replacement must start cold with an independent fill history.

### `TieredEntry::record_hit`

```rust
/// Called only after checking that this entry is still live.
fn record_hit(&mut self, base_ttl: Duration) {
    self.hit_count = self.hit_count.saturating_add(1);
    match self.hit_count {
        WARM_HITS => self.expires_at = self.filled_at + base_ttl * 2,
        HOT_HITS => self.expires_at = self.filled_at + base_ttl * 4,
        _ => {}
    }
}
```

**Types and what:** Mutably increments the `u32` counter with `saturating_add`. Exactly hits 2 and 8 set expiry to original fill + 2T and + 4T; all other counts leave it unchanged.

**Why:** Saturation prevents wraparound and the original fill timestamp prevents sliding expiry. The caller must first establish that the entry is live.

### `TtlTieredState::new`

```rust
fn new(max_entries: usize, base_ttl: Duration) -> Self {
    Self {
        entries: HashMap::with_capacity(max_entries),
        lru_order: VecDeque::with_capacity(max_entries),
        max_entries,
        base_ttl,
    }
}
```

**Types and what:** Allocates an empty map and deque with capacity hints, stores the `usize` entry limit and base `Duration`, and returns `Self`.

**Why:** The allocation hints reduce initial growth; the explicit limit in `put`, rather than allocation capacity, enforces the cache budget.

### `TtlTieredState::get`

```rust
fn get(&mut self, key: &str, now: Instant) -> Option<String> {
    let entry = self.entries.get(key)?;
    // Expiration is checked BEFORE incrementing hits: reads cannot revive
    // expired data by promoting it into a longer-lived class.
    if entry.expires_at <= now {
        self.delete(key);
        return None;
    }

    let entry = self.entries.get_mut(key).unwrap();
    entry.record_hit(self.base_ttl);
    let value = entry.value.clone();
    self.remove_from_order(key);
    self.lru_order.push_back(key.to_owned());
    Some(value)
}
```

**Types and what:** Borrows the key as `&str`, returns `None` for absence or expiry at/before `now`, otherwise records a hit, clones the value into `Option<String>`, and moves the key to the queue back. The first map borrow ends before subsequent mutation.

**Why:** Expiry must precede promotion so dead data cannot revive. Owned return data can leave the mutex, and every successful read must update shared recency.

### `TtlTieredState::put`

```rust
/// A put is a fresh fill, including replacement of an existing value.
fn put(&mut self, key: String, value: String, now: Instant) {
    self.delete(&key);
    if self.max_entries == 0 {
        return;
    }
    if self.entries.len() == self.max_entries {
        let oldest = self
            .lru_order
            .pop_front()
            .expect("stored keys have LRU order");
        self.entries.remove(&oldest);
    }
    let entry = TieredEntry::new(value, now, self.base_ttl);
    self.entries.insert(key.clone(), entry);
    self.lru_order.push_back(key);
}
```

**Types and what:** Consumes key and value, deletes any existing copy, returns immediately for capacity zero, evicts the queue front if full, then inserts a new cold entry at `now` and adds its key at MRU.

**Why:** Deleting first avoids duplicate queue records and unnecessary eviction on replacement; `expect` relies on the map/queue invariant when the cache is full.

### `TtlTieredState::remove_from_order`

```rust
fn remove_from_order(&mut self, key: &str) {
    if let Some(position) = self.lru_order.iter().position(|candidate| candidate == key) {
        self.lru_order.remove(position);
    }
}
```

**Types and what:** Searches the `VecDeque<String>` for a borrowed key and removes its position if found. The iterator’s `position` returns `Option<usize>`.

**Why:** A single helper keeps recency updates, replacement, and deletion consistent. The search/removal costs O(n), an intentional simplicity tradeoff.

### `TtlTieredState::delete`

```rust
fn delete(&mut self, key: &str) -> bool {
    let removed = self.entries.remove(key).is_some();
    if removed {
        self.remove_from_order(key);
    }
    removed
}
```

**Types and what:** Removes the key from the L1 map and, if present, removes its queue record. The `bool` result reports stored presence, including an expired but not yet cleaned entry.

**Why:** Deleting both records preserves the invariant. This is local L1 deletion; dual-ring’s L2 snapshots are not invalidated.

### `TtlTieredState::live_len`

```rust
fn live_len(&self, now: Instant) -> usize {
    self.entries
        .values()
        .filter(|entry| entry.expires_at > now)
        .count()
}
```

**Types and what:** At the supplied `Instant`, counts entries whose expiry is strictly later and returns `usize`; it neither promotes nor removes entries.

**Why:** Live occupancy differs from allocated occupancy because expired entries can remain stored until cleanup or eviction.

### `TtlTieredState::evict_expired`

```rust
fn evict_expired(&mut self, now: Instant) {
    let expired: Vec<String> = self
        .entries
        .iter()
        .filter(|(_, entry)| entry.expires_at <= now)
        .map(|(key, _)| key.clone())
        .collect();
    for key in expired {
        self.delete(&key);
    }
}
```

**Types and what:** Collects owned keys with deadlines at/before `now` into `Vec<String>`, then deletes them from the map and queue.

**Why:** Collecting before mutation avoids changing a map while iterating over borrowed entries and preserves surviving entries’ deadlines and recency.

### `TtlTieredCache::new`

```rust
pub fn new(default_ttl: Duration, max_entries: usize) -> Self {
    Self {
        state: Arc::new(Mutex::new(TtlTieredState::new(max_entries, default_ttl))),
    }
}
```

**Types and what:** Accepts base lifetime as `default_ttl: Duration` and capacity as `usize`, constructs state, and wraps it in `Mutex` and `Arc`.

**Why:** The wrapper lets cloned public handles share one in-memory cache while internal tests can exercise state directly.

### `<TtlTieredCache as CacheBackend>::get`

```rust
fn get(&self, key: &str) -> Option<String> {
    let mut state = self.state.lock().unwrap();
    state.get(key, Instant::now())
}
```

**Types and what:** Locks state and calls the explicit-time get with `Instant::now()`, returning `Option<String>`.

**Why:** The trait’s shared `&self` is safe because the mutex protects the hit-count and LRU mutations.

### `<TtlTieredCache as CacheBackend>::put`

```rust
fn put(&self, key: String, value: String) {
    let mut state = self.state.lock().unwrap();
    // Fill time starts after acquiring the lock, so lock waiting does not
    // consume the lifetime of a value that has not been inserted yet.
    state.put(key, value, Instant::now());
}
```

**Types and what:** Locks first, then samples `Instant::now()` and transfers owned key/value strings into state insertion.

**Why:** Time spent waiting for the mutex must not consume the lifetime of a value that has not yet been filled.

### `<TtlTieredCache as CacheBackend>::delete`

```rust
fn delete(&self, key: &str) -> bool {
    self.state.lock().unwrap().delete(key)
}
```

**Types and what:** Locks state, deletes the borrowed key, and returns its stored-presence boolean.

**Why:** The map and queue change together for all cloned handles.

### `<TtlTieredCache as CacheBackend>::live_len`

```rust
fn live_len(&self) -> usize {
    let state = self.state.lock().unwrap();
    state.live_len(Instant::now())
}
```

**Types and what:** Locks state, samples the current clock, and returns the live-entry `usize` count.

**Why:** The shared trait reports live occupancy without counting expired entries or changing hit history.

### `<TtlTieredCache as CacheBackend>::evict_expired`

```rust
fn evict_expired(&self) {
    let mut state = self.state.lock().unwrap();
    state.evict_expired(Instant::now());
}
```

**Types and what:** Locks state and invokes cleanup at the current monotonic time, returning unit.

**Why:** Maintenance reclaims expired entries from both collections without promoting surviving data.

## Strategy goal

The goal is to retain repeatedly useful values longer so a constrained backing store may face fewer repeat misses during disruption. The policy is one shared LRU cache: 0–1 successful hits since fill give lifetime T, 2–7 give 2T, and 8+ give 4T. A deadline is always measured from the latest fill, never the latest read. The cache checks expiry first, and capacity eviction can remove even a hot entry.

For T = 10 seconds and fill time 0, the first hit leaves expiry at 10; the second moves it to 20; the eighth moves it to 40. Later reads cannot extend it beyond 40. A read exactly at the current deadline misses, even if it would otherwise cross a promotion threshold. Any replacement starts at zero hits with a new fill timestamp.

The thresholds are experimental starting parameters. This implementation has no lease or peer replica store. Its correctness tests do not establish better metastability resistance: that requires controlled workload experiments, including uniform-T and uniform-4T LRU comparisons. Map lookup is average O(1), but recency searches/removals are O(n) while holding the mutex; cleanup also performs repeated queue searches.

## Testing

The current strategy test module contains **16 tests**. These are pure in-memory tests: they do not start Cassandra, HTTP servers, Docker, or a Tokio replication loop. Most time-sensitive checks capture one `Instant` and supply offsets to state methods rather than sleeping. The code below includes every current test and its helper functions.

Run from the repository root:

```sh
cargo test -p twin_ring_core --offline --locked cache_ttl_tiered::
```

For the whole core suite and workspace compatibility check:

```sh
cargo test -p twin_ring_core --offline --locked
cargo check --workspace --offline --locked
```

The tests cover exact promotion/expiry boundaries, count saturation, replacement/refill, shared capacity and recency, deletion, cleanup, zero TTL/capacity, and public clone/trait behavior. The helper explicitly checks map/queue equality and uniqueness. They do not measure throughput, concurrent contention, or recovery under a constrained backing store.

### `assert_consistent`

```rust
fn assert_consistent(cache: &TtlTieredState) {
    let queued: HashSet<_> = cache.lru_order.iter().collect();
    let stored: HashSet<_> = cache.entries.keys().collect();
    assert_eq!(queued.len(), cache.lru_order.len(), "duplicate queue keys");
    assert_eq!(queued, stored, "map and queue disagree");
    assert!(cache.entries.len() <= cache.max_entries);
}
```

**Types and what:** Builds `HashSet` views of queued and stored keys, checks queue uniqueness, set equality, and the capacity bound through a borrowed state.

**Why:** These invariants detect bookkeeping corruption across otherwise independent cache operations.

### `a_new_fill_is_cold`

```rust
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
```

**Types and what:** Asserts the owned value, zero hits, original fill time, and fill + T expiry of a newly constructed entry.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `only_the_second_and_eighth_hits_extend_the_deadline`

```rust
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
```

**Types and what:** Records 20 hits and checks the exact count, unchanged fill time, and T/2T/4T deadline after every hit.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `hit_counter_saturates_instead_of_wrapping_to_cold`

```rust
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
```

**Types and what:** Sets the count to `u32::MAX`, records another hit, and asserts that both the saturated count and hot deadline remain unchanged.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `a_single_read_does_not_restart_cold_ttl`

```rust
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
```

**Types and what:** Reads at second 9 of a 10-second fill and misses at exactly second 10, then checks map/queue consistency.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `warm_promotion_uses_fill_time_not_read_time`

```rust
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
```

**Types and what:** Reads at seconds 8 and 9, checks expiry at second 20 rather than 29, confirms a hit at 19 and a miss at 20.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `the_eighth_hit_promotes_warm_to_hot_but_continuous_reads_cannot_extend_past_four_ttl`

```rust
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
```

**Types and what:** Makes seven early hits and an eighth at second 19, then checks continued hits before second 40 and a miss exactly at 40.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `an_expired_entry_cannot_be_promoted_at_either_threshold`

```rust
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
```

**Types and what:** Sets up one or seven prior hits and attempts the threshold-crossing read exactly at the existing cold or warm deadline; both cases must remove the entry and miss.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `a_hot_key_can_be_evicted_by_capacity`

```rust
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
```

**Types and what:** Promotes a key to hot in a two-entry cache, adds two later keys, and verifies that capacity still evicts the now-oldest hot key.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `reads_refresh_lru_order_across_the_shared_store`

```rust
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
```

**Types and what:** Refreshes a key in a full cache, checks a missing-key read, inserts another key, and asserts that the untouched older key was evicted.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `replacement_resets_popularity_and_does_not_evict_another_key`

```rust
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
```

**Types and what:** Replaces a hot value in a full cache and checks count zero, a new fill/deadline, the new value, and survival of the other key.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `refill_after_expiration_starts_cold`

```rust
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
```

**Types and what:** Expires a hot fill at 4T, refills at the same time, and checks that its new count and TTL return to the cold defaults.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `cleanup_obeys_each_class_deadline_without_resetting_survivors`

```rust
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
```

**Types and what:** Creates cold, warm, and hot entries and checks live counts and physical removal at T, 2T, and 4T, with map/queue invariants after each cleanup.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `deletion_and_reinsertion_do_not_duplicate_order_entries`

```rust
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
```

**Types and what:** Deletes, repeats deletion, and reinserts several keys, asserting presence booleans and map/queue uniqueness each time.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `zero_and_one_capacity_respect_the_single_budget`

```rust
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
```

**Types and what:** Exercises budgets zero and one with successive inserts; only the newest value can remain at capacity one, and none at zero.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `zero_ttl_expires_immediately`

```rust
#[test]
fn zero_ttl_expires_immediately() {
    let start = Instant::now();
    let mut cache = TtlTieredState::new(1, Duration::ZERO);
    cache.put("a".into(), "a".into(), start);
    assert_eq!(cache.live_len(start), 0);
    assert_eq!(cache.get("a", start), None);
    assert_consistent(&cache);
}
```

**Types and what:** Inserts with `Duration::ZERO` and checks zero live count and an immediate miss at the fill time.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

### `public_clones_and_trait_methods_share_one_cache`

```rust
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
```

**Types and what:** Uses a cloned public handle as `&dyn CacheBackend`, verifies cross-handle read/delete visibility, invokes maintenance, and checks the state invariants.

**Why:** This makes the stated behavior an executable regression check. The explicit setup and assertions above define what is covered; broader deployment or performance behavior is outside this test.

