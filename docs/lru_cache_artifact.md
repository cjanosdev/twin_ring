# LRU cache artifact

Source: `crates/twin_ring_core/src/cache_lru/mod.rs`. Tests: `cache_lru/lru_tests.rs`.

## Actual data structures

```rust
struct Entry {
    value: String,
    expires_at: Instant,
}

struct LruState {
    entries: HashMap<String, Entry>,
    lru_order: VecDeque<String>,
    max_entries: usize,
}

pub struct LruCache {
    state: Arc<Mutex<LruState>>,
    default_ttl: Duration,
    logger: Option<CsvLogger>,
}
```

`Entry` owns the cached value and its deadline. `LruState` owns the map and queue; map keys must appear once in `lru_order`. The deque front is LRU and its back is MRU. `LruCache` shares one `LruState` through `Arc<Mutex<_>>`.

## Actual constructors and clone implementation

```rust
impl LruState {
    fn new(max_entries: usize) -> Self {
        LruState {
            entries: HashMap::with_capacity(max_entries + 1),
            lru_order: VecDeque::with_capacity(max_entries + 1),
            max_entries,
        }
    }
}

impl Clone for LruCache {
    fn clone(&self) -> Self {
        LruCache {
            state: self.state.clone(),
            default_ttl: self.default_ttl,
            logger: self.logger.clone(),
        }
    }
}

impl LruCache {
    pub fn new(default_ttl: Duration, max_entries: usize, logger: Option<CsvLogger>) -> Self {
        LruCache {
            state: Arc::new(Mutex::new(LruState::new(max_entries))),
            default_ttl,
            logger,
        }
    }
}
```

`new` is a constructor by convention: it is an associated function that returns `Self`. `state.clone()` clones the `Arc`, not the map or deque, so every clone sees the same cache.

## Actual internal functions

```rust
fn get(&mut self, key: &str, now: Instant) -> Option<String> {
    match self.entries.get(key) {
        None => None,
        Some(entry) if entry.expires_at <= now => {
            self.entries.remove(key);
            if let Some(pos) = self.lru_order.iter().position(|k| k == key) {
                self.lru_order.remove(pos);
            }
            None
        }
        Some(entry) => {
            let value = entry.value.clone();
            if let Some(pos) = self.lru_order.iter().position(|k| k == key) {
                self.lru_order.remove(pos);
            }
            self.lru_order.push_back(key.to_string());
            Some(value)
        }
    }
}

fn put(&mut self, key: String, entry: Entry) {
    if self.entries.contains_key(&key) {
        if let Some(pos) = self.lru_order.iter().position(|k| k == &key) {
            self.lru_order.remove(pos);
        }
    }
    self.entries.insert(key.clone(), entry);
    self.lru_order.push_back(key);
    while self.entries.len() > self.max_entries {
        if let Some(lru_key) = self.lru_order.pop_front() {
            self.entries.remove(&lru_key);
        }
    }
}
```

`get` takes `&mut self` because a hit moves the key and an expired read deletes it. It clones the value because returning `&String` would keep a borrow into mutex-protected state after the lock is released. `put` clones `key` once so the map owns one copy and the deque owns the original. Both deque searches are O(n), by design.

## Exact code, before commentary

Read the complete implementation alongside this guide: [cache_lru/mod.rs](../crates/twin_ring_core/src/cache_lru/mod.rs). Do not skip the private `LruState` methods: they contain the actual map/deque invariant. The public `LruCache` methods are only the thread-safe wrapper.

## Goal

LRU is the control group. It answers: how well does a normal RAM cache with fixed expiry recover after the cache population is damaged? It has no lease, popularity tier, replica, or distributed coordination.

## Read the types first

`Entry` contains `value: String` and `expires_at: Instant`. `String` owns its bytes; the cache can outlive the request that inserted it. `Instant` is a monotonic local clock used only to compare deadlines.

`LruState` owns `entries: HashMap<String, Entry>` and `lru_order: VecDeque<String>`. The map gives fast lookup by key. The deque gives eviction order: front is least recently used; back is most recently used. Both store owned `String`s, so the state has no borrowed data. Its invariant is: every map key appears exactly once in the deque.

`LruCache` wraps `LruState` in `Arc<Mutex<LruState>>`. `Arc` means clones of `LruCache` point to the same allocation. `Mutex` permits exactly one thread at a time to mutate the map and deque together.

## Function-by-function walkthrough

`LruState::new` creates empty collections with capacity near `max_entries`. Capacity is an allocation hint, not a correctness rule.

`LruState::get(key, now)` first borrows the map entry. If absent it returns `None`. If `expires_at <= now`, it removes the entry and its deque key, then returns `None`. For a live entry it clones `entry.value`: the mutex-protected state keeps its owned value while the caller receives a separate owned `String`. It removes the old deque position and pushes `key.to_owned()` to the back. `to_owned` creates a `String` because the deque cannot store the borrowed `&str` received by the function.

`LruState::put(key, entry)` removes an old order record when replacing a key, inserts the new map entry, and pushes the key to MRU. While the map is over capacity, it pops the deque front and removes that map key.

`remove`, `live_len`, and `evict_expired` are maintenance operations. `evict_expired` collects expired keys before removing them because Rust does not allow mutating a map while iterating through it.

The public `LruCache::{get,put,delete,live_len,evict_expired}` methods lock the state, call the matching state method, and unlock when the temporary guard drops at the end of the method.

## Concurrency and request flow

There is no async mutex here because no method awaits. The lock lasts only for map/deque work. The HTTP node releases it before Cassandra is queried, so a slow database request cannot hold the cache lock. Concurrent readers serialize briefly because a read changes LRU order.

Example: insert `a`, insert `b`, read `a`, insert `c` at capacity two. The deque progresses `[a,b]`, `[b,a]`, then `[a,c]`; `b` is evicted. Reading `a` does not extend its fixed TTL.

## Tests to read

Read `reading_a_key_changes_eviction_order_but_not_its_expiration`, `capacity_evicts_the_oldest_unaccessed_key`, `fixed_ttl_expires_at_the_deadline_despite_reads`, and `mixed_operations_match_a_reference_model` in that order.
