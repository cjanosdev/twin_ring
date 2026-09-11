use crate::cache_backend::CacheBackend;
use crate::metrics::CsvLogger;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A cached value with a fixed expiration deadline.
struct Entry {
    value: String,
    expires_at: Instant,
}

/// Stored entries, eviction order, and capacity limit.
///
/// Every key in `entries` appears exactly once in `lru_order`.
/// The front is least recently used; the back is most recently used.
/// Expired entries may remain stored until lookup or expiration cleanup.
struct LruState {
    entries: HashMap<String, Entry>,
    lru_order: VecDeque<String>,
    max_entries: usize,
}

impl LruState {
    fn new(max_entries: usize) -> Self {
        LruState {
            entries: HashMap::with_capacity(max_entries + 1),
            lru_order: VecDeque::with_capacity(max_entries + 1),
            max_entries,
        }
    }

    /// Look up a key. Returns the value if present and not expired.
    /// Moves the key to the MRU position (back of order).
    /// Removes and returns None if expired.
    fn get(&mut self, key: &str, now: Instant) -> Option<String> {
        match self.entries.get(key) {
            None => None,
            Some(entry) if entry.expires_at <= now => {
                // Expired — evict
                self.entries.remove(key);
                if let Some(pos) = self.lru_order.iter().position(|k| k == key) {
                    self.lru_order.remove(pos);
                }
                None
            }
            Some(entry) => {
                let value = entry.value.clone();
                // Refresh LRU order: move to back (MRU)
                if let Some(pos) = self.lru_order.iter().position(|k| k == key) {
                    self.lru_order.remove(pos);
                }
                self.lru_order.push_back(key.to_string());
                Some(value)
            }
        }
    }

    /// Insert or update a key. Evicts the LRU entry if over capacity.
    fn put(&mut self, key: String, entry: Entry) {
        // If key already exists, remove its old position in the order deque
        if self.entries.contains_key(&key) {
            if let Some(pos) = self.lru_order.iter().position(|k| k == &key) {
                self.lru_order.remove(pos);
            }
        }

        self.entries.insert(key.clone(), entry);
        self.lru_order.push_back(key);

        // Evict LRU entry if over capacity
        while self.entries.len() > self.max_entries {
            if let Some(lru_key) = self.lru_order.pop_front() {
                self.entries.remove(&lru_key);
            }
        }
    }

    /// Remove a key explicitly (e.g. DELETE /delete/{key}).
    fn remove(&mut self, key: &str) -> bool {
        let existed = self.entries.remove(key).is_some();
        if existed {
            if let Some(pos) = self.lru_order.iter().position(|k| k == key) {
                self.lru_order.remove(pos);
            }
        }
        existed
    }

    /// Count of entries that are currently live (not yet expired).
    fn live_len(&self, now: Instant) -> usize {
        self.entries.values().filter(|e| e.expires_at > now).count()
    }

    /// Evict all entries whose TTL has expired.
    /// Called periodically if desired; not required for correctness.
    fn evict_expired(&mut self, now: Instant) -> Vec<String> {
        let expired: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, e)| e.expires_at <= now)
            .map(|(k, _)| k.clone())
            .collect();
        for key in &expired {
            self.entries.remove(key);
            if let Some(pos) = self.lru_order.iter().position(|k| k == key) {
                self.lru_order.remove(pos);
            }
        }
        expired
    }
}

/// A thread-safe LRU cache with fixed TTL and optional operation logging.
///
/// Reads update recency without extending expiration. Clones share the same
/// state; the mutex protects entries and eviction order together.
pub struct LruCache {
    state: Arc<Mutex<LruState>>,
    default_ttl: Duration,
    logger: Option<CsvLogger>,
}

// Manual Clone impl — we clone the Arc (shared state), not the LruState itself.
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
    /// Create a new empty cache.
    ///
    /// `max_entries` limits stored keys, including expired entries awaiting removal.
    /// Inserting a new key beyond this limit evicts the least recently used key.
    /// Updating an existing key does not evict another key. Zero stores nothing.
    pub fn new(default_ttl: Duration, max_entries: usize, logger: Option<CsvLogger>) -> Self {
        LruCache {
            state: Arc::new(Mutex::new(LruState::new(max_entries))),
            default_ttl,
            logger,
        }
    }

    /// Get a value (if it exists and hasn't expired). Updates LRU order.
    pub fn get(&self, key: &str) -> Option<String> {
        let start = Instant::now();
        let result = self.state.lock().unwrap().get(key, Instant::now());

        if let Some(ref l) = self.logger {
            let hit = result.is_some();
            l.log("GET", key, hit, start.elapsed(), None);
        }
        result
    }

    /// Put a key/value with the default TTL. Evicts LRU entry if at capacity.
    pub fn put(&self, key: String, value: String) {
        let start = Instant::now();
        let entry = Entry {
            value,
            expires_at: Instant::now() + self.default_ttl,
        };
        self.state.lock().unwrap().put(key.clone(), entry);

        if let Some(ref l) = self.logger {
            l.log("PUT", &key, true, start.elapsed(), Some(self.default_ttl));
        }
    }

    /// Delete a key. Returns true if the key existed.
    pub fn delete(&self, key: &str) -> bool {
        let start = Instant::now();
        let removed = self.state.lock().unwrap().remove(key);

        if let Some(ref l) = self.logger {
            l.log("DELETE", key, removed, start.elapsed(), None);
        }
        removed
    }

    /// Count of live (non-expired) entries currently in the cache.
    pub fn live_len(&self) -> usize {
        self.state.lock().unwrap().live_len(Instant::now())
    }

    /// Evict all expired entries. Can be called periodically if desired.
    pub fn evict_expired(&self) {
        let expired = self.state.lock().unwrap().evict_expired(Instant::now());
        if let Some(ref l) = self.logger {
            for key in &expired {
                l.log("EXPIRE", key, false, Duration::ZERO, None);
            }
        }
    }
}

impl CacheBackend for LruCache {
    fn get(&self, key: &str) -> Option<String> {
        self.get(key)
    }
    fn put(&self, key: String, value: String) {
        self.put(key, value)
    }
    fn delete(&self, key: &str) -> bool {
        self.delete(key)
    }
    fn live_len(&self) -> usize {
        self.live_len()
    }
    fn evict_expired(&self) {
        self.evict_expired()
    }
}

#[cfg(test)]
mod lru_tests;
