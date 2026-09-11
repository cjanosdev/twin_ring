//! Popularity-based TTL classes in one capacity-limited LRU store.
//!
//! Cold: 0–1 hits, lifetime T. Warm: 2–7 hits, lifetime 2T.
//! Hot: 8+ hits, lifetime 4T. Every deadline is measured from the last fill,
//! not the last read. These are initial experimental parameters, not a claim
//! that this policy improves recovery. All classes share one eviction order.

use crate::cache_backend::CacheBackend;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const WARM_HITS: u32 = 2;
const HOT_HITS: u32 = 8;

/// One value and the history of its current fill.
struct TieredEntry {
    value: String,
    filled_at: Instant,
    expires_at: Instant,
    hit_count: u32,
}

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

/// One shared capacity budget and least-to-most recently used ordering.
/// Every stored key appears exactly once in the queue. Expired entries count
/// toward capacity until lookup, eviction, deletion, or cleanup removes them.
struct TtlTieredState {
    entries: HashMap<String, TieredEntry>,
    lru_order: VecDeque<String>,
    max_entries: usize,
    base_ttl: Duration,
}

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

/// Thread-safe TTL-tiered cache. Clones share state; it contains no DB client.
#[derive(Clone)]
pub struct TtlTieredCache {
    state: Arc<Mutex<TtlTieredState>>,
}

impl TtlTieredCache {
    pub fn new(default_ttl: Duration, max_entries: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(TtlTieredState::new(max_entries, default_ttl))),
        }
    }
}

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

#[cfg(test)]
mod ttl_tiered_tests;
