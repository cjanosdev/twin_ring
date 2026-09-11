/// Look-aside cache with per-key fill leases.
///
/// **Research question:** Does allowing only one cache fill per key reduce Cassandra
/// saturation during miss storms? The baseline `LruCache` fires an immediate Cassandra
/// query for every miss — 50 threads missing the same cold key = 50 concurrent DB reads.
///
/// **Mechanism:**
/// A cache lookup atomically returns a hit, a lease for this request to fill, or
/// `LeaseHeld` when another request owns that key's fill. Waiters never block or
/// query Cassandra. Leases expire so a failed filler cannot hold a key forever.
use crate::cache_backend::CacheBackend;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Result of checking a key in the leased cache.
///
/// A request receives exactly one of these outcomes. `LeaseGranted` means this
/// request may fetch the backing store. `LeaseHeld` means another request is
/// already responsible for that fill, so this request must not fetch it too.
#[derive(Debug, PartialEq, Eq)]
pub enum LeaseLookup {
    Hit(String),
    LeaseGranted(Lease),
    LeaseHeld,
}

/// Opaque proof that a request owns one particular lease.
///
/// The key and unique ID are kept private so only this module can decide
/// whether a later completion is allowed to clear the active lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    key: String,
    id: u64,
}

/// The outcome of asking the registry for a lease.
#[derive(Debug, PartialEq, Eq)]
enum LeaseAcquisition {
    Granted(Lease),
    Held,
}

/// Local, per-key lease state.
///
/// It stores deadlines rather than async locks. A caller never waits while this
/// registry is locked: it is either granted a lease immediately or learns that
/// another request already owns one.
struct LeaseRegistry {
    active: HashMap<String, ActiveLease>,
    lease_duration: Duration,
    next_id: u64,
}

struct ActiveLease {
    id: u64,
    expires_at: Instant,
}

impl LeaseRegistry {
    fn new(lease_duration: Duration) -> Self {
        Self {
            active: HashMap::new(),
            lease_duration,
            next_id: 0,
        }
    }

    /// Grant a lease if this key has no active, unexpired owner.
    fn acquire(&mut self, key: &str, now: Instant) -> LeaseAcquisition {
        if let Some(existing) = self.active.get(key) {
            if existing.expires_at > now {
                return LeaseAcquisition::Held;
            }
        }

        self.next_id = self.next_id.checked_add(1).expect("lease ID overflow");
        let lease = Lease {
            key: key.to_owned(),
            id: self.next_id,
        };
        self.active.insert(
            lease.key.clone(),
            ActiveLease {
                id: lease.id,
                expires_at: now + self.lease_duration,
            },
        );
        LeaseAcquisition::Granted(lease)
    }

    /// Clear a lease only if this exact lease still owns the key.
    fn release(&mut self, lease: &Lease) -> bool {
        let Some(active) = self.active.get(&lease.key) else {
            return false;
        };
        if active.id != lease.id {
            return false;
        }
        self.active.remove(&lease.key);
        true
    }

    /// Whether this exact lease is still the active, unexpired owner.
    fn is_current(&self, lease: &Lease, now: Instant) -> bool {
        self.active
            .get(&lease.key)
            .is_some_and(|active| active.id == lease.id && active.expires_at > now)
    }
}

// ── Inner cache (sync path — never held across .await) ─────────────────────────

struct Entry {
    value: String,
    expires_at: Instant,
}

struct LeasedInner {
    map: HashMap<String, Entry>,
    order: VecDeque<String>,
    max_entries: usize,
    default_ttl: Duration,
}

impl LeasedInner {
    fn new(max_entries: usize, default_ttl: Duration) -> Self {
        LeasedInner {
            map: HashMap::with_capacity(max_entries + 1),
            order: VecDeque::with_capacity(max_entries + 1),
            max_entries,
            default_ttl,
        }
    }

    fn get(&mut self, key: &str) -> Option<String> {
        self.get_at(key, Instant::now())
    }

    /// Look up using a caller-supplied clock, so lease state tests do not sleep.
    fn get_at(&mut self, key: &str, now: Instant) -> Option<String> {
        match self.map.get(key) {
            None => None,
            Some(e) if e.expires_at <= now => {
                self.map.remove(key);
                if let Some(pos) = self.order.iter().position(|k| k == key) {
                    self.order.remove(pos);
                }
                None
            }
            Some(e) => {
                let val = e.value.clone();
                if let Some(pos) = self.order.iter().position(|k| k == key) {
                    self.order.remove(pos);
                }
                self.order.push_back(key.to_string());
                Some(val)
            }
        }
    }

    fn put(&mut self, key: String, value: String) {
        self.put_at(key, value, Instant::now());
    }

    fn put_at(&mut self, key: String, value: String, now: Instant) {
        if self.map.contains_key(&key) {
            if let Some(pos) = self.order.iter().position(|k| k == &key) {
                self.order.remove(pos);
            }
        }
        self.map.insert(
            key.clone(),
            Entry {
                value,
                expires_at: now + self.default_ttl,
            },
        );
        self.order.push_back(key);
        while self.map.len() > self.max_entries {
            if let Some(lru) = self.order.pop_front() {
                self.map.remove(&lru);
            }
        }
    }

    fn delete(&mut self, key: &str) -> bool {
        if self.map.remove(key).is_some() {
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                self.order.remove(pos);
            }
            true
        } else {
            false
        }
    }

    fn live_len(&self) -> usize {
        let now = Instant::now();
        self.map.values().filter(|e| e.expires_at > now).count()
    }

    fn evict_expired(&mut self) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .map
            .iter()
            .filter(|(_, e)| e.expires_at <= now)
            .map(|(k, _)| k.clone())
            .collect();
        for key in &expired {
            self.map.remove(key);
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                self.order.remove(pos);
            }
        }
    }
}

/// Cache storage and local lease registry guarded together by one mutex.
///
/// The cache must inspect a key and grant a lease as one operation. If those
/// were protected separately, another request could intervene after the cache
/// miss and before the lease was recorded.
struct LeasedState {
    cache: LeasedInner,
    leases: LeaseRegistry,
}

impl LeasedState {
    fn new(max_entries: usize, cache_ttl: Duration, lease_duration: Duration) -> Self {
        Self {
            cache: LeasedInner::new(max_entries, cache_ttl),
            leases: LeaseRegistry::new(lease_duration),
        }
    }

    /// Atomically distinguish a cached value, a newly granted lease, and an
    /// already-held lease for this key.
    fn lookup_or_acquire(&mut self, key: &str, now: Instant) -> LeaseLookup {
        if let Some(value) = self.cache.get_at(key, now) {
            return LeaseLookup::Hit(value);
        }

        match self.leases.acquire(key, now) {
            LeaseAcquisition::Granted(lease) => LeaseLookup::LeaseGranted(lease),
            LeaseAcquisition::Held => LeaseLookup::LeaseHeld,
        }
    }

    /// Store a fetched value only if the caller still owns an active lease.
    ///
    /// Both the insertion and lease removal happen while this state is locked,
    /// so a later lookup sees either the old lease or the completed cache entry,
    /// never a gap between them.
    fn complete_fill(&mut self, lease: &Lease, value: String, now: Instant) -> bool {
        if !self.leases.is_current(lease, now) {
            return false;
        }

        self.cache.put_at(lease.key.clone(), value, now);
        let released = self.leases.release(lease);
        debug_assert!(released, "current lease must be releasable");
        true
    }

    /// Release a failed fill so a later request may try again immediately.
    ///
    /// Unlike completion, this does not require the lease to be unexpired: an
    /// old token cannot remove a replacement lease because `release` compares
    /// the unique lease ID as well as the key.
    fn abandon_fill(&mut self, lease: &Lease) -> bool {
        self.leases.release(lease)
    }
}

/// Initial production lease duration. It exceeds the 120 ms Cassandra driver
/// timeout so normal timeout/error handling can finish before another request
/// is allowed to attempt the same fill.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_millis(500);

pub struct LeasedCache {
    state: Arc<Mutex<LeasedState>>,
}

impl LeasedCache {
    pub fn new(default_ttl: Duration, max_entries: usize) -> Self {
        Self::with_lease_duration(default_ttl, max_entries, DEFAULT_LEASE_DURATION)
    }

    pub fn with_lease_duration(
        default_ttl: Duration,
        max_entries: usize,
        lease_duration: Duration,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(LeasedState::new(
                max_entries,
                default_ttl,
                lease_duration,
            ))),
        }
    }

    /// Return the three possible outcomes of one atomic cache-and-lease check.
    pub fn lookup_or_acquire(&self, key: &str) -> LeaseLookup {
        self.state
            .lock()
            .unwrap()
            .lookup_or_acquire(key, Instant::now())
    }

    /// Finish a successful backing-store fetch for this lease.
    pub fn complete_fill(&self, lease: &Lease, value: String) -> bool {
        self.state
            .lock()
            .unwrap()
            .complete_fill(lease, value, Instant::now())
    }

    /// Stop owning a lease after a backing-store miss or error.
    pub fn abandon_fill(&self, lease: &Lease) -> bool {
        self.state.lock().unwrap().abandon_fill(lease)
    }
}

impl Clone for LeasedCache {
    fn clone(&self) -> Self {
        LeasedCache {
            state: Arc::clone(&self.state),
        }
    }
}

impl CacheBackend for LeasedCache {
    fn get(&self, key: &str) -> Option<String> {
        self.state.lock().unwrap().cache.get(key)
    }

    fn put(&self, key: String, value: String) {
        self.state.lock().unwrap().cache.put(key, value);
    }

    fn delete(&self, key: &str) -> bool {
        self.state.lock().unwrap().cache.delete(key)
    }

    fn live_len(&self) -> usize {
        self.state.lock().unwrap().cache.live_len()
    }

    fn evict_expired(&self) {
        self.state.lock().unwrap().cache.evict_expired();
    }
}

#[cfg(test)]
mod leased_tests;
