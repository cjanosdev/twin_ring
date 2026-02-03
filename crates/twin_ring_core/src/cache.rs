use crate::metrics::CsvLogger;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};


/// An entry stores the value + expiration
#[derive(Clone)]
pub struct Entry {
    pub value: String,
    pub expires_at: Instant,
}

/// Main cache struct
#[derive(Clone)]
pub struct Cache {
    inner: Arc<DashMap<String, Entry>>,
    default_ttl: Duration,
    logger: Option<CsvLogger>,
}


impl Cache {


    ///Create a new empty cache
    pub fn new(default_ttl: Duration, logger: Option<CsvLogger>) -> Self {
        Cache {
            inner: Arc::new(DashMap::new()),
            default_ttl,
            logger,
        }
    }


    /// Get a value (if it exists)
    pub fn get(&self, key: &str) -> Option<String> {
        let start = Instant::now();

        if let Some(entry_ref) = self.inner.get(key) {
            let entry = entry_ref.value();
            let now = Instant::now();

            if now < entry.expires_at {
                // Valid hit
                if let Some(ref l) = self.logger {
                    l.log("GET", key, true, start.elapsed(), Some(entry.expires_at - now));
                }
                return Some(entry.value.clone());
            } else {
                // Expired: remove it
                drop(entry_ref); // Drop guard before mutation
                self.inner.remove(key);

                if let Some(ref l) = self.logger {
                    l.log("EXPIRE", key, false, start.elapsed(), None);
                }
            }
        }

        // Miss
        if let Some(ref l) = self.logger {
            l.log("GET", key, false, start.elapsed(), None);
        }
        None
    }

     /// Put a key/value with default TTL
     pub fn put(&self, key: String, value: String) {
        let start = Instant::now();

        let entry = Entry {
            value,
            expires_at: Instant::now() + self.default_ttl,
        };

        self.inner.insert(key.clone(), entry);

        if let Some(ref l) = self.logger {
            l.log("PUT", &key, true, start.elapsed(), Some(self.default_ttl));
        }
    }

    /// Delete a key
    pub fn delete(&self, key: &str) -> bool {
        let start = Instant::now();

        let removed = self.inner.remove(key).is_some();

        if let Some(ref l) = self.logger {
            l.log("DELETE", key, removed, start.elapsed(), None);
        }
        removed
    }

    /// Manually evict expired entries (can be called periodically if desired)
    pub fn evict_expired(&self) {
        let now = Instant::now();

        // DashMap iterates shard by shard
        self.inner.retain(|k, entry| {
            let keep = entry.expires_at > now;

            if !keep {
                if let Some(ref l) = self.logger {
                    l.log("EXPIRE", k, false, Duration::ZERO, None);
                }
            }

            keep
        });
    }
}
