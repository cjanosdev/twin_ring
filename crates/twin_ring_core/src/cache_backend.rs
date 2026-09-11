/// Trait that all cache backend implementations must satisfy.
///
/// The shared storage API used by every cache strategy.
///
/// `Send + Sync` is required so the backend can be wrapped in `Arc<dyn CacheBackend>`
/// and shared across actix worker threads via `web::Data<Arc<dyn CacheBackend>>`.
pub trait CacheBackend: Send + Sync {
    fn get(&self, key: &str) -> Option<String>;
    fn put(&self, key: String, value: String);
    fn delete(&self, key: &str) -> bool;
    fn live_len(&self) -> usize;
    fn evict_expired(&self);

    // TODO (future): per-key quorum guard for false-positive failover prevention
    // TODO (future): bloom filter interface for learned absence hints
}
