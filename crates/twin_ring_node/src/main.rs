use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use anyhow::Result;
use clap::Parser;
use scylla::execution_profile::ExecutionProfile;
use scylla::{Session, SessionBuilder};
use serde_json::json;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use twin_ring_core::{
    CacheBackend, CombinedCache, CombinedLookup, CsvLogger, DualRing, DualRingCache, LeaseLookup,
    LeasedCache, LruCache, ReplicatePayload, ReplicateStats, TtlTieredCache,
};

// ============================================================
// CLI args
// ============================================================

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// How long (in seconds) a cached value stays valid before expiring
    #[arg(long, default_value_t = 10)]
    ttl: u64,

    /// Maximum number of entries the LRU cache will hold.
    /// Set to ~1/3 of the total keyspace so the cache cannot absorb both
    /// native keys AND foreign keys when a peer node goes down.
    #[arg(long, default_value_t = 35_000)]
    max_entries: usize,

    #[arg(long, default_value = "cache_metrics.csv")]
    log: String,

    /// Cache backend strategy: lru | dual-ring | ttl-tiered | leased
    #[arg(long, default_value = "lru")]
    cache_strategy: String,

    /// (dual-ring only) How often to replicate hot keys to backup nodes, in seconds
    #[arg(long, default_value_t = 5)]
    replicate_interval: u64,

    /// (dual-ring only) How many top hot keys to replicate per interval
    #[arg(long, default_value_t = 500)]
    hot_top_k: usize,
}

// ============================================================
// NodeStats — per-node metrics collected on every GET request.
//
// Rust concept: AtomicU64
//   A counter that many threads can increment simultaneously without
//   needing a Mutex. Each fetch_add() is a single CPU instruction.
//   Ordering::Relaxed means "I don't care about ordering relative
//   to other memory operations — I just need the count to be correct
//   eventually." This is the right choice for simple event counters.
//
// Rust concept: Mutex<Vec<u128>>
//   When you need to store multiple values (a growing list of latencies),
//   you need a Mutex to protect access. One thread locks it, pushes a
//   value, then unlocks — other threads wait their turn.
// ============================================================

pub struct NodeStats {
    // ---- Cache-level results ----
    /// Requests served entirely from in-memory cache (fast path)
    hits: AtomicU64,
    /// Hits served by the normal level-1 cache path.
    l1_hits: AtomicU64,
    /// Hits served from a level-2 replica through `/backup`.
    l2_hits: AtomicU64,
    /// Requests that missed the cache (key expired or not present)
    misses: AtomicU64,
    /// `/backup` requests whose level-2 replica was absent.
    backup_misses: AtomicU64,
    /// Cassandra calls caused by those backup misses.
    backup_db_calls: AtomicU64,

    // ---- DB-level sub-results (only recorded on a cache miss) ----
    /// Cassandra found the key and returned it
    db_hits: AtomicU64,
    /// Cassandra responded but the key genuinely wasn't there
    db_not_found: AtomicU64,
    /// Cassandra failed: timeout, connection refused, or overloaded.
    /// When this number spikes it means Cassandra is the bottleneck —
    /// this is the key signal for the metastable feedback loop.
    db_errors: AtomicU64,

    // ---- Latency samples ----
    /// Full request latency for every GET (µs). For hits this is ~sub-ms.
    /// For misses it includes the Cassandra round-trip.
    cache_latencies_us: Mutex<Vec<u128>>,
    /// How long the Cassandra call itself took (µs). Only recorded
    /// when we actually called the DB (i.e. on a cache miss).
    db_latencies_us: Mutex<Vec<u128>>,
}

impl NodeStats {
    fn new() -> Self {
        NodeStats {
            hits: AtomicU64::new(0),
            l1_hits: AtomicU64::new(0),
            l2_hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            backup_misses: AtomicU64::new(0),
            backup_db_calls: AtomicU64::new(0),
            db_hits: AtomicU64::new(0),
            db_not_found: AtomicU64::new(0),
            db_errors: AtomicU64::new(0),
            cache_latencies_us: Mutex::new(Vec::new()),
            db_latencies_us: Mutex::new(Vec::new()),
        }
    }

    /// Record a level-1 cache hit. Fast path — two atomic increments + one Vec push.
    fn record_l1_hit(&self, latency_us: u128) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.l1_hits.fetch_add(1, Ordering::Relaxed);
        self.cache_latencies_us.lock().unwrap().push(latency_us);
    }

    /// Record a level-2 replica hit served through `/backup`.
    fn record_l2_hit(&self, latency_us: u128) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.l2_hits.fetch_add(1, Ordering::Relaxed);
        self.cache_latencies_us.lock().unwrap().push(latency_us);
    }

    /// Record a cache MISS where Cassandra successfully returned the value.
    fn record_miss_db_hit(&self, total_us: u128, db_us: u128) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.db_hits.fetch_add(1, Ordering::Relaxed);
        self.cache_latencies_us.lock().unwrap().push(total_us);
        self.db_latencies_us.lock().unwrap().push(db_us);
    }

    /// Record a cache MISS where Cassandra replied "key not found."
    fn record_miss_db_not_found(&self, total_us: u128, db_us: u128) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.db_not_found.fetch_add(1, Ordering::Relaxed);
        self.cache_latencies_us.lock().unwrap().push(total_us);
        self.db_latencies_us.lock().unwrap().push(db_us);
    }

    /// Record a cache MISS where Cassandra failed (timeout, overload, etc.).
    fn record_miss_db_error(&self, total_us: u128, db_us: u128) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.db_errors.fetch_add(1, Ordering::Relaxed);
        self.cache_latencies_us.lock().unwrap().push(total_us);
        self.db_latencies_us.lock().unwrap().push(db_us);
    }

    /// Mark a `/backup` request that missed L2 before its Cassandra result is recorded.
    fn record_backup_miss(&self) {
        self.backup_misses.fetch_add(1, Ordering::Relaxed);
        self.backup_db_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a miss that was deliberately not sent to Cassandra because a
    /// different request is currently filling the same key.
    fn record_miss_lease_held(&self, total_us: u128) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.cache_latencies_us.lock().unwrap().push(total_us);
    }

    /// Build a JSON snapshot of the current window and reset everything to zero.
    ///
    /// Rust concept: std::mem::take()
    ///   Moves the Vec out of the Mutex, leaving an empty Vec behind.
    ///   This is more efficient than .clone() — it's a pointer swap, not a copy.
    ///
    /// Rust concept: .swap(0, Ordering::Relaxed)
    ///   Atomically reads the current value AND sets it to 0 in one step.
    ///   Returns the value that was there before the swap.
    fn snapshot_and_reset(&self) -> serde_json::Value {
        let hits = self.hits.swap(0, Ordering::Relaxed);
        let l1_hits = self.l1_hits.swap(0, Ordering::Relaxed);
        let l2_hits = self.l2_hits.swap(0, Ordering::Relaxed);
        let misses = self.misses.swap(0, Ordering::Relaxed);
        let backup_misses = self.backup_misses.swap(0, Ordering::Relaxed);
        let backup_db_calls = self.backup_db_calls.swap(0, Ordering::Relaxed);
        let db_hits = self.db_hits.swap(0, Ordering::Relaxed);
        let db_not_found = self.db_not_found.swap(0, Ordering::Relaxed);
        let db_errors = self.db_errors.swap(0, Ordering::Relaxed);

        // Drain the latency vecs — no copying, just a pointer swap
        let cache_lats = std::mem::take(&mut *self.cache_latencies_us.lock().unwrap());
        let db_lats = std::mem::take(&mut *self.db_latencies_us.lock().unwrap());

        json!({
            "hits":         hits,
            "l1_hits":      l1_hits,
            "l2_hits":      l2_hits,
            "misses":       misses,
            "backup_misses": backup_misses,
            "backup_db_calls": backup_db_calls,
            "db_hits":      db_hits,
            "db_not_found": db_not_found,
            "db_errors":    db_errors,
            "cache_p50_us": percentile(&cache_lats, 0.50),
            "cache_p99_us": percentile(&cache_lats, 0.99),
            "db_p50_us":    percentile(&db_lats, 0.50),
            "db_p99_us":    percentile(&db_lats, 0.99),
        })
    }
}

/// Compute a percentile over a slice of microsecond latency values.
/// p = 0.99 → p99, p = 0.50 → median (p50).
fn percentile(lats: &[u128], p: f64) -> u64 {
    if lats.is_empty() {
        return 0;
    }
    let mut sorted = lats.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() as f64 * p).floor() as usize).min(sorted.len() - 1);
    sorted[idx] as u64
}

// ============================================================
// Cassandra helper
//
// Rust concept: enum with data
//   Instead of Option<String> (which collapses "not found" and "error"
//   into the same None), we return a richer type with three outcomes.
//   This lets us count db_errors separately from db_not_found —
//   a critical distinction for diagnosing metastable failures.
// ============================================================

enum DbResult {
    Hit(String), // Cassandra found the key — value is inside
    Miss,        // Cassandra replied: key doesn't exist in the DB
    Error,       // Cassandra failed: timeout, overloaded, connection refused
}

async fn cassandra_get(session: &Session, key: &str) -> DbResult {
    // No application-level timeout — Cassandra's own read_request_timeout_in_ms
    // (configured in cassandra.yaml) governs when queries fail. Under overload,
    // Cassandra returns a ReadTimeoutException over the CQL wire, which the
    // scylla driver surfaces as QueryError::DbError → DbResult::Error below.
    // This is how real-world systems experience Cassandra overload.
    match session
        .query("SELECT value FROM kvstore.kv WHERE key = ?", (key,))
        .await
    {
        Err(_db_err) => DbResult::Error, // Cassandra timed out or returned an error
        Ok(rows) => match rows.maybe_first_row_typed::<(String,)>() {
            Ok(Some((value,))) => DbResult::Hit(value), // found it
            Ok(None) => DbResult::Miss,                 // replied but key not there
            Err(_) => DbResult::Error,                  // malformed response
        },
    }
}

// ============================================================
// HTTP handlers
// ============================================================

async fn hello() -> impl Responder {
    HttpResponse::Ok().body("TwinRing node alive!")
}

/// GET /strategy
///
/// Reports which cache backend this node is actually running.
///
/// Experiment binaries call this during preflight to verify the cluster matches
/// the strategy they were asked to measure. Without it, running `--strategy
/// dual-ring` against nodes started as `lru` silently produces LRU results
/// labeled "dual-ring".
///
/// Deliberately separate from `GET /stats`: that endpoint drains and resets the
/// counters, so a preflight check against it would discard the first window of
/// every run. This handler has no side effects.
async fn strategy(cache_strategy: web::Data<Arc<String>>) -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({ "strategy": cache_strategy.as_str() }))
}

/// GET /stats
///
/// Returns a JSON snapshot of this window's metrics, then resets all
/// counters so the next poll gets a clean window.
///
/// Poll this every N seconds from an experiment binary to get a clean
/// time-series of cache and DB performance.
async fn stats(
    node_stats: web::Data<Arc<NodeStats>>,
    cache: web::Data<Arc<dyn CacheBackend>>,
) -> impl Responder {
    let mut snap = node_stats.snapshot_and_reset();
    snap["live_entries"] = serde_json::json!(cache.live_len());
    HttpResponse::Ok().json(snap)
}

/// Serve a request through the explicit leased-cache state machine.
async fn get_leased(
    key: String,
    cache: &LeasedCache,
    session: &Session,
    node_stats: &NodeStats,
) -> HttpResponse {
    let request_start = Instant::now();

    match cache.lookup_or_acquire(&key) {
        LeaseLookup::Hit(value) => {
            node_stats.record_l1_hit(request_start.elapsed().as_micros());
            HttpResponse::Ok().body(value)
        }
        LeaseLookup::LeaseHeld => {
            node_stats.record_miss_lease_held(request_start.elapsed().as_micros());
            HttpResponse::ServiceUnavailable()
                .insert_header(("Retry-After", "0"))
                .body("cache fill in progress")
        }
        LeaseLookup::LeaseGranted(lease) => {
            let db_start = Instant::now();
            match cassandra_get(session, &key).await {
                DbResult::Hit(value) => {
                    node_stats.record_miss_db_hit(
                        request_start.elapsed().as_micros(),
                        db_start.elapsed().as_micros(),
                    );
                    if cache.complete_fill(&lease, value.clone()) {
                        HttpResponse::Ok().body(value)
                    } else {
                        HttpResponse::ServiceUnavailable().body("cache fill lease expired")
                    }
                }
                DbResult::Miss => {
                    cache.abandon_fill(&lease);
                    node_stats.record_miss_db_not_found(
                        request_start.elapsed().as_micros(),
                        db_start.elapsed().as_micros(),
                    );
                    HttpResponse::NotFound().finish()
                }
                DbResult::Error => {
                    cache.abandon_fill(&lease);
                    node_stats.record_miss_db_error(
                        request_start.elapsed().as_micros(),
                        db_start.elapsed().as_micros(),
                    );
                    HttpResponse::ServiceUnavailable().body("backend unavailable")
                }
            }
        }
    }
}

async fn get_combined(
    key: String,
    cache: &CombinedCache,
    session: &Session,
    node_stats: &NodeStats,
) -> HttpResponse {
    let request_start = Instant::now();
    match cache.lookup_or_acquire(&key) {
        CombinedLookup::Hit(value) => {
            node_stats.record_l1_hit(request_start.elapsed().as_micros());
            HttpResponse::Ok().body(value)
        }
        CombinedLookup::LeaseHeld => {
            node_stats.record_miss_lease_held(request_start.elapsed().as_micros());
            HttpResponse::ServiceUnavailable().body("cache fill in progress")
        }
        CombinedLookup::LeaseGranted(lease) => {
            let db_start = Instant::now();
            match cassandra_get(session, &key).await {
                DbResult::Hit(value) => {
                    node_stats.record_miss_db_hit(
                        request_start.elapsed().as_micros(),
                        db_start.elapsed().as_micros(),
                    );
                    if cache.complete_fill(&lease, value.clone()) {
                        HttpResponse::Ok().body(value)
                    } else {
                        HttpResponse::ServiceUnavailable().body("cache fill lease expired")
                    }
                }
                DbResult::Miss => {
                    cache.abandon_fill(&lease);
                    node_stats.record_miss_db_not_found(
                        request_start.elapsed().as_micros(),
                        db_start.elapsed().as_micros(),
                    );
                    HttpResponse::NotFound().finish()
                }
                DbResult::Error => {
                    cache.abandon_fill(&lease);
                    node_stats.record_miss_db_error(
                        request_start.elapsed().as_micros(),
                        db_start.elapsed().as_micros(),
                    );
                    HttpResponse::ServiceUnavailable().body("backend unavailable")
                }
            }
        }
    }
}

async fn get(
    key: web::Path<String>,
    cache: web::Data<Arc<dyn CacheBackend>>,
    leased_cache: web::Data<Option<Arc<LeasedCache>>>,
    combined_cache: web::Data<Option<Arc<CombinedCache>>>,
    session: web::Data<Arc<Session>>,
    node_stats: web::Data<Arc<NodeStats>>,
) -> impl Responder {
    if let Some(leased) = leased_cache.get_ref().as_ref() {
        return get_leased(
            key.into_inner(),
            leased,
            session.get_ref(),
            node_stats.get_ref(),
        )
        .await;
    }
    if let Some(combined) = combined_cache.get_ref().as_ref() {
        return get_combined(
            key.into_inner(),
            combined,
            session.get_ref(),
            node_stats.get_ref(),
        )
        .await;
    }

    let request_start = Instant::now();

    // Fast path: key is already in memory
    if let Some(val) = cache.get(&key) {
        node_stats.record_l1_hit(request_start.elapsed().as_micros());
        return HttpResponse::Ok().body(val);
    }

    let db_start = Instant::now();
    match cassandra_get(&session, &key).await {
        DbResult::Hit(val) => {
            // Found in Cassandra → populate cache so next request is a hit
            node_stats.record_miss_db_hit(
                request_start.elapsed().as_micros(),
                db_start.elapsed().as_micros(),
            );
            cache.put(key.to_string(), val.clone());
            HttpResponse::Ok().body(val)
        }
        DbResult::Miss => {
            // Key doesn't exist anywhere
            node_stats.record_miss_db_not_found(
                request_start.elapsed().as_micros(),
                db_start.elapsed().as_micros(),
            );
            HttpResponse::NotFound().finish()
        }
        DbResult::Error => {
            // Cassandra failed — 503 tells the client to retry or route elsewhere.
            // When db_errors spikes in /stats, the feedback loop is active.
            node_stats.record_miss_db_error(
                request_start.elapsed().as_micros(),
                db_start.elapsed().as_micros(),
            );
            HttpResponse::ServiceUnavailable().body("backend unavailable")
        }
    }
}

/// GET /backup/{key}
///
/// The experiment client calls this only after it cannot reach the key's L1
/// primary. Dual-ring checks its designated L2 replica store first; an L2 hit
/// promotes the requested value into L1. A replica miss follows the ordinary
/// read-through path to Cassandra.
async fn get_backup(
    key: web::Path<String>,
    cache: web::Data<Arc<dyn CacheBackend>>,
    dual_ring: web::Data<Option<Arc<DualRingCache>>>,
    combined: web::Data<Option<Arc<CombinedCache>>>,
    session: web::Data<Arc<Session>>,
    node_stats: web::Data<Arc<NodeStats>>,
) -> impl Responder {
    let request_start = Instant::now();
    if let Some(dual_ring) = dual_ring.get_ref().as_ref() {
        if let Some(value) = dual_ring.get_from_backup(&key) {
            node_stats.record_l2_hit(request_start.elapsed().as_micros());
            return HttpResponse::Ok().body(value);
        }
    } else if let Some(combined) = combined.get_ref().as_ref() {
        if let Some(value) = combined.get_from_backup(&key) {
            node_stats.record_l2_hit(request_start.elapsed().as_micros());
            return HttpResponse::Ok().body(value);
        }
    } else if let Some(value) = cache.get(&key) {
        // The same client fallback is used for every strategy. Backends without
        // an L2 store simply treat this as an ordinary cache lookup on a healthy
        // failover node before going to Cassandra.
        node_stats.record_l1_hit(request_start.elapsed().as_micros());
        return HttpResponse::Ok().body(value);
    }

    node_stats.record_backup_miss();
    let db_start = Instant::now();
    match cassandra_get(&session, &key).await {
        DbResult::Hit(value) => {
            node_stats.record_miss_db_hit(
                request_start.elapsed().as_micros(),
                db_start.elapsed().as_micros(),
            );
            cache.put(key.to_string(), value.clone());
            HttpResponse::Ok().body(value)
        }
        DbResult::Miss => {
            node_stats.record_miss_db_not_found(
                request_start.elapsed().as_micros(),
                db_start.elapsed().as_micros(),
            );
            HttpResponse::NotFound().finish()
        }
        DbResult::Error => {
            node_stats.record_miss_db_error(
                request_start.elapsed().as_micros(),
                db_start.elapsed().as_micros(),
            );
            HttpResponse::ServiceUnavailable().body("backend unavailable")
        }
    }
}

/// POST /put/{key}  (request body = value string)
async fn put(
    key: web::Path<String>,
    body: String,
    cache: web::Data<Arc<dyn CacheBackend>>,
) -> impl Responder {
    cache.put(key.to_string(), body);
    HttpResponse::Ok().finish()
}

/// DELETE /delete/{key}
async fn delete(key: web::Path<String>, cache: web::Data<Arc<dyn CacheBackend>>) -> impl Responder {
    if cache.delete(&key) {
        HttpResponse::Ok().finish()
    } else {
        HttpResponse::NotFound().finish()
    }
}

/// POST /replicate  (dual-ring and combined only)
///
/// Peer nodes POST their top-K hot keys here. Replaces the hot store for the
/// sender atomically. Returns 404 if neither dual-ring nor combined is active.
async fn replicate(
    body: web::Json<ReplicatePayload>,
    dual_ring: web::Data<Option<Arc<DualRingCache>>>,
    combined: web::Data<Option<Arc<CombinedCache>>>,
) -> impl Responder {
    let payload = body.into_inner();
    if let Some(cache) = dual_ring.as_ref() {
        cache.receive_replicate(payload);
        return HttpResponse::Ok().finish();
    }
    if let Some(cache) = combined.as_ref() {
        cache.receive_replicate(payload);
        return HttpResponse::Ok().finish();
    }
    HttpResponse::NotFound().body("replication not active for this strategy")
}

/// GET /replicate-stats  (dual-ring only)
///
/// Returns a JSON snapshot of the hot store and top hot keys.
/// Used by the experiment binary to verify replication arrived before fault injection.
/// Returns 404 for non-dual-ring strategies.
async fn replicate_stats_handler(
    dual_ring: web::Data<Option<Arc<DualRingCache>>>,
) -> impl Responder {
    if let Some(cache) = dual_ring.as_ref() {
        let stats: ReplicateStats = cache.replicate_stats();
        return HttpResponse::Ok().json(stats);
    }
    HttpResponse::NotFound().body("replicate-stats not available for this strategy")
}

/// GET /replicas/{primary}
///
/// Used only by the experiment controller during a planned failure
/// reconfiguration. It reads the in-memory stand-in for the backup SSD tier;
/// the controller copies these selected hot values to their new L1 owners.
async fn replicas_from_handler(
    primary: web::Path<usize>,
    dual_ring: web::Data<Option<Arc<DualRingCache>>>,
    combined: web::Data<Option<Arc<CombinedCache>>>,
) -> impl Responder {
    if let Some(cache) = dual_ring.get_ref().as_ref() {
        return HttpResponse::Ok().json(cache.replicas_from(*primary));
    }
    if let Some(cache) = combined.get_ref().as_ref() {
        return HttpResponse::Ok().json(cache.replicas_from(*primary));
    }
    HttpResponse::NotFound().body("replicas are only available for dual-ring")
}

/// POST /ring-members
///
/// Install a new, coordinated membership view on a healthy dual-ring node.
/// The experiment controller sends this before it begins routing workers with
/// the matching ring. This endpoint changes placement only; it does not erase
/// the node's L1 cache or its received L2 replicas.
async fn replace_ring_members(
    body: web::Json<serde_json::Value>,
    dual_ring: web::Data<Option<Arc<DualRingCache>>>,
    combined: web::Data<Option<Arc<CombinedCache>>>,
) -> impl Responder {
    let Some(members) = body["members"].as_array().and_then(|members| {
        members
            .iter()
            .map(|member| member.as_u64().and_then(|member| member.try_into().ok()))
            .collect::<Option<Vec<usize>>>()
    }) else {
        return HttpResponse::BadRequest().body("members must be an array of server indexes");
    };
    let Some(ring) = DualRing::new(members.clone()) else {
        return HttpResponse::BadRequest().body("a ring requires at least two distinct members");
    };

    let result = if let Some(cache) = dual_ring.get_ref().as_ref() {
        cache.replace_ring(ring)
    } else if let Some(cache) = combined.get_ref().as_ref() {
        cache.replace_ring(ring)
    } else {
        return HttpResponse::NotFound().body("ring reconfiguration is not available");
    };
    match result {
        Ok(()) => HttpResponse::Ok().json(json!({ "members": members })),
        Err(message) => HttpResponse::BadRequest().body(message),
    }
}

// ============================================================
// Entry point
// ============================================================

#[actix_web::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Read Cassandra connection details from environment variables.
    // docker-compose sets CASSANDRA_HOST and CASSANDRA_PORT automatically.
    // Defaults work for local development without Docker.
    let cassandra_host = env::var("CASSANDRA_HOST").unwrap_or_else(|_| "localhost".to_string());
    let cassandra_port = env::var("CASSANDRA_PORT").unwrap_or_else(|_| "9042".to_string());
    let known_node = format!("{}:{}", cassandra_host, cassandra_port);

    // Driver timeout set just above Cassandra's read_request_timeout (100ms in cassandra.yaml).
    // Under overload Cassandra fires ReadTimeoutException at 100ms — driver receives it and
    // returns Err → DbResult::Error. Setting the driver timeout to 120ms lets Cassandra's
    // server-side exception surface cleanly rather than the driver cutting the connection first.
    // This is the key signal for the metastable feedback loop: when db_errors spikes,
    // the cache cannot fill fast enough and the system stays stuck.
    let execution_profile = ExecutionProfile::builder()
        .request_timeout(Some(Duration::from_millis(120)))
        .build();

    println!("🔌 Connecting to Cassandra at {}", known_node);
    let session: Arc<Session> = Arc::new(
        SessionBuilder::new()
            .known_node(&known_node)
            .default_execution_profile_handle(execution_profile.into_handle())
            .build()
            .await?,
    );
    println!("✅ Cassandra connected");

    let logger = CsvLogger::new(&args.log);

    // Build the cache backend.
    // For dual-ring and combined we keep typed Arcs so the /replicate handler
    // can call receive_replicate() without downcasting through the trait object.
    // CACHE_STRATEGY env var overrides --cache-strategy CLI flag.
    // This lets docker-compose inject the strategy without editing the compose file.
    let cache_strategy = env::var("CACHE_STRATEGY").unwrap_or(args.cache_strategy);

    let dual_ring_cache: Option<Arc<DualRingCache>>;
    let combined_cache: Option<Arc<CombinedCache>>;
    let leased_cache: Option<Arc<LeasedCache>>;
    let cache: Arc<dyn CacheBackend> = match cache_strategy.as_str() {
        "lru" => {
            dual_ring_cache = None;
            combined_cache = None;
            leased_cache = None;
            Arc::new(LruCache::new(
                Duration::from_secs(args.ttl),
                args.max_entries,
                Some(logger),
            ))
        }
        "dual-ring" => {
            let server_index = env::var("NODE_INDEX")
                .expect("dual-ring requires NODE_INDEX")
                .parse()
                .expect("NODE_INDEX must be a zero-based integer");
            // `CLUSTER_MEMBERS` is the membership snapshot shared with clients
            // and peers, for example `0,2` after node 1 is removed. Keep the
            // older count setting as a default for the existing 3-node compose
            // experiment while it is migrated.
            let members: Vec<usize> = match env::var("CLUSTER_MEMBERS") {
                Ok(raw) => raw
                    .split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(|part| {
                        part.parse()
                            .expect("CLUSTER_MEMBERS must contain comma-separated integers")
                    })
                    .collect(),
                Err(_) => {
                    let server_count = env::var("CLUSTER_SIZE")
                        .unwrap_or_else(|_| "3".to_string())
                        .parse()
                        .expect("CLUSTER_SIZE must be an integer");
                    (0..server_count).collect()
                }
            };
            let ring = DualRing::new(members)
                .expect("dual-ring requires at least two distinct cache servers");
            let c = Arc::new(DualRingCache::new(
                Duration::from_secs(args.ttl),
                args.max_entries,
                args.hot_top_k,
                server_index,
                ring,
            ));
            c.spawn_replication_task(Duration::from_secs(args.replicate_interval));
            dual_ring_cache = Some(Arc::clone(&c));
            combined_cache = None;
            leased_cache = None;
            c
        }
        "ttl-tiered" => {
            dual_ring_cache = None;
            combined_cache = None;
            leased_cache = None;
            Arc::new(TtlTieredCache::new(
                Duration::from_secs(args.ttl),
                args.max_entries,
            ))
        }
        "leased" => {
            dual_ring_cache = None;
            combined_cache = None;
            let c = Arc::new(LeasedCache::new(
                Duration::from_secs(args.ttl),
                args.max_entries,
            ));
            leased_cache = Some(Arc::clone(&c));
            c
        }
        "combined" => {
            let server_index = env::var("NODE_INDEX").expect("combined requires NODE_INDEX").parse().expect("NODE_INDEX must be an integer");
            let server_count: usize = env::var("CLUSTER_SIZE").unwrap_or_else(|_| "3".into()).parse().expect("CLUSTER_SIZE must be an integer");
            let c = Arc::new(CombinedCache::new(
                Duration::from_secs(args.ttl),
                args.max_entries,
                args.hot_top_k,
                server_index,
                DualRing::new(0..server_count).expect("combined requires at least two servers"),
            ));
            c.spawn_replication_task(Duration::from_secs(args.replicate_interval));
            dual_ring_cache = None;
            combined_cache = Some(Arc::clone(&c));
            leased_cache = None;
            c
        }
        other => panic!(
            "Unknown cache strategy: {other}. Valid values: lru | dual-ring | ttl-tiered | leased | combined"
        ),
    };
    let node_stats = Arc::new(NodeStats::new());
    // No extra Arc here: `web::Data` is already an Arc internally, and actix
    // resolves app_data by exact type. Wrapping these in Arc registers
    // `web::Data<Arc<Option<..>>>`, which never matches the handlers'
    // `web::Data<Option<..>>` — the extractor then fails and every /replicate
    // request 500s.
    let dual_ring_data: Option<Arc<DualRingCache>> = dual_ring_cache;
    let combined_data: Option<Arc<CombinedCache>> = combined_cache;
    let leased_data: Option<Arc<LeasedCache>> = leased_cache;
    // Served by GET /strategy so experiments can verify the cluster matches what
    // they were asked to measure.
    let strategy_data: Arc<String> = Arc::new(cache_strategy.clone());

    println!(
        "🚀 TwinRing node starting on port {} (strategy={}, TTL={}s, max_entries={})",
        args.port, cache_strategy, args.ttl, args.max_entries
    );

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(Arc::clone(&cache)))
            .app_data(web::Data::new(session.clone()))
            .app_data(web::Data::new(node_stats.clone()))
            .app_data(web::Data::new(dual_ring_data.clone()))
            .app_data(web::Data::new(combined_data.clone()))
            .app_data(web::Data::new(leased_data.clone()))
            .app_data(web::Data::new(Arc::clone(&strategy_data)))
            .route("/", web::get().to(hello))
            .route("/strategy", web::get().to(strategy))
            .route("/stats", web::get().to(stats))
            .route("/get/{key}", web::get().to(get))
            .route("/backup/{key}", web::get().to(get_backup))
            .route("/put/{key}", web::post().to(put))
            .route("/delete/{key}", web::delete().to(delete))
            .route("/replicate", web::post().to(replicate))
            .route("/replicate-stats", web::get().to(replicate_stats_handler))
            .route("/replicas/{primary}", web::get().to(replicas_from_handler))
            .route("/ring-members", web::post().to(replace_ring_members))
    })
    .workers(num_cpus::get())
    .backlog(2048)
    .max_connections(100_000)
    .max_connection_rate(250_000)
    .keep_alive(actix_web::http::KeepAlive::Os)
    .bind(("0.0.0.0", args.port))?
    .run()
    .await?;

    Ok(())
}
