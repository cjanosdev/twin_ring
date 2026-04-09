use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use anyhow::Result;
use clap::Parser;
use scylla::{Session, SessionBuilder};
use serde_json::json;
use std::env;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use twin_ring_core::{Cache, CsvLogger};


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

    #[arg(long, default_value = "cache_metrics.csv")]
    log: String,
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
    /// Requests that missed the cache (key expired or not present)
    misses: AtomicU64,

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
            hits:            AtomicU64::new(0),
            misses:          AtomicU64::new(0),
            db_hits:         AtomicU64::new(0),
            db_not_found:    AtomicU64::new(0),
            db_errors:       AtomicU64::new(0),
            cache_latencies_us: Mutex::new(Vec::new()),
            db_latencies_us:    Mutex::new(Vec::new()),
        }
    }

    /// Record a cache HIT. Fast path — one atomic increment + one Vec push.
    fn record_hit(&self, latency_us: u128) {
        self.hits.fetch_add(1, Ordering::Relaxed);
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
        let hits         = self.hits.swap(0, Ordering::Relaxed);
        let misses       = self.misses.swap(0, Ordering::Relaxed);
        let db_hits      = self.db_hits.swap(0, Ordering::Relaxed);
        let db_not_found = self.db_not_found.swap(0, Ordering::Relaxed);
        let db_errors    = self.db_errors.swap(0, Ordering::Relaxed);

        // Drain the latency vecs — no copying, just a pointer swap
        let cache_lats = std::mem::take(&mut *self.cache_latencies_us.lock().unwrap());
        let db_lats    = std::mem::take(&mut *self.db_latencies_us.lock().unwrap());

        json!({
            "hits":         hits,
            "misses":       misses,
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
    // 1-second deadline: if Cassandra is too busy to respond in time,
    // count it as an error. This is what turns "slow Cassandra" into
    // visible db_errors — the key signal for the metastable feedback loop.
    // Without this timeout, a saturated Cassandra just queues requests
    // indefinitely and db_errors stays 0 even under severe overload.
    let result = tokio::time::timeout(
        Duration::from_millis(800),
        session.query("SELECT value FROM kvstore.kv WHERE key = ?", (key,)),
    )
    .await;

    match result {
        Err(_timeout)    => DbResult::Error, // took longer than 1s
        Ok(Err(_db_err)) => DbResult::Error, // Cassandra returned an error
        Ok(Ok(rows)) => match rows.maybe_first_row_typed::<(String,)>() {
            Ok(Some((value,))) => DbResult::Hit(value), // found it
            Ok(None)           => DbResult::Miss,       // replied but key not there
            Err(_)             => DbResult::Error,      // malformed response
        },
    }
}


// ============================================================
// HTTP handlers
// ============================================================

async fn hello() -> impl Responder {
    HttpResponse::Ok().body("TwinRing node alive!")
}

/// GET /stats
///
/// Returns a JSON snapshot of this window's metrics, then resets all
/// counters so the next poll gets a clean window.
///
/// Poll this every N seconds from an experiment binary to get a clean
/// time-series of cache and DB performance.
async fn stats(node_stats: web::Data<Arc<NodeStats>>) -> impl Responder {
    HttpResponse::Ok().json(node_stats.snapshot_and_reset())
}

async fn get(
    key: web::Path<String>,
    cache: web::Data<Cache>,
    session: web::Data<Arc<Session>>,
    node_stats: web::Data<Arc<NodeStats>>,
) -> impl Responder {
    let request_start = Instant::now();

    // Fast path: key is already in memory
    if let Some(val) = cache.get(&key) {
        node_stats.record_hit(request_start.elapsed().as_micros());
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

/// POST /put/{key}  (request body = value string)
async fn put(key: web::Path<String>, body: String, cache: web::Data<Cache>) -> impl Responder {
    cache.put(key.to_string(), body);
    HttpResponse::Ok().finish()
}

/// DELETE /delete/{key}
async fn delete(key: web::Path<String>, cache: web::Data<Cache>) -> impl Responder {
    if cache.delete(&key) {
        HttpResponse::Ok().finish()
    } else {
        HttpResponse::NotFound().finish()
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

    println!("🔌 Connecting to Cassandra at {}", known_node);
    let session: Arc<Session> = Arc::new(
        SessionBuilder::new()
            .known_node(&known_node)
            .build()
            .await?,
    );
    println!("✅ Cassandra connected");

    let logger     = CsvLogger::new(&args.log);
    let cache      = Cache::new(Duration::from_secs(args.ttl), Some(logger));
    let node_stats = Arc::new(NodeStats::new());

    println!("🚀 TwinRing node starting on port {} (TTL={}s)", args.port, args.ttl);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(cache.clone()))
            .app_data(web::Data::new(session.clone()))
            .app_data(web::Data::new(node_stats.clone()))
            .route("/",             web::get().to(hello))
            .route("/stats",        web::get().to(stats))
            .route("/get/{key}",    web::get().to(get))
            .route("/put/{key}",    web::post().to(put))
            .route("/delete/{key}", web::delete().to(delete))
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
