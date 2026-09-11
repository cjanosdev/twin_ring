//! No-cache baseline: ramp Cassandra concurrency until db_errors appear.
//!
//! # Purpose
//!
//! Measures the raw Cassandra saturation point — the ops/sec at which the DB
//! starts returning errors with no cache in front of it. This is the reference
//! that makes the metastable experiment compelling:
//!
//!   - baseline_no_cache: Cassandra saturates at X ops/sec
//!   - baseline (cached):  system handles Y total RPS, Cassandra only sees ~0.1X
//!   - simple_metastable:  killing a cache node pushes Cassandra past X → failure
//!
//! # How to run
//!
//!   docker compose -f docker/docker-compose-baseline.yml up -d
//!   cargo run -p twin_ring_exp --bin baseline_no_cache
//!
//! Output:
//!   experiment_results/runs/<date>/baseline_no_cache_HHMMSS.csv
//!   experiment_results/baseline_no_cache.json

use anyhow::Result;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Zipf};
use scylla::execution_profile::ExecutionProfile;
use scylla::{Session, SessionBuilder};
use std::env;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use twin_ring_core::experiment_path::results_path;


// ============================================================
// Configuration
// ============================================================

const KEY_SPACE: u32 = 100_000;

/// Concurrency steps — doubles each time to find the cliff efficiently.
const STEPS: &[usize] = &[10, 20, 40, 80, 160, 320, 640];

/// How long to run at each concurrency level before measuring.
/// 15s gives Cassandra time to reach steady state at each level.
const STEP_SECS: u64 = 15;

/// Client-side driver timeout — same as the cache node (apples-to-apples).
const DRIVER_TIMEOUT_MS: u64 = 300;

const BASELINE_NO_CACHE_JSON: &str = "experiment_results/baseline_no_cache.json";


// ============================================================
// Per-step statistics
// ============================================================

struct StepStats {
    ops:    AtomicU64,
    errors: AtomicU64,
    latencies_us: Mutex<Vec<u128>>,
}

impl StepStats {
    fn new() -> Self {
        StepStats {
            ops:          AtomicU64::new(0),
            errors:       AtomicU64::new(0),
            latencies_us: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, latency_us: u128, is_error: bool) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        if is_error {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        self.latencies_us.lock().unwrap().push(latency_us);
    }

    /// Drain and return (ops, errors, p50_us, p99_us).
    fn snapshot_and_reset(&self) -> (u64, u64, u64, u64) {
        let ops    = self.ops.swap(0, Ordering::Relaxed);
        let errors = self.errors.swap(0, Ordering::Relaxed);
        let mut lats = std::mem::take(&mut *self.latencies_us.lock().unwrap());
        lats.sort_unstable();
        let p50 = percentile(&lats, 0.50);
        let p99 = percentile(&lats, 0.99);
        (ops, errors, p50, p99)
    }
}

fn percentile(sorted: &[u128], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((sorted.len() as f64 * p).floor() as usize).min(sorted.len() - 1);
    sorted[idx] as u64
}


// ============================================================
// Worker
// ============================================================

fn spawn_worker(
    worker_id: usize,
    session: Arc<Session>,
    active_ceiling: Arc<AtomicUsize>,
    stats: Arc<StepStats>,
) {
    tokio::spawn(async move {
        let mut rng = ChaCha8Rng::seed_from_u64(worker_id as u64);
        let zipf = Zipf::new(KEY_SPACE as f64, 0.9).unwrap();
        loop {
            if worker_id >= active_ceiling.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            let k = (zipf.sample(&mut rng) as u32).saturating_sub(1);
            let key = format!("key{}", k);
            let t = Instant::now();
            let is_error = session
                .query("SELECT value FROM kvstore.kv WHERE key = ?", (&key,))
                .await
                .is_err();
            let latency_us = t.elapsed().as_micros();
            stats.record(latency_us, is_error);
            tokio::task::yield_now().await;
        }
    });
}


// ============================================================
// Main
// ============================================================

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    std::fs::create_dir_all("experiment_results")?;

    let cassandra_host = env::var("CASSANDRA_HOST").unwrap_or_else(|_| "localhost".to_string());
    let cassandra_port = env::var("CASSANDRA_PORT").unwrap_or_else(|_| "9042".to_string());
    let known_node = format!("{}:{}", cassandra_host, cassandra_port);

    let execution_profile = ExecutionProfile::builder()
        .request_timeout(Some(Duration::from_millis(DRIVER_TIMEOUT_MS)))
        .build();

    println!("Connecting to Cassandra at {}...", known_node);
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(&known_node)
            .default_execution_profile_handle(execution_profile.into_handle())
            .build()
            .await?,
    );
    println!("Connected.\n");

    let out_path = results_path("baseline_no_cache")?;
    let mut csv_writer = csv::Writer::from_path(&out_path)?;
    csv_writer.write_record(["step", "workers", "ops_per_sec", "db_p50_us", "db_p99_us", "db_errors"])?;
    csv_writer.flush()?;
    println!("CSV -> {}\n", out_path.display());

    let stats = Arc::new(StepStats::new());
    let active_ceiling = Arc::new(AtomicUsize::new(0));

    // Pre-spawn the maximum number of workers — they sleep until ceiling rises.
    let max_workers = *STEPS.last().unwrap();
    for worker_id in 0..max_workers {
        spawn_worker(worker_id, session.clone(), active_ceiling.clone(), stats.clone());
    }

    println!(
        "{:<6}  {:<10}  {:<12}  {:<12}  {:<12}  {:<10}",
        "step", "workers", "ops/sec", "db_p50_us", "db_p99_us", "db_errors"
    );
    println!("{}", "-".repeat(68));

    let mut last_clean: Option<(usize, f64, u64)> = None; // (workers, ops/s, p50)
    let mut saturation: Option<(usize, f64)> = None;      // (workers, ops/s)

    for (step_idx, &workers) in STEPS.iter().enumerate() {
        // Warm up new workers before the measurement window.
        active_ceiling.store(workers, Ordering::Relaxed);
        // Discard any stats accumulated during the ramp-up.
        tokio::time::sleep(Duration::from_secs(2)).await;
        stats.snapshot_and_reset();

        // Measure for STEP_SECS.
        tokio::time::sleep(Duration::from_secs(STEP_SECS)).await;
        let (ops, errors, p50, p99) = stats.snapshot_and_reset();
        let ops_per_sec = ops as f64 / STEP_SECS as f64;

        println!(
            "{:<6}  {:<10}  {:<12.0}  {:<12}  {:<12}  {:<10}",
            step_idx + 1, workers, ops_per_sec, p50, p99, errors
        );

        csv_writer.write_record(&[
            (step_idx + 1).to_string(),
            workers.to_string(),
            format!("{:.1}", ops_per_sec),
            p50.to_string(),
            p99.to_string(),
            errors.to_string(),
        ])?;
        csv_writer.flush()?;

        if errors > 0 {
            saturation = Some((workers, ops_per_sec));
            println!("\n  db_errors appeared — Cassandra saturated at step {}.", step_idx + 1);
            break;
        }

        last_clean = Some((workers, ops_per_sec, p50));
    }

    // ── Summary ────────────────────────────────────────────────────────────────
    println!();
    match (saturation, last_clean) {
        (Some((sat_workers, sat_ops)), Some((clean_workers, clean_ops, clean_p50))) => {
            println!("Saturation point:");
            println!("   workers={sat_workers}  ops/sec={sat_ops:.0}  <- db_errors first appeared");
            println!("Last clean step:");
            println!("   workers={clean_workers}  ops/sec={clean_ops:.0}  db_p50={clean_p50}µs");

            let json = serde_json::json!({
                "saturation_workers":     sat_workers,
                "saturation_ops_per_sec": sat_ops,
                "last_clean_workers":     clean_workers,
                "last_clean_ops_per_sec": clean_ops,
                "last_clean_db_p50_us":   clean_p50,
            });
            std::fs::write(BASELINE_NO_CACHE_JSON, serde_json::to_string_pretty(&json)?)?;
            println!("\nSaved -> {BASELINE_NO_CACHE_JSON}");
        }
        (None, Some((clean_workers, clean_ops, clean_p50))) => {
            println!("No saturation observed up to {} workers.", STEPS.last().unwrap());
            println!("Last step: workers={clean_workers}  ops/sec={clean_ops:.0}  db_p50={clean_p50}µs");
            println!("Consider adding more steps to STEPS[] in baseline_no_cache.rs.");
        }
        _ => {
            println!("No data collected — did the binary exit before any step completed?");
        }
    }

    Ok(())
}
