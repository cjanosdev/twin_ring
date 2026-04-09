//! Baseline measurement for the TwinRing cache system.
//!
//! # Purpose
//!
//! Measures `cache_p99` latency under healthy conditions (warm caches, full load,
//! no faults). The result is saved to `experiment_results/baseline.json` and used
//! by `simple_metastable` as the reference point for the 20x latency-spike criterion.
//!
//! # How to run
//!
//!   docker compose -f docker/docker-compose-baseline.yml down -v
//!   docker compose -f docker/docker-compose-baseline.yml up --build
//!   cargo run -p twin_ring_exp --bin baseline
//!
//! Output: experiment_results/baseline.json
//!   { "cache_p99_us": N, "hit_rate": X, "throughput_rps": Y }

use anyhow::Result;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use twin_ring_exp::metrics::{MetricsWriter, NodeWindow, StatsPoller, run_phase};
use twin_ring_core::experiment_path::results_path;


// ============================================================
// Configuration — must match simple_metastable
// ============================================================

const NODES: &[&str] = &[
    "http://localhost:8001",  // owns shard 0
    "http://localhost:8002",  // owns shard 1
    "http://localhost:8003",  // owns shard 2
];

const KEY_SPACE: u32   = 100_000;
const NUM_WORKERS: usize = 150;

/// Gradual warmup ramp: start with few workers so Cassandra is not saturated
/// during the initial cache fill. Hot Zipfian keys fill quickly at low concurrency.
const WARMUP_SECS:           u64   = 120;  // 90s ramp + 30s buffer at full load
const WARMUP_START_WORKERS:  usize = 15;   // 5/shard — low Cassandra pressure
const WARMUP_RAMP_STEP:      usize = 15;   // add 15 workers per step
const WARMUP_RAMP_STEP_SECS: u64   = 15;   // one step every 15s → full 150 at t=90s

/// Steady-state measurement window — caches fully warm, measure cache_p99.
const STEADY_SECS:        u64 = 60;
const POLL_INTERVAL_SECS: u64 = 5;

/// Reqwest timeout — must exceed Cassandra query timeout (800ms).
const REQUEST_TIMEOUT_MS: u64 = 4000;

/// Fixed output path — simple_metastable reads from here.
const BASELINE_JSON: &str = "experiment_results/baseline.json";


// ============================================================
// Shard-based routing — must match simple_metastable
// ============================================================

/// Each worker's home shard is interleaved: worker 0→shard0, 1→shard1, 2→shard2, 3→shard0...
/// This ensures all 3 nodes warm evenly from the first ramp step.
fn home_shard(worker_id: usize) -> usize {
    worker_id % NODES.len()
}

/// Key range for a given shard.
fn shard_range(shard: usize) -> (u32, u32) {
    let size = KEY_SPACE / NODES.len() as u32;
    let start = shard as u32 * size;
    let end = if shard == NODES.len() - 1 {
        KEY_SPACE
    } else {
        start + size
    };
    (start, end)
}

/// Zipfian key selection within a shard: 80% of requests hit hottest 20% of keys.
fn zipf_key(shard: usize, rng: &mut ChaCha8Rng) -> u32 {
    let (start, end) = shard_range(shard);
    let hot_end = start + (end - start) / 5;
    if rng.random::<f64>() < 0.8 {
        rng.random_range(start..hot_end.max(start + 1))
    } else {
        rng.random_range(hot_end..end)
    }
}


// ============================================================
// Worker
// ============================================================

fn spawn_worker(
    worker_id: usize,
    client: Arc<Client>,
    nodes: Vec<String>,
    active_ceiling: Arc<AtomicUsize>,
) {
    tokio::spawn(async move {
        let mut rng = ChaCha8Rng::seed_from_u64(worker_id as u64);
        let shard = home_shard(worker_id);
        loop {
            if worker_id >= active_ceiling.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            let k = zipf_key(shard, &mut rng);
            for attempt in 0..NODES.len() {
                let node_idx = (shard + attempt) % NODES.len();
                let url = format!("{}/get/key{}", nodes[node_idx], k);
                match client.get(&url).send().await {
                    Ok(_)  => break,
                    Err(_) => {}
                }
            }
            tokio::task::yield_now().await;
        }
    });
}


// ============================================================
// Main
// ============================================================

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Ensure output directory exists
    std::fs::create_dir_all("experiment_results")?;

    let client = Arc::new(
        Client::builder()
            .pool_max_idle_per_host(200)
            .timeout(Duration::from_millis(REQUEST_TIMEOUT_MS))
            .build()?,
    );

    let nodes: Vec<String> = NODES.iter().map(|s| s.to_string()).collect();

    let (phase_tx, phase_rx) = watch::channel("warmup".to_string());
    let (window_tx, mut window_rx) = mpsc::channel::<NodeWindow>(256);

    {
        let poller = StatsPoller::new(nodes.clone(), POLL_INTERVAL_SECS);
        tokio::spawn(async move {
            let _ = poller.run(phase_rx, window_tx).await;
        });
    }

    // We still need a MetricsWriter even though we discard the warmup CSV.
    // Use a timestamped path for the warmup CSV (archive it, don't overwrite).
    let warmup_csv_path = results_path("baseline_warmup")?;
    let mut csv = MetricsWriter::new(&warmup_csv_path)?;

    // ── Warmup: gradual ramp ─────────────────────────────────────────────────
    println!("\n⏳ [warmup] {WARMUP_SECS}s — ramping {WARMUP_START_WORKERS} → {NUM_WORKERS} workers...");
    println!("   One step of {WARMUP_RAMP_STEP} workers every {WARMUP_RAMP_STEP_SECS}s.");
    println!("   Expect: hit_rate → ~95%+ by end of warmup.");

    let active_ceiling = Arc::new(AtomicUsize::new(WARMUP_START_WORKERS));

    for worker_id in 0..NUM_WORKERS {
        spawn_worker(worker_id, client.clone(), nodes.clone(), active_ceiling.clone());
    }

    {
        let ceiling = active_ceiling.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(WARMUP_RAMP_STEP_SECS)).await;
                let prev = ceiling.load(Ordering::Relaxed);
                if prev >= NUM_WORKERS { break; }
                let next = (prev + WARMUP_RAMP_STEP).min(NUM_WORKERS);
                ceiling.store(next, Ordering::Relaxed);
                println!("   [warmup ramp] active workers -> {next}");
            }
        });
    }

    run_phase(WARMUP_SECS, &mut window_rx, &mut csv, |w| {
        println!(
            "  [warmup] {:<28} | hit={:>5.1}%  rps={:>5.0}  cache_p99={:>8}µs",
            w.node, w.hit_rate * 100.0, w.throughput_rps, w.cache_p99_us,
        );
    }).await?;

    // ── Steady-state: measure baseline cache_p99 ────────────────────────────
    phase_tx.send("steady".to_string()).ok();
    println!("\n📏 [steady] {STEADY_SECS}s — measuring cache_p99 at full load...");

    let steady_csv_path = results_path("baseline_steady")?;
    let mut steady_csv = MetricsWriter::new(&steady_csv_path)?;

    let mut steady_samples: Vec<NodeWindow> = Vec::new();

    run_phase(STEADY_SECS, &mut window_rx, &mut steady_csv, |w| {
        println!(
            "  [steady]  {:<28} | hit={:>5.1}%  rps={:>5.0}  cache_p99={:>8}µs",
            w.node, w.hit_rate * 100.0, w.throughput_rps, w.cache_p99_us,
        );
        steady_samples.push(w.clone());
    }).await?;

    // Compute averages across all steady-state windows
    let n = steady_samples.len() as f64;
    let (avg_cache_p99, avg_hit_rate, avg_rps) = if steady_samples.is_empty() {
        (0.0, 0.0, 0.0)
    } else {
        (
            steady_samples.iter().map(|w| w.cache_p99_us as f64).sum::<f64>() / n,
            steady_samples.iter().map(|w| w.hit_rate).sum::<f64>() / n,
            steady_samples.iter().map(|w| w.throughput_rps).sum::<f64>() / n,
        )
    };

    println!("\n✅ Baseline measurement complete.");
    println!("   cache_p99 = {:.0}µs", avg_cache_p99);
    println!("   hit_rate  = {:.1}%",  avg_hit_rate * 100.0);
    println!("   rps       = {:.0}",   avg_rps);

    // ── Write baseline.json ──────────────────────────────────────────────────
    let json = serde_json::json!({
        "cache_p99_us":    avg_cache_p99,
        "hit_rate":        avg_hit_rate,
        "throughput_rps":  avg_rps,
    });
    std::fs::write(BASELINE_JSON, serde_json::to_string_pretty(&json)?)?;
    println!("   Saved → {BASELINE_JSON}");

    Ok(())
}
