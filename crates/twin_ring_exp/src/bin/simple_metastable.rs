//! Simple metastable failure experiment.
//!
//! # What this proves
//!
//! Metastable failure: a system enters a degraded state and cannot self-recover
//! even after the original fault is resolved.
//!
//! # The scenario
//!
//! Key-based routing with 6-slot split failover.
//!
//! Each key maps to one of 6 virtual slots via `key % 6`. The slot determines
//! which node is primary and which nodes are tried on failover:
//!
//!   slot 0 (key%6==0): node1 → node2 → node3
//!   slot 1 (key%6==1): node2 → node3 → node1
//!   slot 2 (key%6==2): node3 → node1 → node2
//!   slot 3 (key%6==3): node1 → node3 → node2
//!   slot 4 (key%6==4): node2 → node1 → node3
//!   slot 5 (key%6==5): node3 → node2 → node1
//!
//! Node1 owns slots 0 and 3 (1/3 of keys). Node2 owns slots 1 and 4. Node3
//! owns slots 2 and 5. All workers request keys from the full keyspace — the
//! key itself determines routing, not the worker.
//!
//! WARMUP: workers ramp from 15 → 150 gradually. Each node warms its 1/3 of
//! the keyspace. Hit rate climbs to ~95%+ on all three nodes.
//!
//! FAULT INJECT: node 1 is killed. Slot-0 keys fail over to node2; slot-3
//! keys fail over to node3. Both nodes simultaneously absorb foreign keys they
//! have never cached → 100% miss on those keys → Cassandra hammered from both
//! sides. Load surges to fault_workers. Node 1 stays down for the full
//! fault_down_secs so nodes 2 and 3 lose their own cached keys via TTL expiry.
//!
//! OBSERVE: node 1 restarts with a cold cache. Cassandra is already saturated,
//! so node 1 cannot fill its cache fast enough. All three nodes see high
//! cache_p99 (miss → Cassandra timeout) — the system cannot self-recover.
//!
//! # Metastable criterion
//!
//! cache_p99 spikes ≥ 20× the baseline (from baseline.rs) AND hit_rate drops
//! ≥ 30pp below baseline. Run baseline.rs first to generate experiment_results/baseline.json.
//!
//! # How to run
//!
//!   docker compose -f docker/docker-compose-baseline.yml down -v
//!   docker compose -f docker/docker-compose-baseline.yml up --build
//!   cargo run -p twin_ring_exp --bin baseline
//!   cargo run -p twin_ring_exp --bin simple_metastable
//!
//! Results → experiment_results/runs/<date>/simple_metastable_HHMMSS.csv

use anyhow::Result;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand::SeedableRng;
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use twin_ring_core::experiment_path::results_path;
use twin_ring_exp::metrics::{MetricsWriter, NodeWindow, StatsPoller, run_phase};


// ============================================================
// Configuration
// ============================================================

const NODES: &[&str] = &[
    "http://localhost:8001",  // primary for key%6 ∈ {0, 3}
    "http://localhost:8002",  // primary for key%6 ∈ {1, 4}
    "http://localhost:8003",  // primary for key%6 ∈ {2, 5}
];

/// Failover order for each virtual slot (key % 6).
/// Slot 0 and 3 both have node1 as primary but different failover targets,
/// so when node1 is down its traffic splits evenly between node2 and node3.
const FAILOVER_ORDER: [[usize; 3]; 6] = [
    [0, 1, 2], // slot 0: node1 → node2 → node3
    [1, 2, 0], // slot 1: node2 → node3 → node1
    [2, 0, 1], // slot 2: node3 → node1 → node2
    [0, 2, 1], // slot 3: node1 → node3 → node2
    [1, 0, 2], // slot 4: node2 → node1 → node3
    [2, 1, 0], // slot 5: node3 → node2 → node1
];

const CONTROL_API: &str = "http://localhost:9000";

/// Path written by baseline.rs — loaded at startup.
const BASELINE_JSON: &str = "experiment_results/baseline.json";

/// Read an env var, parse it as T, or return a default.
fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}


// ============================================================
// Phase label
// ============================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Warmup,
    FaultInject,
    Observe,
}

impl Phase {
    fn as_label(self) -> &'static str {
        match self {
            Phase::Warmup      => "warmup",
            Phase::FaultInject => "fault_inject",
            Phase::Observe     => "observe",
        }
    }
}


// ============================================================
// Main
// ============================================================

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // ── Runtime configuration (overridable via TR_* env vars) ─────────────────
    let key_space:                  u32   = env_or("TR_KEY_SPACE",                  100_000u32);
    let workers_per_shard:          usize = env_or("TR_WORKERS_PER_SHARD",          50usize);
    let num_workers:                usize = workers_per_shard * 3;
    let warmup_secs:                u64   = env_or("TR_WARMUP_SECS",                180u64);
    let warmup_start_workers:       usize = env_or("TR_WARMUP_START_WORKERS",       15usize);
    let warmup_ramp_step:           usize = env_or("TR_WARMUP_RAMP_STEP",           15usize);
    let warmup_ramp_step_secs:      u64   = env_or("TR_WARMUP_RAMP_STEP_SECS",      10u64);
    let fault_down_secs:            u64   = env_or("TR_FAULT_DOWN_SECS",            180u64);
    let observe_secs:               u64   = env_or("TR_OBSERVE_SECS",               180u64);
    let poll_interval_secs:         u64   = env_or("TR_POLL_INTERVAL_SECS",         5u64);
    let fault_workers:              usize = env_or("TR_FAULT_WORKERS",              600usize);
    let fault_ramp_step_secs:       u64   = env_or("TR_FAULT_RAMP_STEP_SECS",       3u64);
    let cassandra_mem_threshold_pct: f64  = env_or("TR_CASSANDRA_MEM_THRESHOLD_PCT", 80.0f64);
    let mem_poll_interval_ms:       u64   = env_or("TR_MEM_POLL_INTERVAL_MS",       1_000u64);
    let request_timeout_ms:         u64   = env_or("TR_REQUEST_TIMEOUT_MS",         4000u64);


    // ── Load baseline ────────────────────────────────────────────────────────
    let baseline: serde_json::Value = {
        let s = std::fs::read_to_string(BASELINE_JSON).unwrap_or_else(|_| {
            eprintln!("ERROR: {BASELINE_JSON} not found.");
            eprintln!("Run: cargo run -p twin_ring_exp --bin baseline");
            std::process::exit(1);
        });
        serde_json::from_str(&s)?
    };
    let baseline_cache_p99 = baseline["cache_p99_us"].as_f64().unwrap_or(1.0);
    let baseline_hit_rate  = baseline["hit_rate"].as_f64().unwrap_or(0.0);
    let baseline_rps       = baseline["throughput_rps"].as_f64().unwrap_or(0.0);
    println!("📐 Baseline (from {BASELINE_JSON}):");
    println!("   cache_p99={:.0}µs  hit_rate={:.1}%  rps={:.0}",
        baseline_cache_p99, baseline_hit_rate * 100.0, baseline_rps);
    println!("   Metastable threshold: cache_p99 >= {:.0}µs (20×)  AND  hit_rate drop >= 30pp",
        baseline_cache_p99 * 20.0);

    let out_path = results_path("simple_metastable")?;
    println!("📊 Metrics → {}", out_path.display());
    let mut csv = MetricsWriter::new(&out_path)?;

    let client = Arc::new(
        Client::builder()
            .pool_max_idle_per_host(200)
            .timeout(Duration::from_millis(request_timeout_ms))
            .build()?,
    );

    let control_client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let nodes: Vec<String> = NODES.iter().map(|s| s.to_string()).collect();

    let (phase_tx, phase_rx) = watch::channel(Phase::Warmup);
    let (window_tx, mut window_rx) = mpsc::channel::<NodeWindow>(256);

    // Background stats poller
    {
        let poller = StatsPoller::new(nodes.clone(), poll_interval_secs);
        let mut poller_phase_rx = phase_rx.clone();
        let (label_tx, label_rx) = watch::channel(Phase::Warmup.as_label().to_string());

        tokio::spawn(async move {
            loop {
                if poller_phase_rx.changed().await.is_err() { break; }
                let p = *poller_phase_rx.borrow();
                let _ = label_tx.send(p.as_label().to_string());
            }
        });

        tokio::spawn(async move {
            let _ = poller.run(label_rx, window_tx).await;
        });
    }

    // Key-based routing worker. The key determines which node is primary and
    // which nodes are tried on failover via FAILOVER_ORDER[key % 6].
    // All workers — warmup and surge — are identical. More surge workers means
    // more total Cassandra pressure when TTL-expired keys can't be refilled.
    let spawn_worker = move |worker_id: usize,
              client: Arc<Client>,
              nodes: Vec<String>,
              active_ceiling: Arc<AtomicUsize>| {
            let ks = key_space;
            tokio::spawn(async move {
                let mut rng = ChaCha8Rng::seed_from_u64(worker_id as u64);
                loop {
                    if worker_id >= active_ceiling.load(Ordering::Relaxed) {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    // Zipfian over full keyspace: 80% of requests hit hottest 20% of keys.
                    let hot_end = (ks / 5).max(1);
                    let k: u32 = if rng.random::<f64>() < 0.8 {
                        rng.random_range(0..hot_end)
                    } else {
                        rng.random_range(hot_end..ks)
                    };
                    let slot = (k % 6) as usize;
                    let order = FAILOVER_ORDER[slot];
                    for attempt in 0..NODES.len() {
                        let url = format!("{}/get/key{}", nodes[order[attempt]], k);
                        match client.get(&url).send().await {
                            Ok(_)  => break,
                            Err(_) => {}
                        }
                    }
                    tokio::task::yield_now().await;
                }
            });
    };

    // ── Phase 1: WARMUP ──────────────────────────────────────────────────────
    // Gradual ramp so Cassandra is not overwhelmed during cache fill.
    // Hot Zipfian keys fill quickly → hit_rate climbs to ~95%+ → cache_p99 drops
    // to sub-ms hit latency. This is the healthy-system state.
    println!("\n⏳ [warmup] {warmup_secs}s — ramping {warmup_start_workers}→{num_workers} workers...");
    println!("   Expect: hit_rate → ~95%+, cache_p99 → sub-ms");

    let active_ceiling = Arc::new(AtomicUsize::new(warmup_start_workers));

    for worker_id in 0..num_workers {
        spawn_worker(worker_id, client.clone(), nodes.clone(), active_ceiling.clone());
    }

    {
        let ceiling = active_ceiling.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(warmup_ramp_step_secs)).await;
                let prev = ceiling.load(Ordering::Relaxed);
                if prev >= num_workers { break; }
                let next = (prev + warmup_ramp_step).min(num_workers);
                ceiling.store(next, Ordering::Relaxed);
                println!("   [warmup ramp] active workers -> {next}");
            }
        });
    }

    run_phase(warmup_secs, &mut window_rx, &mut csv, print_window).await?;

    // ── Phase 2: FAULT_INJECT ────────────────────────────────────────────────
    phase_tx.send(Phase::FaultInject).ok();
    println!("\n💥 [fault_inject] Killing node 1...");
    println!("   Slot-0 keys fail over to node2, slot-3 keys fail over to node3 — 100% miss on both.");
    println!("   Surging load to {fault_workers} workers while Cassandra absorbs the spike.");
    let fault_start = tokio::time::Instant::now();
    node_kill("1", &control_client).await;

    for worker_id in num_workers..fault_workers {
        spawn_worker(worker_id, client.clone(), nodes.clone(), active_ceiling.clone());
    }
    {
        let ceiling = active_ceiling.clone();
        tokio::spawn(async move {
            let to_add   = fault_workers.saturating_sub(num_workers);
            let steps    = (to_add / 10).max(1);
            let per_step = (to_add + steps - 1) / steps;
            for _ in 0..steps {
                tokio::time::sleep(Duration::from_secs(fault_ramp_step_secs)).await;
                let next = (ceiling.load(Ordering::Relaxed) + per_step).min(fault_workers);
                ceiling.store(next, Ordering::Relaxed);
                println!("   [surge] active workers -> {next}");
            }
        });
    }

    // Wait for Cassandra memory threshold (or fault_down_secs timeout).
    // Either way, node 1 stays DOWN — we do not restart yet.
    let fault_deadline = fault_start + Duration::from_secs(fault_down_secs);
    let mut cassandra_mem_pct = 0.0_f64;
    loop {
        let remaining = fault_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            println!("   [cassandra] timeout — proceeding at {cassandra_mem_pct:.1}%");
            break;
        }
        while let Ok(w) = window_rx.try_recv() {
            print_window(&w);
            csv.write(&w)?;
        }
        tokio::time::sleep(Duration::from_millis(mem_poll_interval_ms)).await;
        if let Ok(pct) = get_cassandra_mem_pct(&control_client).await {
            cassandra_mem_pct = pct;
            println!("   [cassandra] mem={pct:.1}%  (threshold: {cassandra_mem_threshold_pct}%)");
            if pct >= cassandra_mem_threshold_pct {
                println!("   Cassandra at {cassandra_mem_pct:.1}% — keeping node 1 down for remainder of fault window so nodes 2/3 decay via TTL...");
                break;
            }
        }
    }

    // Drain the rest of fault_down_secs (nodes 2+3 lose warmth via TTL expiry).
    let remaining_fault = fault_deadline.saturating_duration_since(tokio::time::Instant::now());
    if !remaining_fault.is_zero() {
        println!("   Waiting {:.0}s for nodes 2/3 to decay (TTL=30s)...", remaining_fault.as_secs_f64());
        run_phase(remaining_fault.as_secs().max(1), &mut window_rx, &mut csv, print_window).await?;
    }

    // ── Restart node 1 cold (nodes 2+3 should now be decayed) ───────────────
    println!("   Restarting node 1 cold — all 3 nodes should now miss together.");
    node_start("1", &control_client).await;
    run_phase(poll_interval_secs, &mut window_rx, &mut csv, print_window).await?;
    phase_tx.send(Phase::Observe).ok();

    // ── Phase 3: OBSERVE ─────────────────────────────────────────────────────
    let threshold_p99    = baseline_cache_p99 * 20.0;
    let threshold_hrdrop = 0.30_f64;
    println!("\n👁  [observe] {observe_secs}s — fault resolved, watching for self-recovery...");
    println!("   METASTABLE = cache_p99 >= {threshold_p99:.0}µs (20×)  AND  hit_rate drop >= 30pp");
    println!("   RECOVERY   = cache_p99 → baseline ({:.0}µs), hit_rate → {:.1}%",
        baseline_cache_p99, baseline_hit_rate * 100.0);

    let mut obs_p99_samples:      Vec<u64> = Vec::new();
    let mut obs_hit_rate_samples: Vec<f64> = Vec::new();
    let mut spike_rps: Option<f64>         = None;

    run_phase(observe_secs, &mut window_rx, &mut csv, |w| {
        print_window(w);
        let ratio = w.cache_p99_us as f64 / baseline_cache_p99;
        println!("   [metastable check] {}  hit={:.1}% (base {:.1}%)  cache_p99={}µs  ratio={:.1}×  rps={:.0}",
            w.node, w.hit_rate * 100.0, baseline_hit_rate * 100.0,
            w.cache_p99_us, ratio, w.throughput_rps);
        if spike_rps.is_none() && ratio >= 20.0 {
            spike_rps = Some(w.throughput_rps);
            println!("   *** 20× SPIKE DETECTED at {:.0} RPS ***", w.throughput_rps);
        }
        if w.cache_p99_us > 0 { obs_p99_samples.push(w.cache_p99_us); }
        obs_hit_rate_samples.push(w.hit_rate);
    }).await?;

    // ── Final verdict ─────────────────────────────────────────────────────────
    let avg_obs_p99 = if obs_p99_samples.is_empty() { 0.0 } else {
        obs_p99_samples.iter().sum::<u64>() as f64 / obs_p99_samples.len() as f64
    };
    let avg_obs_hit_rate = if obs_hit_rate_samples.is_empty() { 0.0 } else {
        obs_hit_rate_samples.iter().sum::<f64>() / obs_hit_rate_samples.len() as f64
    };
    let p99_ratio = if baseline_cache_p99 > 0.0 { avg_obs_p99 / baseline_cache_p99 } else { 0.0 };
    let hr_drop   = baseline_hit_rate - avg_obs_hit_rate;

    let latency_degraded = p99_ratio >= 20.0;
    let cache_degraded   = hr_drop   >= threshold_hrdrop;

    println!("\n✅ Experiment complete.");
    println!("   baseline  hit_rate={:.1}%   cache_p99={:.0}µs", baseline_hit_rate * 100.0, baseline_cache_p99);
    println!("   observe   hit_rate={:.1}%   cache_p99={:.0}µs", avg_obs_hit_rate * 100.0, avg_obs_p99);
    println!("   p99 ratio      = {p99_ratio:.1}×  [threshold: 20×]");
    println!("   hit_rate drop  = {:.1}pp  [threshold: 30pp]", hr_drop * 100.0);
    if let Some(rps) = spike_rps {
        println!("   20× spike RPS  = {rps:.0}");
    }
    if latency_degraded && cache_degraded {
        println!("   VERDICT: METASTABLE FAILURE confirmed (both conditions met)");
    } else {
        println!("   VERDICT: System recovered (p99 ratio={p99_ratio:.1}×, hit_rate drop={:.1}pp)", hr_drop * 100.0);
    }
    println!("   Results: {}", out_path.display());
    Ok(())
}


// ============================================================
// Display
// ============================================================

fn print_window(w: &NodeWindow) {
    println!(
        "  [{:>12}] {:<28} | hit={:>5.1}%  rps={:>5.0} \
         | cache_p99={:>8}µs  db_p99={:>8}µs  db_err={}",
        w.phase,
        w.node,
        w.hit_rate * 100.0,
        w.throughput_rps,
        w.cache_p99_us,
        w.db_p99_us,
        w.db_errors,
    );
}


// ============================================================
// Control API
// ============================================================

async fn get_cassandra_mem_pct(client: &Client) -> Result<f64> {
    let resp = client
        .get(format!("{CONTROL_API}/cassandra-mem"))
        .send().await?
        .json::<serde_json::Value>().await?;
    Ok(resp["mem_pct"].as_f64().unwrap_or(0.0))
}

async fn node_kill(node_id: &str, client: &Client) {
    let url = format!("{}/kill/{}", CONTROL_API, node_id);
    match client.post(&url).send().await {
        Ok(resp) if resp.status().is_success() => println!("  ✓ Killed node {}", node_id),
        Ok(resp) => println!("  ✗ Kill returned {}: {:?}", resp.status(), resp.text().await),
        Err(e)   => println!("  ✗ Kill failed: {}", e),
    }
}

async fn node_start(node_id: &str, client: &Client) {
    let url = format!("{}/start/{}", CONTROL_API, node_id);
    match client.post(&url).send().await {
        Ok(_)  => println!("  ✓ Started node {}", node_id),
        Err(e) => println!("  ✗ Start failed: {}", e),
    }
}
