use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand::SeedableRng;
use reqwest::Client;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{sleep, interval};

// -------------------------------
// Config
// -------------------------------

const NUM_WORKERS: usize = 10;
const OPS_PER_WORKER: usize = 10_000; // each does this many PUT+GET
const KEY_SPACE: u32 = 1000;          // keys are in 0..KEY_SPACE
const REPORT_INTERVAL_SECS: u64 = 5;
const BASELINE_MIN_REQUESTS: usize = 5_000;
const OVERLOAD_LATENCY_MULTIPLIER: u128 = 3; // p99 > 3x baseline
const OVERLOAD_RPS_FRACTION: f64 = 0.6;      // rps < 60% baseline
const OVERLOAD_FAILURE_RATE: f64 = 0.05;     // >5% failures
const METASTABLE_DURATION_SECS: u64 = 30;    // overload persists this long ⇒ metastable

// -------------------------------
// Metrics structures
// -------------------------------

#[derive(Debug)]
struct Metrics {
    latency: Duration,
    node: String,
    success: bool,
}

#[derive(Default)]
struct Stats {
    latencies: Vec<u128>,                // µs
    node_counts: HashMap<String, usize>, // per-node requests
    success: usize,
    failure: usize,
    requests: usize,
}

struct OverloadState {
    baseline_p99: Option<u128>,
    baseline_rps: Option<f64>,
    last_window_requests: usize,
    last_window_start: Instant,
    overload_start: Option<Instant>,
    metastable_detected: bool,
}

impl Default for OverloadState {
    fn default() -> Self {
        Self {
            baseline_p99: None,
            baseline_rps: None,
            last_window_requests: 0,
            last_window_start: Instant::now(),
            overload_start: None,
            metastable_detected: false,
        }
    }
}

// -------------------------------
// Main
// -------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let client = Arc::new(Client::new());
    let peers = Arc::new(vec![
        "http://localhost:8001".to_string(),
        "http://localhost:8002".to_string(),
        "http://localhost:8003".to_string(),
    ]);

    let (tx, rx) = mpsc::channel::<Metrics>(10_000);

    println!(
        "🚀 Starting metastability simulation: {} workers × {} ops (≈ {}M requests)",
        NUM_WORKERS,
        OPS_PER_WORKER,
        (NUM_WORKERS * OPS_PER_WORKER * 2) as f64 / 1_000_000.0
    );

    // ---------------------------------------------
    // Metrics + overload / metastability detector
    // ---------------------------------------------
    tokio::spawn(metrics_task(rx));

    // ---------------------------------------------
    // Chaos task: kill & restart nodes periodically
    // ---------------------------------------------
    tokio::spawn(chaos_task());

    // ---------------------------------------------
    // Worker tasks
    // ---------------------------------------------
    let mut workers = JoinSet::new();

    for worker_id in 0..NUM_WORKERS {
        let client = client.clone();
        let peers = peers.clone();
        let tx = tx.clone();

        workers.spawn(async move {
            // Each worker gets its own deterministic, Send-safe RNG
            let mut rng = ChaCha8Rng::seed_from_u64(1234 + worker_id as u64);

            for i in 0..OPS_PER_WORKER {
                // pick a node
                let peer = peers[rng.random_range(0..peers.len())].clone();
                let key = format!("key{}", rng.random_range(0..KEY_SPACE));
                let value = format!("value{}_{}", worker_id, i);

                let start = Instant::now();

                // PUT
                let put_ok = client
                    .post(format!("{}/put/{}", &peer, key))
                    .body(value)
                    .send()
                    .await
                    .is_ok();

                // GET
                let get_ok = client
                    .get(format!("{}/get/{}", &peer, key))
                    .send()
                    .await
                    .is_ok();

                let success = put_ok && get_ok;

                // Send metrics (if the channel is full or closed, ignore)
                let _ = tx
                    .send(Metrics {
                        latency: start.elapsed(),
                        node: peer,
                        success,
                    })
                    .await;
            }
        });
    }

    // Drop our own tx so channel closes once workers are done
    drop(tx);

    // Wait for all workers to finish
    while workers.join_next().await.is_some() {}

    println!("✅ Workers completed all operations.");
    println!("(metrics + overload detection task will exit once all data is consumed)");

    Ok(())
}

// -------------------------------
// Metrics & overload detection task
// -------------------------------

async fn metrics_task(mut rx: mpsc::Receiver<Metrics>) {
    let mut stats = Stats::default();
    let mut overload = OverloadState::default();
    let mut tick = interval(Duration::from_secs(REPORT_INTERVAL_SECS));

    println!("📈 Metrics + overload detector started.");

    loop {
        tokio::select! {
            maybe_m = rx.recv() => {
                match maybe_m {
                    Some(m) => {
                        stats.requests += 1;
                        if m.success {
                            stats.success += 1;
                        } else {
                            stats.failure += 1;
                        }
                        stats.latencies.push(m.latency.as_micros());
                        *stats.node_counts.entry(m.node).or_default() += 1;
                    }
                    None => {
                        // Channel closed: final report then exit
                        println!("📉 Metrics channel closed. Final report:");
                        print_report(&stats);
                        detect_overload_and_metastability(&mut stats, &mut overload, true);
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                if stats.requests == 0 {
                    println!("(no requests yet)");
                    continue;
                }

                print_report(&stats);
                detect_overload_and_metastability(&mut stats, &mut overload, false);
            }
        }
    }
}

fn print_report(stats: &Stats) {
    println!("================ METRICS ================");
    println!("Total requests : {}", stats.requests);
    println!("Success        : {}", stats.success);
    println!("Failure        : {}", stats.failure);

    let n = stats.latencies.len();
    if n == 0 {
        println!("No latency data yet.");
        println!("=========================================");
        return;
    }

    let mut sorted = stats.latencies.clone();
    sorted.sort_unstable();

    let idx = |p: f64| -> usize {
        if n == 0 {
            0
        } else {
            let pos = (n as f64 * p).floor() as usize;
            pos.min(n - 1)
        }
    };

    let p50 = sorted[idx(0.50)];
    let p90 = sorted[idx(0.90)];
    let p99 = sorted[idx(0.99)];
    let max = sorted[n - 1];

    println!("p50 latency    : {} µs", p50);
    println!("p90 latency    : {} µs", p90);
    println!("p99 latency    : {} µs", p99);
    println!("max latency    : {} µs", max);

    println!("Load per node:");
    for (node, count) in &stats.node_counts {
        println!("  {} => {}", node, count);
    }

    println!("=========================================");
}

fn detect_overload_and_metastability(
    stats: &mut Stats,
    overload: &mut OverloadState,
    final_check: bool,
) {
    let now = Instant::now();

    // Compute RPS over this window
    let window_reqs = stats.requests - overload.last_window_requests;
    let window_secs = now
        .duration_since(overload.last_window_start)
        .as_secs_f64()
        .max(1e-6);
    let rps = window_reqs as f64 / window_secs;

    overload.last_window_requests = stats.requests;
    overload.last_window_start = now;

    // Need latency data
    let n = stats.latencies.len();
    if n == 0 {
        return;
    }

    let mut sorted = stats.latencies.clone();
    sorted.sort_unstable();
    let idx_p99 = ((n as f64 * 0.99).floor() as usize).min(n - 1);
    let p99 = sorted[idx_p99];

    // Establish baseline once
    if overload.baseline_p99.is_none() && stats.requests > BASELINE_MIN_REQUESTS {
        overload.baseline_p99 = Some(p99);
        overload.baseline_rps = Some(rps);
        println!("📊 Established baseline: p99={}µs, rps={:.1}", p99, rps);
        return;
    }

    let (Some(base_p99), Some(base_rps)) = (overload.baseline_p99, overload.baseline_rps) else {
        // baseline not ready yet
        return;
    };

    let fail_rate = stats.failure as f64 / stats.requests.max(1) as f64;

    let overload_now =
        p99 > base_p99 * OVERLOAD_LATENCY_MULTIPLIER ||
        rps < base_rps * OVERLOAD_RPS_FRACTION ||
        fail_rate > OVERLOAD_FAILURE_RATE;

    println!(
        "RPS: {:.1} | p99: {}µs | base p99: {}µs | fail_rate: {:.2} | overload_now: {}",
        rps, p99, base_p99, fail_rate, overload_now
    );

    if overload_now {
        if overload.overload_start.is_none() {
            overload.overload_start = Some(now);
            println!("🔥 OVERLOAD DETECTED at {:?}", now);
        }
    } else {
        // If not overloaded now, clear overload timer
        overload.overload_start = None;
    }

    if let Some(start_t) = overload.overload_start {
        let dur = now.duration_since(start_t);
        if !overload.metastable_detected && dur > Duration::from_secs(METASTABLE_DURATION_SECS) {
            overload.metastable_detected = true;
            println!("💀💀💀 METASTABLE FAILURE DETECTED 💀💀💀");
            println!(
                "System has been overloaded for > {}s and has not recovered — this indicates metastable behavior.",
                METASTABLE_DURATION_SECS
            );
        }
    }

    if final_check && overload.metastable_detected {
        println!("(Final check) Metastable state was reached during this run.");
    }
}

// -------------------------------
// Chaos task: kill + restart nodes
// -------------------------------

async fn chaos_task() {
    let control = Client::new();
    let nodes = vec!["1", "2", "3"];

    println!("🧨 Chaos task started: will randomly kill & restart cache nodes.");

    loop {
        // Pick a random node to kill
        let mut rng = ChaCha8Rng::seed_from_u64(rand::random());
        let node = nodes[rng.random_range(0..nodes.len())];

        println!("💥 Killing cache_node_{}", node);
        let kill_url = format!("http://localhost:9000/kill/{}", node);
        let _ = control.post(&kill_url).send().await;

        // Keep it down for a bit to force Cassandra traffic
        sleep(Duration::from_secs(3)).await;

        println!("🔄 Restarting cache_node_{}", node);
        let start_url = format!("http://localhost:9000/start/{}", node);
        let _ = control.post(&start_url).send().await;

        // Give time for node to come up, but not enough for Cassandra to fully recover
        sleep(Duration::from_secs(10)).await;
    }
}
