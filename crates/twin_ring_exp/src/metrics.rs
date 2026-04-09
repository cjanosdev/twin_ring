//! Reusable experiment metrics infrastructure.
//!
//! # How to use in any experiment binary
//!
//! ```rust
//! use twin_ring_exp::metrics::{MetricsWriter, StatsPoller, run_phase};
//! use twin_ring_core::experiment_path::results_path;
//! use tokio::sync::{mpsc, watch};
//!
//! // 1. Open CSV output file
//! let mut csv = MetricsWriter::new(&results_path("my_experiment")?)?;
//!
//! // 2. watch channel: you control the phase label; poller reads it to tag each row
//! let (phase_tx, phase_rx) = watch::channel("warmup".to_string());
//!
//! // 3. mpsc channel: poller sends NodeWindow events → your experiment reads them
//! let (window_tx, mut window_rx) = mpsc::channel(256);
//!
//! // 4. Start the poller as a background task
//! let poller = StatsPoller::new(vec!["http://localhost:8001".into()], 5);
//! tokio::spawn(poller.run(phase_rx, window_tx));
//!
//! // 5. Run a timed phase — collects windows, prints them, writes to CSV
//! run_phase(30, &mut window_rx, &mut csv, |w| println!("{:?}", w)).await?;
//!
//! // 6. Change phase and run again
//! phase_tx.send("observe".into()).ok();
//! run_phase(60, &mut window_rx, &mut csv, |w| println!("{:?}", w)).await?;
//! ```

use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::fs::File;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;


// ============================================================
// NodeStatsResponse — mirrors the JSON from GET /stats on a node
// ============================================================

/// Deserializes the JSON body returned by `GET /stats` on a cache node.
///
/// Rust concept: #[derive(Deserialize)]
///   This attribute tells the compiler to automatically generate code
///   that can parse JSON (or other formats) into this struct.
///   The field names must match the JSON keys exactly.
#[derive(Debug, Deserialize, Clone)]
pub struct NodeStatsResponse {
    pub hits:         u64,
    pub misses:       u64,
    pub db_hits:      u64,
    pub db_not_found: u64,
    /// Spikes when Cassandra is overloaded — the primary metastable signal
    pub db_errors:    u64,
    pub cache_p50_us: u64,
    pub cache_p99_us: u64,
    pub db_p50_us:    u64,
    pub db_p99_us:    u64,
}


// ============================================================
// NodeWindow — one polling window's worth of computed metrics
// ============================================================

/// All metrics for a single time window on a single node.
/// This is the unit of data flowing from StatsPoller → your experiment.
#[derive(Debug, Clone)]
pub struct NodeWindow {
    /// Unix timestamp in milliseconds when this window was recorded
    pub timestamp_ms:    u128,
    /// The experiment phase at the time of this poll ("warmup", "inject", etc.)
    pub phase:           String,
    /// Which node these metrics came from (e.g. "http://localhost:8001")
    pub node:            String,

    // ---- Raw counts for this window ----
    pub hits:         u64,
    pub misses:       u64,
    pub db_hits:      u64,
    pub db_not_found: u64,
    pub db_errors:    u64,

    // ---- Derived rates ----
    /// hits / (hits + misses) — rises during warmup, drops when cache goes cold
    pub hit_rate:       f64,
    /// Total GET requests per second in this window
    pub throughput_rps: f64,
    /// Cassandra calls per second — spikes when cache is cold
    pub db_call_rate:   f64,

    // ---- Latency percentiles (from the node) ----
    pub cache_p50_us: u64,
    /// p99 of full request latency. For hits: sub-ms. For misses: includes DB time.
    pub cache_p99_us: u64,
    pub db_p50_us:    u64,
    /// p99 of Cassandra call latency. Spikes under metastable failure.
    pub db_p99_us:    u64,
}

impl NodeWindow {
    /// Build a NodeWindow from a raw /stats response plus context.
    pub fn from_response(
        resp: NodeStatsResponse,
        node: String,
        phase: String,
        window_secs: f64,
    ) -> Self {
        let total    = resp.hits + resp.misses;
        let db_total = resp.db_hits + resp.db_not_found + resp.db_errors;

        NodeWindow {
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            phase,
            node,
            hits:         resp.hits,
            misses:       resp.misses,
            db_hits:      resp.db_hits,
            db_not_found: resp.db_not_found,
            db_errors:    resp.db_errors,
            hit_rate:       if total == 0 { 0.0 } else { resp.hits as f64 / total as f64 },
            throughput_rps: total as f64 / window_secs,
            db_call_rate:   db_total as f64 / window_secs,
            cache_p50_us: resp.cache_p50_us,
            cache_p99_us: resp.cache_p99_us,
            db_p50_us:    resp.db_p50_us,
            db_p99_us:    resp.db_p99_us,
        }
    }
}


// ============================================================
// MetricsWriter — writes NodeWindows to a CSV file
// ============================================================

/// Opens a CSV file and writes one row per NodeWindow.
/// Create it once at the start of your experiment, call write() for each window.
pub struct MetricsWriter {
    writer: csv::Writer<File>,
}

impl MetricsWriter {
    /// Create (or overwrite) the CSV file at `path` and write the header row.
    pub fn new(path: &Path) -> Result<Self> {
        let mut writer = csv::Writer::from_path(path)?;
        writer.write_record([
            "timestamp_ms", "phase", "node",
            "hits", "misses", "db_hits", "db_not_found", "db_errors",
            "hit_rate", "throughput_rps", "db_call_rate",
            "cache_p50_us", "cache_p99_us",
            "db_p50_us", "db_p99_us",
        ])?;
        writer.flush()?;
        Ok(MetricsWriter { writer })
    }

    /// Append one row for the given window. Flushes to disk immediately
    /// so data is not lost if the experiment crashes.
    pub fn write(&mut self, w: &NodeWindow) -> Result<()> {
        self.writer.write_record(&[
            w.timestamp_ms.to_string(),
            w.phase.clone(),
            w.node.clone(),
            w.hits.to_string(),
            w.misses.to_string(),
            w.db_hits.to_string(),
            w.db_not_found.to_string(),
            w.db_errors.to_string(),
            format!("{:.4}", w.hit_rate),
            format!("{:.1}", w.throughput_rps),
            format!("{:.1}", w.db_call_rate),
            w.cache_p50_us.to_string(),
            w.cache_p99_us.to_string(),
            w.db_p50_us.to_string(),
            w.db_p99_us.to_string(),
        ])?;
        self.writer.flush()?;
        Ok(())
    }
}


// ============================================================
// StatsPoller — polls GET /stats from nodes on a timer
// ============================================================

/// Polls `GET /stats` from each node every `interval_secs` seconds.
/// Emits one `NodeWindow` per node per interval via the mpsc sender.
///
/// # Rust concept: watch::channel
///   One sender, many receivers. The sender holds the "current value."
///   Any receiver can read it at any time with .borrow().
///   Perfect for a "current phase" label: your experiment updates it,
///   the poller reads it to tag each window.
///
/// # Rust concept: mpsc::channel
///   Multiple producers, single consumer. Here the poller is the only
///   producer, but mpsc is the idiomatic channel for "stream of events."
///   Your experiment loop reads from the other end with .recv().await.
pub struct StatsPoller {
    nodes:         Vec<String>,
    interval_secs: u64,
    client:        Client,
}

impl StatsPoller {
    pub fn new(nodes: Vec<String>, interval_secs: u64) -> Self {
        StatsPoller {
            nodes,
            interval_secs,
            client: Client::new(),
        }
    }

    /// Run the polling loop forever (until the receiver is dropped).
    /// Spawn this as a background task: `tokio::spawn(poller.run(phase_rx, tx))`.
    pub async fn run(
        self,
        phase_rx: watch::Receiver<String>,
        tx: mpsc::Sender<NodeWindow>,
    ) {
        let interval = Duration::from_secs(self.interval_secs);
        loop {
            sleep(interval).await;

            // .borrow() reads the current phase without blocking
            let phase = phase_rx.borrow().clone();

            for node in &self.nodes {
                let url = format!("{}/stats", node);
                match self.client.get(&url).send().await {
                    Ok(resp) => match resp.json::<NodeStatsResponse>().await {
                        Ok(stats) => {
                            let window = NodeWindow::from_response(
                                stats,
                                node.clone(),
                                phase.clone(),
                                self.interval_secs as f64,
                            );
                            // If the receiver is gone (experiment ended), stop quietly
                            if tx.send(window).await.is_err() {
                                return;
                            }
                        }
                        // Node returned unexpected data (maybe still starting up)
                        Err(_) => {}
                    },
                    // Node is down — expected during fault injection, skip silently
                    Err(_) => {}
                }
            }
        }
    }
}


// ============================================================
// run_phase — drives a timed experiment phase
// ============================================================

/// Collect `NodeWindow`s for `secs` seconds, write each to CSV,
/// and call `on_window` for display.
///
/// # Rust concept: impl Fn(&NodeWindow)
///   `impl Trait` in a function parameter means "any type that implements
///   this trait." `Fn(&NodeWindow)` is the trait for callable things
///   (functions, closures) that take a &NodeWindow.
///   At compile time Rust monomorphizes this — it generates a specialized
///   version for each concrete type you pass, so there is zero overhead
///   compared to calling the function directly.
///
/// # Example
/// ```rust
/// run_phase(30, &mut window_rx, &mut csv, |w| {
///     println!("hit_rate={:.0}%", w.hit_rate * 100.0);
/// }).await?;
/// ```
pub async fn run_phase(
    secs: u64,
    window_rx: &mut mpsc::Receiver<NodeWindow>,
    csv: &mut MetricsWriter,
    mut on_window: impl FnMut(&NodeWindow),
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        // Wait up to `remaining` time for the next window.
        // tokio::time::timeout returns Err if the deadline fires first.
        match tokio::time::timeout(remaining, window_rx.recv()).await {
            Ok(Some(w)) => {
                on_window(&w);
                csv.write(&w)?;
            }
            // Either deadline hit (Err) or channel closed (Ok(None))
            _ => break,
        }
    }
    Ok(())
}
