//! Per-node CSV measurements and complete cluster polling rounds.
//! Client success means a completed HTTP 200 read; node counters supply database
//! diagnostics. Rates use measured elapsed time, not a nominal sleep duration.

use crate::experiment::workload::{ClientSample, WorkloadStats};
use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
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
    pub hits: u64,
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub misses: u64,
    pub backup_misses: u64,
    pub backup_db_calls: u64,
    pub db_hits: u64,
    pub db_not_found: u64,
    /// Spikes when Cassandra is overloaded — the primary metastable signal
    pub db_errors: u64,
    pub cache_p50_us: u64,
    pub cache_p99_us: u64,
    pub db_p50_us: u64,
    pub db_p99_us: u64,
    pub live_entries: u64,
}

// ============================================================
// NodeWindow — one polling window's worth of computed metrics
// ============================================================

/// All metrics for a single time window on a single node.
/// Kept as per-node rows in the CSV; recovery evaluates complete polling rounds.
#[derive(Debug, Clone, Serialize)]
pub struct NodeWindow {
    /// Unix timestamp in milliseconds when this window was recorded
    pub timestamp_ms: u128,
    /// The experiment phase at the time of this poll ("warmup", "inject", etc.)
    pub phase: String,
    /// Which node these metrics came from (e.g. "http://localhost:8001")
    pub node: String,

    // ---- Raw counts for this window ----
    pub hits: u64,
    /// Normal main-cache hits, including non-dual-ring strategies.
    pub l1_hits: u64,
    /// Replicas served through a dual-ring `/backup` request.
    pub l2_hits: u64,
    pub misses: u64,
    pub backup_misses: u64,
    pub backup_db_calls: u64,
    pub db_hits: u64,
    pub db_not_found: u64,
    pub db_errors: u64,

    // ---- Derived rates ----
    /// hits / (hits + misses) — rises during warmup, drops when cache goes cold
    pub hit_rate: f64,
    /// Total GET requests per second in this window
    pub throughput_rps: f64,
    /// Cassandra calls per second — spikes when cache is cold
    pub db_call_rate: f64,

    // ---- Latency percentiles (from the node) ----
    pub cache_p50_us: u64,
    /// p99 of full request latency. For hits: sub-ms. For misses: includes DB time.
    pub cache_p99_us: u64,
    pub db_p50_us: u64,
    /// p99 of Cassandra call latency. Spikes under metastable failure.
    pub db_p99_us: u64,
    /// Number of non-expired entries currently in the cache.
    pub live_entries: u64,
}

impl NodeWindow {
    /// Build a NodeWindow from a raw /stats response plus context.
    pub fn from_response(
        resp: NodeStatsResponse,
        node: String,
        phase: String,
        window_secs: f64,
    ) -> Self {
        let total = resp.hits + resp.misses;
        let db_total = resp.db_hits + resp.db_not_found + resp.db_errors;

        NodeWindow {
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            phase,
            node,
            hits: resp.hits,
            l1_hits: resp.l1_hits,
            l2_hits: resp.l2_hits,
            misses: resp.misses,
            backup_misses: resp.backup_misses,
            backup_db_calls: resp.backup_db_calls,
            db_hits: resp.db_hits,
            db_not_found: resp.db_not_found,
            db_errors: resp.db_errors,
            hit_rate: if total == 0 {
                0.0
            } else {
                resp.hits as f64 / total as f64
            },
            throughput_rps: total as f64 / window_secs,
            db_call_rate: db_total as f64 / window_secs,
            cache_p50_us: resp.cache_p50_us,
            cache_p99_us: resp.cache_p99_us,
            db_p50_us: resp.db_p50_us,
            db_p99_us: resp.db_p99_us,
            live_entries: resp.live_entries,
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
            "timestamp_ms",
            "phase",
            "node",
            "hits",
            "l1_hits",
            "l2_hits",
            "misses",
            "backup_misses",
            "backup_db_calls",
            "db_hits",
            "db_not_found",
            "db_errors",
            "hit_rate",
            "throughput_rps",
            "db_call_rate",
            "cache_p50_us",
            "cache_p99_us",
            "db_p50_us",
            "db_p99_us",
            "live_entries",
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
            w.l1_hits.to_string(),
            w.l2_hits.to_string(),
            w.misses.to_string(),
            w.backup_misses.to_string(),
            w.backup_db_calls.to_string(),
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
            w.live_entries.to_string(),
        ])?;
        self.writer.flush()?;
        Ok(())
    }
}

/// One polling round, with actual elapsed time and all client outcomes.
/// Missing node telemetry and windows spanning phases remain explicit.
#[derive(Debug, Clone, Serialize)]
pub struct MeasurementRound {
    pub sequence: u64,
    pub started_ms: u128,
    pub timestamp_ms: u128,
    pub duration_secs: f64,
    pub phase: String,
    pub nodes: Vec<NodeWindow>,
    pub client: ClientSample,
    pub missing_nodes: Vec<String>,
    pub unaligned_nodes: Vec<String>,
    pub aligned: bool,
    pub phase_consistent: bool,
}

impl MeasurementRound {
    /// A polling interval can be wholly in observation while a slow read from
    /// overload is still running or completes inside it.
    pub fn within_one_phase(&self) -> bool {
        self.phase_consistent
            && self.client.prior_phase_in_flight == 0
            && self.client.prior_phase_finished == 0
    }

    pub fn complete(&self, expected: usize) -> bool {
        self.missing_nodes.is_empty()
            && self.nodes.len() == expected
            && self.client.by_primary.len() == expected
    }
}

pub struct StatsPoller {
    nodes: Vec<String>,
    interval_secs: u64,
    client: Client,
    workload: Arc<WorkloadStats>,
}

impl StatsPoller {
    pub fn new(
        nodes: Vec<String>,
        interval_secs: u64,
        workload: Arc<WorkloadStats>,
    ) -> Result<Self> {
        Ok(Self {
            nodes,
            interval_secs,
            workload,
            client: Client::builder()
                .timeout(Duration::from_secs(interval_secs.clamp(1, 2)))
                .build()?,
        })
    }

    /// Poll the nodes concurrently. A failed request produces an incomplete
    /// round rather than disappearing. The next round is also unaligned because
    /// it may contain counters accumulated across the failed drain.
    pub async fn run(self, phase_rx: watch::Receiver<String>, tx: mpsc::Sender<MeasurementRound>) {
        let interval = Duration::from_secs(self.interval_secs);
        let mut last = tokio::time::Instant::now();
        let mut last_ms = unix_ms();
        let mut previously_missing = self.nodes.clone();
        let mut previous_phase = None;
        let mut sequence = 0;
        loop {
            sleep(interval).await;
            let phase = phase_rx.borrow().clone();
            let http = &self.client;
            let responses = futures::future::join_all(self.nodes.iter().map(|node| async move {
                let response = http
                    .get(format!("{node}/stats"))
                    .send()
                    .await?
                    .error_for_status()?;
                response.json::<NodeStatsResponse>().await
            }))
            .await;
            let now = tokio::time::Instant::now();
            let timestamp_ms = unix_ms();
            let duration_secs = now.duration_since(last).as_secs_f64();
            let client = self.workload.snapshot_and_reset();
            let mut nodes = Vec::new();
            let mut missing_nodes = Vec::new();
            for (node, response) in self.nodes.iter().zip(responses) {
                match response {
                    Ok(stats) => nodes.push(NodeWindow::from_response(
                        stats,
                        node.clone(),
                        phase.clone(),
                        duration_secs,
                    )),
                    Err(_) => missing_nodes.push(node.clone()),
                }
            }
            sequence += 1;
            let unaligned_nodes: Vec<_> = self
                .nodes
                .iter()
                .filter(|node| previously_missing.contains(node) || missing_nodes.contains(node))
                .cloned()
                .collect();
            let phase_consistent =
                previous_phase.as_ref() == Some(&phase) && *phase_rx.borrow() == phase;
            let round = MeasurementRound {
                sequence,
                started_ms: last_ms,
                timestamp_ms,
                duration_secs,
                phase: phase.clone(),
                nodes,
                client,
                missing_nodes,
                aligned: unaligned_nodes.is_empty(),
                unaligned_nodes,
                phase_consistent,
            };
            previously_missing = round.missing_nodes.clone();
            previous_phase = Some(phase);
            last = now;
            last_ms = timestamp_ms;
            if tx.send(round).await.is_err() {
                return;
            }
        }
    }
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
