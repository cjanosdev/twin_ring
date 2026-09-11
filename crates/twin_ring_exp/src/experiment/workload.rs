//! Arrival-rate-controlled Zipfian load generator.
//!
//! Thirty overlapping cohorts create concentrated hot keys. A wall-clock pacer
//! offers requests independently of response time, so latency cannot silently
//! reduce demand. A bounded in-flight pool prevents runaway memory and records
//! rejected demand as shed load.

use hdrhistogram::Histogram;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Zipf};
use reqwest::Client;
use serde::Serialize;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use twin_ring_core::DualRing;

const NUM_COHORTS: usize = 30;
const COHORT_OVERLAP: u32 = 5;
const SCHEDULER_TICK: Duration = Duration::from_millis(1);

pub type SharedRing = Arc<RwLock<Arc<DualRing>>>;

pub fn shared_ring(ring: DualRing) -> SharedRing {
    Arc::new(RwLock::new(Arc::new(ring)))
}

pub fn replace_ring(shared: &SharedRing, ring: DualRing) {
    *shared.write().unwrap() = Arc::new(ring);
}

#[derive(Debug, Clone, Copy)]
pub enum RequestOutcome {
    Success,
    HttpError,
    TransportError,
}

#[derive(Debug, Clone, Copy)]
enum RequestFailure {
    Timeout,
    Connection,
    Other,
}

async fn attempt_read(client: &Client, url: &str) -> Result<u16, RequestFailure> {
    let response = client.get(url).send().await.map_err(|error| {
        if error.is_timeout() {
            RequestFailure::Timeout
        } else if error.is_connect() {
            RequestFailure::Connection
        } else {
            RequestFailure::Other
        }
    })?;
    let status = response.status().as_u16();
    response.bytes().await.map_err(|_| RequestFailure::Other)?;
    Ok(status)
}

async fn read_with_fallback<F, Fut>(mut attempt: F) -> RequestOutcome
where
    F: FnMut(bool) -> Fut,
    Fut: Future<Output = Result<u16, RequestFailure>>,
{
    let response = match attempt(false).await {
        Err(RequestFailure::Connection) => attempt(true).await,
        response => response,
    };
    match response {
        Ok(status) => response_outcome(status, true),
        Err(_) => RequestOutcome::TransportError,
    }
}

pub(super) fn response_outcome(status: u16, complete_body: bool) -> RequestOutcome {
    if !complete_body {
        RequestOutcome::TransportError
    } else if status == 200 {
        RequestOutcome::Success
    } else {
        RequestOutcome::HttpError
    }
}

#[derive(Debug)]
struct RequestCounters {
    offered: u64,
    admitted: u64,
    shed: u64,
    successful: u64,
    http_errors: u64,
    transport_errors: u64,
    success_latency: Histogram<u64>,
}

impl Default for RequestCounters {
    fn default() -> Self {
        Self {
            offered: 0,
            admitted: 0,
            shed: 0,
            successful: 0,
            http_errors: 0,
            transport_errors: 0,
            success_latency: Histogram::new(3).expect("valid histogram precision"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestWindow {
    pub offered: u64,
    pub admitted: u64,
    pub shed: u64,
    pub successful: u64,
    pub http_errors: u64,
    pub transport_errors: u64,
    /// p95 across successful logical reads, including failover and body read.
    pub success_latency_p95_us: Option<u64>,
}

impl RequestWindow {
    pub fn completed(&self) -> u64 {
        self.successful + self.http_errors + self.transport_errors
    }

    pub fn error_rate(&self) -> Option<f64> {
        let total = self.completed();
        (total > 0).then(|| (self.http_errors + self.transport_errors) as f64 / total as f64)
    }

    pub fn shed_rate(&self) -> Option<f64> {
        (self.offered > 0).then(|| self.shed as f64 / self.offered as f64)
    }
}

impl RequestCounters {
    fn window(&self) -> RequestWindow {
        RequestWindow {
            offered: self.offered,
            admitted: self.admitted,
            shed: self.shed,
            successful: self.successful,
            http_errors: self.http_errors,
            transport_errors: self.transport_errors,
            success_latency_p95_us: (!self.success_latency.is_empty())
                .then(|| self.success_latency.value_at_quantile(0.95)),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientSample {
    /// Indexed by the key's primary. A fallback remains part of the same read.
    pub by_primary: Vec<RequestWindow>,
    pub total: RequestWindow,
    pub in_flight: usize,
    pub prior_phase_in_flight: usize,
    pub prior_phase_finished: usize,
}

#[derive(Debug, Default)]
struct PhaseRequests {
    generation: u64,
    in_flight: BTreeMap<u64, usize>,
    prior_phase_finished: usize,
}

impl PhaseRequests {
    fn finish(&mut self, generation: u64) {
        let count = self
            .in_flight
            .get_mut(&generation)
            .expect("registered request");
        *count -= 1;
        if *count == 0 {
            self.in_flight.remove(&generation);
        }
        if generation != self.generation {
            self.prior_phase_finished += 1;
        }
    }
}

pub(super) struct InFlightRead {
    stats: Arc<WorkloadStats>,
    generation: u64,
    primary: usize,
    finished: bool,
}

impl InFlightRead {
    pub(super) fn finish(mut self, outcome: RequestOutcome, elapsed: Duration) {
        let mut phase = self.stats.phase.lock().unwrap();
        self.stats.record(self.primary, outcome, elapsed);
        phase.finish(self.generation);
        self.finished = true;
    }
}

impl Drop for InFlightRead {
    fn drop(&mut self) {
        if !self.finished {
            self.stats.phase.lock().unwrap().finish(self.generation);
        }
    }
}

#[derive(Debug)]
pub struct WorkloadStats {
    nodes: Vec<Mutex<RequestCounters>>,
    phase: Mutex<PhaseRequests>,
    succeeded: AtomicUsize,
    failed: AtomicUsize,
}

impl WorkloadStats {
    pub fn new(node_count: usize) -> Arc<Self> {
        Arc::new(Self {
            nodes: (0..node_count)
                .map(|_| Mutex::new(RequestCounters::default()))
                .collect(),
            phase: Mutex::new(PhaseRequests::default()),
            succeeded: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
        })
    }

    pub fn begin_phase(&self) {
        self.phase.lock().unwrap().generation += 1;
    }

    #[cfg(test)]
    pub(super) fn begin_read(self: &Arc<Self>, primary: usize) -> InFlightRead {
        let mut phase = self.phase.lock().unwrap();
        let generation = phase.generation;
        *phase.in_flight.entry(generation).or_default() += 1;
        InFlightRead {
            stats: self.clone(),
            generation,
            primary,
            finished: false,
        }
    }

    fn begin_admitted_read(self: &Arc<Self>, primary: usize) -> InFlightRead {
        let mut phase = self.phase.lock().unwrap();
        let generation = phase.generation;
        {
            let mut counters = self.nodes[primary].lock().unwrap();
            counters.offered += 1;
            counters.admitted += 1;
        }
        *phase.in_flight.entry(generation).or_default() += 1;
        InFlightRead {
            stats: self.clone(),
            generation,
            primary,
            finished: false,
        }
    }

    fn record_shed(&self, primary: usize) {
        let _phase = self.phase.lock().unwrap();
        let mut counters = self.nodes[primary].lock().unwrap();
        counters.offered += 1;
        counters.shed += 1;
    }

    pub fn any_success(&self) -> bool {
        self.succeeded.load(Ordering::Relaxed) > 0
    }

    pub fn failed(&self) -> usize {
        self.failed.load(Ordering::Relaxed)
    }

    fn record(&self, primary: usize, outcome: RequestOutcome, elapsed: Duration) {
        let mut counters = self.nodes[primary].lock().unwrap();
        match outcome {
            RequestOutcome::Success => {
                counters.successful += 1;
                let micros = u64::try_from(elapsed.as_micros())
                    .unwrap_or(u64::MAX)
                    .max(1);
                counters
                    .success_latency
                    .record(micros)
                    .expect("auto-resizing latency histogram");
                self.succeeded.fetch_add(1, Ordering::Relaxed);
            }
            RequestOutcome::HttpError => {
                counters.http_errors += 1;
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
            RequestOutcome::TransportError => {
                counters.transport_errors += 1;
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot_and_reset(&self) -> ClientSample {
        let mut phase = self.phase.lock().unwrap();
        let counters: Vec<_> = self
            .nodes
            .iter()
            .map(|node| std::mem::take(&mut *node.lock().unwrap()))
            .collect();
        let in_flight = phase.in_flight.values().sum();
        let prior_phase_in_flight = phase
            .in_flight
            .iter()
            .filter(|(generation, _)| **generation != phase.generation)
            .map(|(_, count)| *count)
            .sum();
        let prior_phase_finished = std::mem::take(&mut phase.prior_phase_finished);
        drop(phase);

        let by_primary = counters.iter().map(RequestCounters::window).collect();
        let mut total = RequestCounters::default();
        for node in counters {
            total.offered += node.offered;
            total.admitted += node.admitted;
            total.shed += node.shed;
            total.successful += node.successful;
            total.http_errors += node.http_errors;
            total.transport_errors += node.transport_errors;
            total
                .success_latency
                .add(node.success_latency)
                .expect("matching histogram precision");
        }
        ClientSample {
            by_primary,
            total: total.window(),
            in_flight,
            prior_phase_in_flight,
            prior_phase_finished,
        }
    }
}

/// Turns elapsed wall time into arrivals while retaining fractional credit.
#[derive(Debug, Default)]
struct ArrivalPacer {
    fractional: f64,
    previous_rate: usize,
}

impl ArrivalPacer {
    fn arrivals(&mut self, rate_per_sec: usize, elapsed: Duration) -> usize {
        if rate_per_sec != self.previous_rate {
            self.fractional = 0.0;
            self.previous_rate = rate_per_sec;
        }
        let due = self.fractional + rate_per_sec as f64 * elapsed.as_secs_f64();
        let whole = due.floor().min(usize::MAX as f64) as usize;
        self.fractional = due - whole as f64;
        whole
    }
}

struct KeyGenerator {
    rng: ChaCha8Rng,
    zipfs: Vec<Zipf<f64>>,
    next_cohort: usize,
    key_space: u32,
    step: u32,
}

impl KeyGenerator {
    fn new(key_space: u32) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(0),
            zipfs: (0..NUM_COHORTS)
                .map(|cohort| Zipf::new(key_space as f64, 1.00 + cohort as f64 * 0.005).unwrap())
                .collect(),
            next_cohort: 0,
            key_space,
            step: (key_space / (NUM_COHORTS as u32 * COHORT_OVERLAP)).max(1),
        }
    }

    fn next(&mut self) -> String {
        let cohort = self.next_cohort;
        self.next_cohort = (self.next_cohort + 1) % NUM_COHORTS;
        let base = (cohort as u32 * self.step) % self.key_space;
        let raw = (self.zipfs[cohort].sample(&mut self.rng) as u32).saturating_sub(1);
        format!("key{}", (raw + base) % self.key_space)
    }
}

/// Spawn the fixed-rate scheduler. Aborting it also cancels its child requests.
pub fn spawn_scheduler(
    client: Arc<Client>,
    nodes: Vec<String>,
    ring: SharedRing,
    target_rps: Arc<AtomicUsize>,
    max_in_flight: usize,
    key_space: u32,
    stats: Arc<WorkloadStats>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let permits = Arc::new(Semaphore::new(max_in_flight));
        let mut reads = JoinSet::new();
        let mut keys = KeyGenerator::new(key_space);
        let mut pacer = ArrivalPacer::default();
        let mut tick = tokio::time::interval(SCHEDULER_TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut last_tick = Instant::now();

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let now = Instant::now();
                    // A suspended or starved host invalidates the measurement;
                    // do not answer it with an enormous catch-up burst.
                    let elapsed = now
                        .saturating_duration_since(last_tick)
                        .min(Duration::from_secs(1));
                    last_tick = now;
                    let scheduled_rate = target_rps.load(Ordering::Relaxed);
                    let due = pacer.arrivals(scheduled_rate, elapsed);
                    for _ in 0..due {
                        let key = keys.next();
                        let current_ring = Arc::clone(&ring.read().unwrap());
                        let placement = current_ring.placement_for(&key);
                        drop(current_ring);

                        let Ok(permit) = permits.clone().try_acquire_owned() else {
                            if target_rps.load(Ordering::Relaxed) == scheduled_rate {
                                stats.record_shed(placement.primary);
                            }
                            continue;
                        };
                        if target_rps.load(Ordering::Relaxed) != scheduled_rate {
                            drop(permit);
                            continue;
                        }
                        let read = stats.begin_admitted_read(placement.primary);
                        let client = client.clone();
                        let l1_url = format!("{}/get/{key}", nodes[placement.primary]);
                        let l2_url = format!("{}/backup/{key}", nodes[placement.backup]);
                        reads.spawn(async move {
                            let started = std::time::Instant::now();
                            let outcome = read_with_fallback(|backup| {
                                attempt_read(&client, if backup { &l2_url } else { &l1_url })
                            }).await;
                            read.finish(outcome, started.elapsed());
                            drop(permit);
                        });
                    }
                }
                result = reads.join_next(), if !reads.is_empty() => {
                    let _ = result;
                }
            }
        }
    })
}

#[cfg(test)]
#[path = "workload_tests.rs"]
mod workload_tests;
