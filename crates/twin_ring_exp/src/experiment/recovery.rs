//! Sustained recovery of successful service across all cache nodes.
//! Thresholds are experimental starting values, saved with each run.

use super::scenario::Scenario;
use super::stats::median_u64;
use super::topology::NODES;
use crate::measurements::MeasurementRound;
use anyhow::{Result, ensure};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct RecoveryCriteria {
    pub min_successful_rps_ratio: f64,
    pub max_latency_ratio: f64,
    pub latency_floor_us: u64,
    pub max_request_error_rate: f64,
    pub max_load_shed_rate: f64,
    pub max_db_error_rate: f64,
    pub consecutive_windows: usize,
}

impl Default for RecoveryCriteria {
    fn default() -> Self {
        Self {
            min_successful_rps_ratio: 0.9,
            max_latency_ratio: 2.0,
            latency_floor_us: 1_000,
            max_request_error_rate: 0.01,
            max_load_shed_rate: 0.01,
            max_db_error_rate: 0.01,
            consecutive_windows: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeBaseline {
    pub node: String,
    /// Duration-weighted mean, retained to describe typical regular service.
    pub successful_rps: f64,
    /// Lowest successful-read rate observed in a valid regular-work window.
    pub successful_rps_lower_bound: f64,
    /// Median of the regular-work windows' client p95 values.
    pub client_p95_us: f64,
    /// Highest client p95 observed in a valid regular-work window.
    pub client_p95_upper_bound_us: f64,
    /// Median of this node's db p99 values, when DB calls were observed.
    pub db_p99_us: Option<f64>,
    /// Highest DB p99 observed in a valid regular-work window.
    pub db_p99_upper_bound_us: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Baseline {
    pub nodes: Vec<NodeBaseline>,
    pub successful_rps: f64,
    pub client_p95_us: f64,
    pub hit_rate: f64,
    pub windows: usize,
}

fn quality_problem(
    round: &MeasurementRound,
    max_window_secs: f64,
    expected_missing: Option<&str>,
) -> Option<String> {
    if !round.duration_secs.is_finite()
        || round.duration_secs <= 0.0
        || round.duration_secs > max_window_secs
    {
        return Some("measurement interval missing or too long".into());
    }
    let expected_nodes: Vec<_> = NODES
        .iter()
        .copied()
        .filter(|node| Some(*node) != expected_missing)
        .collect();
    let missing_is_expected = match expected_missing {
        Some(node) => round.missing_nodes.len() == 1 && round.missing_nodes[0] == node,
        None => round.missing_nodes.is_empty(),
    };
    if !missing_is_expected
        || round.nodes.len() != expected_nodes.len()
        || round.client.by_primary.len() != NODES.len()
    {
        return Some("missing node measurements do not match this scenario phase".into());
    }
    if !expected_nodes
        .iter()
        .all(|node| round.nodes.iter().filter(|w| w.node == *node).count() == 1)
    {
        return Some("node measurements duplicated or unidentified".into());
    }
    if (expected_missing.is_none() && !round.aligned)
        || round
            .unaligned_nodes
            .iter()
            .any(|node| Some(node.as_str()) != expected_missing)
    {
        return Some("node counters are not aligned after a failed poll".into());
    }
    None
}

fn db_calls(w: &crate::measurements::NodeWindow) -> u64 {
    w.db_hits + w.db_not_found + w.db_errors
}

impl Baseline {
    pub fn from_regular_tail(
        rounds: &[MeasurementRound],
        ended_ms: u128,
        tail_secs: u64,
        criteria: &RecoveryCriteria,
        max_window_secs: f64,
    ) -> Result<Self> {
        Self::from_regular_tail_inner(rounds, ended_ms, tail_secs, criteria, max_window_secs, None)
    }

    pub fn from_regular_tail_at_rate(
        rounds: &[MeasurementRound],
        ended_ms: u128,
        tail_secs: u64,
        criteria: &RecoveryCriteria,
        max_window_secs: f64,
        expected_offered_rps: usize,
        offered_rps_tolerance: f64,
    ) -> Result<Self> {
        Self::from_regular_tail_inner(
            rounds,
            ended_ms,
            tail_secs,
            criteria,
            max_window_secs,
            Some((expected_offered_rps, offered_rps_tolerance)),
        )
    }

    fn from_regular_tail_inner(
        rounds: &[MeasurementRound],
        ended_ms: u128,
        tail_secs: u64,
        criteria: &RecoveryCriteria,
        max_window_secs: f64,
        expected_load: Option<(usize, f64)>,
    ) -> Result<Self> {
        let cutoff = ended_ms.saturating_sub(u128::from(tail_secs) * 1000);
        // A round must be entirely in the requested tail. Do not shorten a
        // window's duration while retaining all of its counters.
        let tail: Vec<_> = rounds
            .iter()
            .filter(|r| {
                r.phase == "regular_work" && r.started_ms >= cutoff && r.timestamp_ms <= ended_ms
            })
            .collect();
        ensure!(
            tail.len() >= criteria.consecutive_windows,
            "regular-work baseline needs at least {} complete windows in its tail",
            criteria.consecutive_windows
        );
        for (i, round) in tail.iter().enumerate() {
            ensure!(
                round.within_one_phase(),
                "baseline tail includes a phase transition"
            );
            if let Some(problem) = quality_problem(round, max_window_secs, None) {
                anyhow::bail!(
                    "baseline tail measurement {} is invalid: {problem}",
                    round.sequence
                );
            }
            if i > 0 {
                ensure!(
                    round.sequence == tail[i - 1].sequence + 1,
                    "baseline tail has a gap"
                );
            }
            if let Some((expected_rps, tolerance)) = expected_load {
                let measured_rps = round.client.total.offered as f64 / round.duration_secs;
                let lower = expected_rps as f64 * (1.0 - tolerance);
                let upper = expected_rps as f64 * (1.0 + tolerance);
                ensure!(
                    (lower..=upper).contains(&measured_rps),
                    "baseline offered rate was {measured_rps:.1}/s; expected {expected_rps}/s within ±{:.1}%",
                    tolerance * 100.0
                );
            }
        }
        let elapsed: f64 = tail.iter().map(|r| r.duration_secs).sum();
        let mut nodes = Vec::new();
        for (index, node) in NODES.iter().enumerate() {
            let mut successful = 0u64;
            let mut request_errors = 0u64;
            let mut offered = 0u64;
            let mut shed = 0u64;
            let mut successful_rates = Vec::new();
            let mut latencies = Vec::new();
            let mut db_latencies = Vec::new();
            let mut database_calls = 0u64;
            let mut database_errors = 0u64;
            for round in &tail {
                let requests = &round.client.by_primary[index];
                ensure!(
                    requests.successful > 0,
                    "baseline has no successful reads for {node}"
                );
                successful += requests.successful;
                request_errors += requests.http_errors + requests.transport_errors;
                offered += requests.offered;
                shed += requests.shed;
                successful_rates.push(requests.successful as f64 / round.duration_secs);
                latencies.push(
                    requests
                        .success_latency_p95_us
                        .filter(|v| *v > 0)
                        .ok_or_else(|| {
                            anyhow::anyhow!("baseline client latency missing for {node}")
                        })?,
                );
                let w = round.nodes.iter().find(|w| w.node == *node).unwrap();
                let calls = db_calls(w);
                database_calls += calls;
                database_errors += w.db_errors;
                if calls > 0 {
                    ensure!(
                        w.db_p99_us > 0,
                        "baseline database latency missing for {node}"
                    );
                    db_latencies.push(w.db_p99_us);
                }
            }
            let completed_requests = successful + request_errors;
            let request_error_rate = request_errors as f64 / completed_requests as f64;
            ensure!(
                request_error_rate <= criteria.max_request_error_rate,
                "baseline request error rate for {node} was {:.3}% ({request_errors} errors across {completed_requests} completed reads); maximum allowed is {:.3}%",
                request_error_rate * 100.0,
                criteria.max_request_error_rate * 100.0
            );
            let shed_rate = if offered > 0 {
                shed as f64 / offered as f64
            } else {
                1.0
            };
            ensure!(
                shed_rate <= criteria.max_load_shed_rate,
                "baseline shed rate for {node} was {:.3}% ({shed} shed across {offered} offered reads); maximum allowed is {:.3}%",
                shed_rate * 100.0,
                criteria.max_load_shed_rate * 100.0
            );
            if database_calls > 0 {
                let database_error_rate = database_errors as f64 / database_calls as f64;
                ensure!(
                    database_error_rate <= criteria.max_db_error_rate,
                    "baseline database error rate for {node} was {:.3}% ({database_errors} errors across {database_calls} database calls); maximum allowed is {:.3}%",
                    database_error_rate * 100.0,
                    criteria.max_db_error_rate * 100.0
                );
            }
            nodes.push(NodeBaseline {
                node: node.to_string(),
                successful_rps: successful as f64 / elapsed,
                successful_rps_lower_bound: successful_rates
                    .into_iter()
                    .reduce(f64::min)
                    .expect("baseline contains complete windows"),
                client_p95_us: median_u64(&latencies),
                client_p95_upper_bound_us: *latencies
                    .iter()
                    .max()
                    .expect("baseline contains client latency")
                    as f64,
                db_p99_us: (!db_latencies.is_empty()).then(|| median_u64(&db_latencies)),
                db_p99_upper_bound_us: db_latencies.iter().max().copied().map(|v| v as f64),
            });
        }
        let hits: u64 = tail.iter().flat_map(|r| &r.nodes).map(|w| w.hits).sum();
        let attempts: u64 = tail
            .iter()
            .flat_map(|r| &r.nodes)
            .map(|w| w.hits + w.misses)
            .sum();
        Ok(Self {
            successful_rps: nodes.iter().map(|n| n.successful_rps).sum(),
            nodes,
            client_p95_us: median_u64(
                &tail
                    .iter()
                    .filter_map(|r| r.client.total.success_latency_p95_us)
                    .collect::<Vec<_>>(),
            ),
            hit_rate: if attempts > 0 {
                hits as f64 / attempts as f64
            } else {
                0.0
            },
            windows: tail.len(),
        })
    }
}

fn health_failures(
    round: &MeasurementRound,
    baseline: &Baseline,
    criteria: &RecoveryCriteria,
) -> Vec<String> {
    let mut reasons = Vec::new();
    for (index, base) in baseline.nodes.iter().enumerate() {
        let requests = &round.client.by_primary[index];
        let goodput = requests.successful as f64 / round.duration_secs;
        if goodput < base.successful_rps_lower_bound * criteria.min_successful_rps_ratio {
            reasons.push(format!(
                "{}: successful-read rate {:.1}/s is below {:.1}/s ({:.1}% of the regular-work lower bound {:.1}/s)",
                base.node, goodput,
                base.successful_rps_lower_bound * criteria.min_successful_rps_ratio,
                100.0 * criteria.min_successful_rps_ratio,
                base.successful_rps_lower_bound
            ));
        }
        if requests
            .error_rate()
            .is_none_or(|v| v > criteria.max_request_error_rate)
        {
            reasons.push(format!(
                "{}: request errors above limit or no completed reads",
                base.node
            ));
        }
        if requests
            .shed_rate()
            .is_none_or(|v| v > criteria.max_load_shed_rate)
        {
            reasons.push(format!(
                "{}: offered-load shed rate above limit or no offered reads",
                base.node
            ));
        }
        if requests.success_latency_p95_us.is_none_or(|v| {
            v as f64
                > base
                    .client_p95_upper_bound_us
                    .max(criteria.latency_floor_us as f64)
                    * criteria.max_latency_ratio
        }) {
            reasons.push(format!(
                "{}: successful-read latency above limit or unobserved",
                base.node
            ));
        }
        let Some(w) = round.nodes.iter().find(|w| w.node == base.node) else {
            // Only the deliberately stopped node may be absent here. Client
            // outcomes for its primary keys were still evaluated above.
            continue;
        };
        let calls = db_calls(w);
        if calls > 0 {
            if w.db_errors as f64 / calls as f64 > criteria.max_db_error_rate {
                reasons.push(format!("{}: database error rate above limit", base.node));
            }
            if let Some(db_base) = base.db_p99_upper_bound_us {
                if w.db_p99_us as f64
                    > db_base.max(criteria.latency_floor_us as f64) * criteria.max_latency_ratio
                {
                    reasons.push(format!(
                        "{}: database latency above baseline tolerance",
                        base.node
                    ));
                }
            }
        }
        // Zero DB calls leave DB latency unobserved. Successful service can
        // still recover through cache hits; we do not claim DB latency is zero.
    }
    reasons
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowState {
    ExpectedOutage,
    Transition,
    Unknown,
    Healthy,
    Recovering,
    Recovered,
    Degraded,
}

#[derive(Debug, Clone, Serialize)]
pub struct Assessment {
    pub sequence: u64,
    pub timestamp_ms: u128,
    pub phase: String,
    pub state: WindowState,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    ResistedDegradation,
    Recovered,
    RecoveredThenRelapsed,
    DidNotRecoverWithinObservation,
    Inconclusive,
}

impl RunOutcome {
    pub fn label(self) -> &'static str {
        match self {
            Self::ResistedDegradation => "No degradation observed during disruption or observation",
            Self::Recovered => "Recovered to regular service",
            Self::RecoveredThenRelapsed => "Recovered, then relapsed before observation ended",
            Self::DidNotRecoverWithinObservation => "Did not establish recovery within observation",
            Self::Inconclusive => "Inconclusive: baseline or measurement evidence is insufficient",
        }
    }
}

pub struct RecoveryTracker {
    pub baseline: Baseline,
    pub criteria: RecoveryCriteria,
    max_window_secs: f64,
    scenario: Scenario,
    outage_node: Option<String>,
    restored_ms: Option<u128>,
    streak: usize,
    currently_recovered: bool,
    relapse_open: bool,
    previous_sequence: Option<u64>,
    last_observation_ms: Option<u128>,
    pub first_recovered_ms: Option<u128>,
    pub first_degraded_ms: Option<u128>,
    pub relapse_times_ms: Vec<u128>,
    pub assessments: Vec<Assessment>,
    overload_valid_windows: usize,
    outage_valid_windows: usize,
    unknown_windows: usize,
    degradation_observed: bool,
    expected_observation_rps: Option<usize>,
    offered_rps_tolerance: f64,
}

impl RecoveryTracker {
    pub fn new(baseline: Baseline, criteria: RecoveryCriteria, max_window_secs: f64) -> Self {
        Self {
            baseline,
            criteria,
            max_window_secs,
            scenario: Scenario::Overload,
            outage_node: None,
            restored_ms: None,
            streak: 0,
            currently_recovered: false,
            relapse_open: false,
            previous_sequence: None,
            last_observation_ms: None,
            first_recovered_ms: None,
            first_degraded_ms: None,
            relapse_times_ms: Vec::new(),
            assessments: Vec::new(),
            overload_valid_windows: 0,
            outage_valid_windows: 0,
            unknown_windows: 0,
            degradation_observed: false,
            expected_observation_rps: None,
            offered_rps_tolerance: 0.0,
        }
    }

    pub fn for_scenario(
        baseline: Baseline,
        criteria: RecoveryCriteria,
        max_window_secs: f64,
        scenario: Scenario,
        outage_node: String,
    ) -> Self {
        let mut tracker = Self::new(baseline, criteria, max_window_secs);
        tracker.scenario = scenario;
        tracker.outage_node = (scenario != Scenario::Overload).then_some(outage_node);
        tracker
    }

    pub fn begin_observation(&mut self, restored_ms: u128) {
        self.restored_ms = Some(restored_ms);
        self.streak = 0;
        self.currently_recovered = false;
        self.previous_sequence = None;
    }

    pub fn expect_observation_rate(&mut self, offered_rps: usize, tolerance: f64) {
        self.expected_observation_rps = Some(offered_rps);
        self.offered_rps_tolerance = tolerance;
    }

    pub fn observe(&mut self, round: &MeasurementRound) {
        let during_disruption = match round.phase.as_str() {
            "overload" => true,
            "node_outage" => self.scenario != Scenario::Overload,
            _ => false,
        };
        if !during_disruption && round.phase != "observe" {
            return;
        }
        if round.phase == "observe"
            && self
                .restored_ms
                .is_none_or(|start| round.timestamp_ms < start)
        {
            return;
        }
        let gap = self
            .previous_sequence
            .is_some_and(|seq| round.sequence != seq + 1);
        self.previous_sequence = Some(round.sequence);
        if round.phase == "observe" {
            self.last_observation_ms = Some(round.timestamp_ms);
        }
        let mut reasons = Vec::new();
        let mut state;
        let expected_missing = if round.phase == "node_outage" {
            self.outage_node.as_deref()
        } else {
            None
        };
        if let Some(problem) = quality_problem(round, self.max_window_secs, expected_missing) {
            state = WindowState::Unknown;
            reasons.push(problem);
        } else if !round.within_one_phase() {
            state = WindowState::Transition;
            reasons.push("window spans a phase change or includes earlier-phase requests".into());
        } else if gap {
            state = WindowState::Unknown;
            reasons.push("gap or repeated measurement sequence".into());
        } else if round
            .nodes
            .iter()
            .any(|w| db_calls(w) > 0 && w.db_p99_us == 0)
        {
            state = WindowState::Unknown;
            reasons.push("database calls measured without latency samples".into());
        } else if self.baseline.nodes.iter().any(|base| {
            base.db_p99_us.is_none()
                && round
                    .nodes
                    .iter()
                    .any(|w| w.node == base.node && db_calls(w) > 0)
        }) {
            state = WindowState::Unknown;
            reasons.push("database calls have no measured baseline latency for comparison".into());
        } else if round
            .client
            .by_primary
            .iter()
            .any(|w| w.successful > 0 && w.success_latency_p95_us.is_none_or(|v| v == 0))
        {
            state = WindowState::Unknown;
            reasons.push("successful reads measured without latency samples".into());
        } else if round.phase == "observe"
            && self.expected_observation_rps.is_some_and(|expected| {
                let measured = round.client.total.offered as f64 / round.duration_secs;
                let lower = expected as f64 * (1.0 - self.offered_rps_tolerance);
                let upper = expected as f64 * (1.0 + self.offered_rps_tolerance);
                !(lower..=upper).contains(&measured)
            })
        {
            state = WindowState::Unknown;
            reasons
                .push("load generator did not maintain the configured regular offered rate".into());
        } else {
            reasons = health_failures(round, &self.baseline, &self.criteria);
            state = if reasons.is_empty() {
                if expected_missing.is_some() {
                    WindowState::ExpectedOutage
                } else {
                    WindowState::Healthy
                }
            } else {
                WindowState::Degraded
            };
            if round.phase == "overload" {
                self.overload_valid_windows += 1;
            }
            if round.phase == "node_outage" {
                self.outage_valid_windows += 1;
            }
        }
        if state == WindowState::Unknown {
            self.unknown_windows += 1;
        }
        if state == WindowState::Degraded {
            self.degradation_observed = true;
            self.first_degraded_ms.get_or_insert(round.timestamp_ms);
        }
        if round.phase == "observe" {
            match state {
                WindowState::Healthy => {
                    self.streak += 1;
                    if self.streak >= self.criteria.consecutive_windows {
                        self.currently_recovered = true;
                        self.relapse_open = false;
                        if self.degradation_observed {
                            self.first_recovered_ms.get_or_insert(round.timestamp_ms);
                        }
                        state = WindowState::Recovered;
                    } else {
                        state = WindowState::Recovering;
                    }
                }
                _ => {
                    if state == WindowState::Degraded
                        && self.first_recovered_ms.is_some()
                        && !self.relapse_open
                    {
                        self.relapse_times_ms.push(round.timestamp_ms);
                        self.relapse_open = true;
                    }
                    self.streak = 0;
                    self.currently_recovered = false;
                }
            }
        }
        self.assessments.push(Assessment {
            sequence: round.sequence,
            timestamp_ms: round.timestamp_ms,
            phase: round.phase.clone(),
            state,
            reasons,
        });
    }

    /// Stale final telemetry prevents an old healthy streak from winning.
    pub fn outcome(&self, ended_ms: u128) -> RunOutcome {
        let last = self.assessments.last().filter(|a| a.phase == "observe");
        if self
            .last_observation_ms
            .is_none_or(|ts| ended_ms.saturating_sub(ts) as f64 > self.max_window_secs * 1000.0)
            || last
                .is_none_or(|a| matches!(a.state, WindowState::Unknown | WindowState::Transition))
        {
            return RunOutcome::Inconclusive;
        }
        if self.currently_recovered {
            if self.degradation_observed {
                RunOutcome::Recovered
            } else if self.unknown_windows == 0
                && match self.scenario {
                    Scenario::Overload => {
                        self.overload_valid_windows >= self.criteria.consecutive_windows
                    }
                    Scenario::NodeOutage => {
                        self.overload_valid_windows >= self.criteria.consecutive_windows
                            && self.outage_valid_windows >= self.criteria.consecutive_windows
                    }
                }
            {
                RunOutcome::ResistedDegradation
            } else {
                RunOutcome::Inconclusive
            }
        } else if self.first_recovered_ms.is_some() && !self.relapse_times_ms.is_empty() {
            RunOutcome::RecoveredThenRelapsed
        } else if self.first_recovered_ms.is_some() {
            RunOutcome::Inconclusive
        } else if self.degradation_observed {
            RunOutcome::DidNotRecoverWithinObservation
        } else {
            RunOutcome::Inconclusive
        }
    }

    pub fn outcome_description(&self, outcome: RunOutcome) -> &'static str {
        outcome.label()
    }

    pub fn time_to_recovery_secs(&self) -> Option<f64> {
        self.first_recovered_ms
            .zip(self.restored_ms)
            .map(|(end, start)| end.saturating_sub(start) as f64 / 1000.0)
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod recovery_tests;
