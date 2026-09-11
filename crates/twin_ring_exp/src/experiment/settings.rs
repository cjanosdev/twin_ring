//! Settings for one experiment run.
//!
//! Missing environment variables use defaults. Malformed or inconsistent values
//! fail before the experiment contacts the cluster or creates output files.

use anyhow::{Context, Result, bail, ensure};
use std::str::FromStr;

use super::recovery::RecoveryCriteria;
use super::scenario::Scenario;
use super::topology::NODES;

/// All tunable parameters for one experiment run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExperimentSettings {
    pub scenario: Scenario,
    pub outage_secs: u64,
    /// One-based Docker control API node ID (1, 2, or 3).
    pub outage_node: usize,
    pub recovery: RecoveryCriteria,
    /// Total distinct keys. Preloaded into Cassandra by the `preload` binary.
    pub key_space: u32,
    /// Requests offered per second at regular load across the whole cluster.
    pub regular_rps: usize,

    // ── Warmup ────────────────────────────────────────────────────────────────
    pub warmup_secs: u64,
    pub warmup_start_rps: usize,
    pub warmup_step_rps: usize,
    pub warmup_step_secs: u64,
    // ── Regular work ─────────────────────────────────────────────────────────
    /// Hold regular load before overload; measure the baseline from its tail.
    pub regular_work_secs: u64,
    /// Seconds from the end of regular work used to compute the baseline.
    pub baseline_window_secs: u64,

    // ── Overload        ───────────────────────────────────────────────────────
    /// Includes the ramp to elevated load. All cache nodes remain running.
    pub overload_secs: u64,
    pub overload_rps: usize,
    pub overload_step_rps: usize,
    pub overload_step_secs: u64,

    // ── Observation ───────────────────────────────────────────────────────────
    pub observe_secs: u64,

    // ── Shared ────────────────────────────────────────────────────────────────
    pub poll_interval_secs: u64,
    /// Timeout for one HTTP attempt, including its response body. A timeout
    /// counts as a failed read and does not trigger a backup attempt.
    pub request_timeout_ms: u64,
    /// Safety bound for admitted requests. Excess offered demand is measured as shed.
    pub max_in_flight: usize,
    /// Permitted deviation between measured and configured regular offered rate.
    pub baseline_offered_rps_tolerance: f64,
    /// Member IDs used to build the L1 and L2 consistent-hash rings.
    /// All scenarios keep the same three-member routing and use all entries in `topology::NODES`.
    pub ring_members: Vec<usize>,
}

impl ExperimentSettings {
    pub fn from_env() -> Result<Self> {
        Self::from_env_for(None)
    }

    pub fn from_env_for(scenario: Option<Scenario>) -> Result<Self> {
        Self::from_reader(|key| match (key, scenario) {
            ("TR_SCENARIO", Some(scenario)) => Ok(Some(scenario.as_str().into())),
            _ => match std::env::var(key) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(error) => Err(error).with_context(|| format!("cannot read {key}")),
            },
        })
    }

    // An explicit reader keeps tests independent of process-wide environment
    // variables, so tests can run concurrently without changing one another.
    pub(super) fn from_reader(read: impl Fn(&str) -> Result<Option<String>>) -> Result<Self> {
        for old in [
            "TR_REGULAR_WORKERS",
            "TR_WARMUP_START_WORKERS",
            "TR_WARMUP_RAMP_STEP",
            "TR_OVERLOAD_WORKERS",
            "TR_FAULT_WORKERS",
        ] {
            if read(old)?.is_some() {
                bail!(
                    "{old} configures the removed closed-loop worker model; use the TR_*_RPS settings documented in docs/experiment-scenarios.md"
                );
            }
        }

        let settings = Self {
            scenario: setting(&read, "TR_SCENARIO", Scenario::Overload)?,
            outage_secs: setting(&read, "TR_OUTAGE_SECS", 60u64)?,
            outage_node: setting(&read, "TR_OUTAGE_NODE", 1usize)?,
            recovery: RecoveryCriteria {
                min_successful_rps_ratio: setting(&read, "TR_RECOVERY_MIN_GOODPUT_RATIO", 0.9f64)?,
                max_latency_ratio: setting(&read, "TR_RECOVERY_MAX_LATENCY_RATIO", 2.0f64)?,
                latency_floor_us: setting(&read, "TR_RECOVERY_LATENCY_FLOOR_US", 1000u64)?,
                max_request_error_rate: setting(
                    &read,
                    "TR_RECOVERY_MAX_REQUEST_ERROR_RATE",
                    0.01f64,
                )?,
                max_load_shed_rate: setting(&read, "TR_RECOVERY_MAX_SHED_RATE", 0.01f64)?,
                max_db_error_rate: setting(&read, "TR_RECOVERY_MAX_DB_ERROR_RATE", 0.01f64)?,
                consecutive_windows: setting(&read, "TR_RECOVERY_CONSECUTIVE_WINDOWS", 3usize)?,
            },
            key_space: setting(&read, "TR_KEY_SPACE", 100_000u32)?,
            regular_rps: setting(&read, "TR_REGULAR_RPS", 7_000usize)?,
            warmup_secs: setting(&read, "TR_WARMUP_SECS", 90u64)?,
            warmup_start_rps: setting(&read, "TR_WARMUP_START_RPS", 1_000usize)?,
            warmup_step_rps: setting(&read, "TR_WARMUP_RAMP_STEP_RPS", 1_000usize)?,
            warmup_step_secs: setting(&read, "TR_WARMUP_RAMP_STEP_SECS", 10u64)?,
            regular_work_secs: setting(&read, "TR_REGULAR_WORK_SECS", 90u64)?,
            baseline_window_secs: setting(&read, "TR_BASELINE_WINDOW_SECS", 60u64)?,
            overload_secs: setting_with_alias(
                &read,
                "TR_OVERLOAD_SECS",
                "TR_FAULT_DOWN_SECS",
                90u64,
            )?,
            overload_rps: setting(&read, "TR_OVERLOAD_RPS", 20_000usize)?,
            overload_step_rps: setting(&read, "TR_OVERLOAD_RAMP_STEP_RPS", 1_000usize)?,
            overload_step_secs: setting_with_alias(
                &read,
                "TR_OVERLOAD_RAMP_STEP_SECS",
                "TR_FAULT_RAMP_STEP_SECS",
                3u64,
            )?,
            observe_secs: setting(&read, "TR_OBSERVE_SECS", 180u64)?,
            poll_interval_secs: setting(&read, "TR_POLL_INTERVAL_SECS", 5u64)?,
            request_timeout_ms: setting(&read, "TR_REQUEST_TIMEOUT_MS", 500u64)?,
            max_in_flight: setting(&read, "TR_MAX_IN_FLIGHT", 12_000usize)?,
            baseline_offered_rps_tolerance: setting(
                &read,
                "TR_BASELINE_OFFERED_RPS_TOLERANCE",
                0.05f64,
            )?,
            ring_members: read("TR_RING_MEMBERS")?
                .unwrap_or_else(|| "0,1,2".to_string())
                .split(',')
                .map(|part| parse("TR_RING_MEMBERS", part))
                .collect::<Result<Vec<usize>>>()?,
        };
        settings.validate()?;
        Ok(settings)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            (1..=NODES.len()).contains(&self.outage_node),
            "TR_OUTAGE_NODE must be 1, 2, or 3"
        );
        ensure!(self.outage_secs > 0, "TR_OUTAGE_SECS must be positive");
        for (name, value) in [
            (
                "TR_RECOVERY_MIN_GOODPUT_RATIO",
                self.recovery.min_successful_rps_ratio,
            ),
            (
                "TR_RECOVERY_MAX_REQUEST_ERROR_RATE",
                self.recovery.max_request_error_rate,
            ),
            (
                "TR_RECOVERY_MAX_DB_ERROR_RATE",
                self.recovery.max_db_error_rate,
            ),
            (
                "TR_RECOVERY_MAX_SHED_RATE",
                self.recovery.max_load_shed_rate,
            ),
        ] {
            ensure!(
                value.is_finite() && (0.0..=1.0).contains(&value),
                "{name} must be a finite fraction between 0 and 1"
            );
        }
        ensure!(
            self.recovery.min_successful_rps_ratio > 0.0,
            "TR_RECOVERY_MIN_GOODPUT_RATIO must be positive"
        );
        ensure!(
            self.recovery.max_latency_ratio.is_finite() && self.recovery.max_latency_ratio >= 1.0,
            "TR_RECOVERY_MAX_LATENCY_RATIO must be finite and at least 1"
        );
        ensure!(
            self.recovery.consecutive_windows > 0,
            "TR_RECOVERY_CONSECUTIVE_WINDOWS must be positive"
        );
        ensure!(
            self.recovery.latency_floor_us > 0,
            "TR_RECOVERY_LATENCY_FLOOR_US must be positive"
        );
        ensure!(self.key_space > 0, "TR_KEY_SPACE must be greater than zero");
        ensure!(
            self.regular_rps > 0,
            "TR_REGULAR_RPS must be greater than zero"
        );
        ensure!(
            self.warmup_start_rps > 0 && self.warmup_start_rps <= self.regular_rps,
            "TR_WARMUP_START_RPS must be between 1 and TR_REGULAR_RPS"
        );
        ensure!(
            self.warmup_step_rps > 0,
            "TR_WARMUP_RAMP_STEP_RPS must be greater than zero"
        );
        ensure!(
            self.max_in_flight > 0,
            "TR_MAX_IN_FLIGHT must be greater than zero"
        );
        ensure!(
            self.baseline_offered_rps_tolerance.is_finite()
                && (0.0..1.0).contains(&self.baseline_offered_rps_tolerance),
            "TR_BASELINE_OFFERED_RPS_TOLERANCE must be a finite fraction between 0 and 1"
        );
        for (name, value) in [
            ("TR_WARMUP_SECS", self.warmup_secs),
            ("TR_WARMUP_RAMP_STEP_SECS", self.warmup_step_secs),
            ("TR_REGULAR_WORK_SECS", self.regular_work_secs),
            ("TR_BASELINE_WINDOW_SECS", self.baseline_window_secs),
            ("TR_OVERLOAD_SECS", self.overload_secs),
            ("TR_OVERLOAD_RAMP_STEP_SECS", self.overload_step_secs),
            ("TR_OBSERVE_SECS", self.observe_secs),
            ("TR_POLL_INTERVAL_SECS", self.poll_interval_secs),
            ("TR_REQUEST_TIMEOUT_MS", self.request_timeout_ms),
        ] {
            ensure!(value > 0, "{name} must be greater than zero");
        }
        let ramp_secs = self.warmup_ramp_secs()?;
        ensure!(
            self.warmup_secs >= ramp_secs,
            "TR_WARMUP_SECS is {}s but the ramp needs at least {ramp_secs}s to reach {} requests/s",
            self.warmup_secs,
            self.regular_rps
        );
        ensure!(
            u64::try_from(self.recovery.consecutive_windows)
                .ok()
                .and_then(|count| count.checked_mul(self.poll_interval_secs))
                .is_some_and(|minimum| self.baseline_window_secs > minimum),
            "TR_BASELINE_WINDOW_SECS must exceed TR_RECOVERY_CONSECUTIVE_WINDOWS polling intervals to fit complete windows"
        );
        ensure!(
            self.baseline_window_secs
                .checked_add(self.poll_interval_secs)
                .is_some_and(|minimum| self.regular_work_secs >= minimum),
            "TR_REGULAR_WORK_SECS must leave at least one polling interval before the baseline tail"
        );
        {
            ensure!(
                self.overload_secs > self.overload_step_secs,
                "TR_OVERLOAD_SECS must exceed TR_OVERLOAD_RAMP_STEP_SECS so elevated load actually runs"
            );
            ensure!(
                self.overload_rps > self.regular_rps,
                "TR_OVERLOAD_RPS must exceed TR_REGULAR_RPS to create elevated load"
            );
            ensure!(
                self.overload_step_rps > 0,
                "TR_OVERLOAD_RAMP_STEP_RPS must be greater than zero"
            );
        }
        if self.scenario == Scenario::NodeOutage {
            let steps = (self.overload_rps - self.regular_rps).div_ceil(self.overload_step_rps);
            let ramp_secs = u64::try_from(steps)
                .ok()
                .and_then(|steps| steps.checked_mul(self.overload_step_secs))
                .context("overload ramp duration is too large")?;
            ensure!(
                self.overload_secs > ramp_secs,
                "TR_OVERLOAD_SECS must exceed the {ramp_secs}s ramp so elevated load runs before stopping the node"
            );
        }
        let mut members = self.ring_members.clone();
        members.sort_unstable();
        members.dedup();
        ensure!(
            members.len() == NODES.len(),
            "TR_RING_MEMBERS must contain all three server indexes for all experiment scenarios"
        );
        ensure!(
            members.len() == self.ring_members.len(),
            "TR_RING_MEMBERS must not contain duplicate server indexes"
        );
        ensure!(
            members.iter().all(|member| *member < NODES.len()),
            "TR_RING_MEMBERS contains a server index without a URL in topology::NODES"
        );
        Ok(())
    }

    /// Time needed to reach regular load. A partial final step still takes one
    /// full interval.
    pub fn warmup_ramp_secs(&self) -> Result<u64> {
        ensure!(
            self.warmup_step_rps > 0,
            "TR_WARMUP_RAMP_STEP_RPS must be greater than zero"
        );
        let steps = self
            .regular_rps
            .saturating_sub(self.warmup_start_rps)
            .div_ceil(self.warmup_step_rps);
        u64::try_from(steps)
            .ok()
            .and_then(|steps| steps.checked_mul(self.warmup_step_secs))
            .context("warmup ramp duration is too large")
    }

    /// A poll normally takes one sleep interval plus at most a bounded HTTP
    /// timeout. Longer rounds break the consecutive-window recovery evidence.
    pub fn max_measurement_secs(&self) -> f64 {
        self.poll_secs() * 3.0
    }

    pub fn poll_secs(&self) -> f64 {
        self.poll_interval_secs as f64
    }
}

fn setting<T: FromStr>(
    read: &impl Fn(&str) -> Result<Option<String>>,
    key: &str,
    default: T,
) -> Result<T> {
    match read(key)? {
        Some(value) => parse(key, &value),
        None => Ok(default),
    }
}

// New overload names take precedence; old dashboard/shell names still work.
fn setting_with_alias<T: FromStr>(
    read: &impl Fn(&str) -> Result<Option<String>>,
    key: &str,
    alias: &str,
    default: T,
) -> Result<T> {
    match read(key)? {
        Some(value) => parse(key, &value),
        None => setting(read, alias, default),
    }
}

fn parse<T: FromStr>(key: &str, value: &str) -> Result<T> {
    match value.trim().parse() {
        Ok(parsed) => Ok(parsed),
        Err(_) => bail!("invalid {key}: {value:?}"),
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod settings_tests;
