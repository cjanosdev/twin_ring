//! Run reports and charts. The sidecar stores measurements and assessments so
//! charts can display the same recovery decisions without reimplementing them.
use super::outage::OutageEvents;
use super::recovery::{RecoveryTracker, RunOutcome};
use super::scenario::Scenario;
use super::settings::ExperimentSettings;
use crate::measurements::MeasurementRound;
use anyhow::Result;
use serde_json::json;
use std::io::{BufWriter, Write};
use std::path::Path;

pub fn print_window(w: &MeasurementRound) {
    let goodput = w.client.total.successful as f64 / w.duration_secs;
    let offered = w.client.total.offered as f64 / w.duration_secs;
    let admitted = w.client.total.admitted as f64 / w.duration_secs;
    let latency = w
        .client
        .total
        .success_latency_p95_us
        .map_or_else(|| "unobserved".into(), |v| format!("{v}µs"));
    println!(
        "  [{}] offered_rps={offered:.0} admitted_rps={admitted:.0} successful_rps={goodput:.0} client_p95={latency} request_errors={} shed={} in_flight={} missing_nodes={}",
        w.phase,
        w.client.total.http_errors + w.client.total.transport_errors,
        w.client.total.shed,
        w.client.in_flight,
        w.missing_nodes.len()
    );
}

pub struct Summary<'a> {
    pub strategy: &'a str,
    pub tracker: &'a RecoveryTracker,
    pub settings: &'a ExperimentSettings,
    pub recovery_started_ms: u128,
    pub regular_load_restored_ms: Option<u128>,
    pub outage_events: &'a OutageEvents,
    pub outcome: RunOutcome,
    pub measurements: &'a [MeasurementRound],
}

pub fn write_summary(path: &Path, s: &Summary<'_>) -> Result<()> {
    let summary = summary_value(s);
    std::fs::write(path, serde_json::to_string_pretty(&summary)?)?;
    Ok(())
}

pub(super) fn summary_value(s: &Summary<'_>) -> serde_json::Value {
    json!({
        "protocol": s.settings.scenario.protocol(),
        "scenario": s.settings.scenario,
        "recovery_evaluation": "sustained_cluster_v6",
        "workload_model": "fixed_arrival_rate_v1",
        "baseline_validation": "complete_fixed_rate_regular_work_and_basic_service_health_v2",
        "recovery_threshold_reference": "observed_regular_work_bounds_v1",
        "run_status": "completed",
        "settings": s.settings,
        "client_fallback_policy": "non_timeout_connection_errors_only_v1",
        "node_failure_injected": s.outage_events.stop_acknowledged_ms.is_some(),
        "outage_node": (s.settings.scenario != Scenario::Overload).then_some(s.settings.outage_node),
        "outage_secs": (s.settings.scenario != Scenario::Overload).then_some(s.settings.outage_secs),
        "outage_events": s.outage_events,
        "routing_membership_changed": false,
        "strategy": s.strategy,
        "outcome": s.outcome,
        "outcome_description": s.tracker.outcome_description(s.outcome),
        "baseline_usable_for_verdict": true,
        "recovery_criteria": s.tracker.criteria,
        "baseline": s.tracker.baseline,
        "baseline_successful_rps": s.tracker.baseline.successful_rps,
        "baseline_client_p95_us": s.tracker.baseline.client_p95_us,
        "baseline_hit_rate": s.tracker.baseline.hit_rate,
        "recovery_started_ms": s.recovery_started_ms,
        "recovery_reference": if s.settings.scenario != Scenario::Overload { "restart_acknowledged" } else { "regular_load_restored" },
        "regular_load_restored_ms": s.regular_load_restored_ms,
        "regular_rps": s.settings.regular_rps,
        "overload_rps": s.settings.overload_rps,
        "warmup_secs": s.settings.warmup_secs,
        "regular_work_secs": s.settings.regular_work_secs,
        "overload_secs": s.settings.overload_secs,
        "observe_secs": s.settings.observe_secs,
        "poll_interval_secs": s.settings.poll_interval_secs,
        "baseline_window_secs": s.settings.baseline_window_secs,
        "request_timeout_ms": s.settings.request_timeout_ms,
        "time_to_recovery_secs": s.tracker.time_to_recovery_secs(),
        "first_recovered_ms": s.tracker.first_recovered_ms,
        "first_degraded_ms": s.tracker.first_degraded_ms,
        "relapse_times_ms": s.tracker.relapse_times_ms,
        "assessments": s.tracker.assessments,
        "measurements": s.measurements,
    })
}

/// Flush every complete round so errors or cancellation cannot discard the
/// client counters and latency measurements that the node-only CSV omits.
pub struct MeasurementJournal(BufWriter<std::fs::File>);

impl MeasurementJournal {
    pub fn new(path: &Path, strategy: &str, settings: &ExperimentSettings) -> Result<Self> {
        let mut log = Self(BufWriter::new(std::fs::File::create(path)?));
        log.append(&json!({
            "type": "run", "strategy": strategy, "settings": settings,
            "recovery_evaluation": "sustained_cluster_v6",
            "workload_model": "fixed_arrival_rate_v1",
        }))?;
        Ok(log)
    }

    fn append(&mut self, value: &impl serde::Serialize) -> Result<()> {
        serde_json::to_writer(&mut self.0, value)?;
        self.0.write_all(b"\n")?;
        self.0.flush()?;
        Ok(())
    }

    pub fn write(&mut self, round: &MeasurementRound) -> Result<()> {
        self.append(&json!({ "type": "measurement", "measurement": round }))
    }
}

pub fn write_failed_summary(
    path: &Path,
    strategy: &str,
    settings: &ExperimentSettings,
    outage_events: &OutageEvents,
    measurements: &[MeasurementRound],
    error: &anyhow::Error,
) -> Result<()> {
    let summary = json!({
        "protocol": settings.scenario.protocol(),
        "scenario": settings.scenario,
        "strategy": strategy,
        "recovery_evaluation": "sustained_cluster_v6",
        "workload_model": "fixed_arrival_rate_v1",
        "run_status": "aborted",
        "outcome": "inconclusive",
        "outcome_description": format!("Experiment aborted: {error:#}"),
        "error": format!("{error:#}"),
        "settings": settings,
        "recovery_criteria": settings.recovery,
        "outage_events": outage_events,
        "time_to_recovery_secs": null,
        "measurements": measurements,
    });
    std::fs::write(path, serde_json::to_string_pretty(&summary)?)?;
    Ok(())
}

/// Shell out to `charts/chart.mjs` to render this run's CSV. Best-effort:
/// a missing node install or chart tool is reported but does not fail the run.
pub fn generate_charts(strategy: &str, csv_path: &Path) {
    let charts_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent()) // workspace root
        .expect("workspace root")
        .join("charts");

    if !charts_dir.join("chart.mjs").exists() {
        println!("  ℹ charts/chart.mjs not found — skipping chart generation");
        return;
    }

    println!("\n📈 [{strategy}] Generating charts...");
    match std::process::Command::new("node")
        .arg(charts_dir.join("chart.mjs"))
        .arg("--csv")
        .arg(csv_path)
        .status()
    {
        Ok(s) if s.success() => {}
        Ok(s) => println!("  ⚠ chart tool exited with status {s}"),
        Err(e) => println!("  ⚠ could not run chart tool (is node installed?): {e}"),
    }
}
