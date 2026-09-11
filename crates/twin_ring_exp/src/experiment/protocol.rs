//! Warmup -> regular baseline -> selected disruption -> recovery at regular load.
//! Each strategy runs separately. Routing membership stays fixed in all scenarios.

use anyhow::{Result, ensure};
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};

use super::cluster;
use super::outage::{HttpNodeControl, NodeControl, NodeOutage};
use super::output::{
    MeasurementJournal, Summary, generate_charts, print_window, write_failed_summary, write_summary,
};
use super::recovery::{Baseline, RecoveryTracker};
use super::scenario::Scenario;
use super::settings::ExperimentSettings;
use super::topology::NODES;
use super::workload::{WorkloadStats, shared_ring, spawn_scheduler};
use crate::measurements::{MeasurementRound, MetricsWriter, StatsPoller};
use twin_ring_core::experiment_path::results_path_prefix;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Warmup,
    RegularWork,
    Overload,
    StoppingNode,
    NodeOutage,
    RestartingNode,
    Observe,
}

impl Phase {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::RegularWork => "regular_work",
            Self::Overload => "overload",
            Self::StoppingNode => "stopping_node",
            Self::NodeOutage => "node_outage",
            Self::RestartingNode => "restarting_node",
            Self::Observe => "observe",
        }
    }
}

/// One phase owns its offered-rate schedule; there is no background ramp task.
#[derive(Clone, Copy, Debug)]
struct PhasePlan {
    phase: Phase,
    duration: Duration,
    initial_rps: usize,
    target_rps: usize,
    step_rps: usize,
    step_secs: u64,
}

impl PhasePlan {
    fn rate_at(self, elapsed: Duration) -> usize {
        let steps = usize::try_from(elapsed.as_secs() / self.step_secs).unwrap_or(usize::MAX);
        self.initial_rps
            .saturating_add(steps.saturating_mul(self.step_rps))
            .min(self.target_rps)
    }
}

fn phase_plans(cfg: &ExperimentSettings) -> [PhasePlan; 4] {
    let regular = PhasePlan {
        phase: Phase::RegularWork,
        duration: Duration::from_secs(cfg.regular_work_secs),
        initial_rps: cfg.regular_rps,
        target_rps: cfg.regular_rps,
        step_rps: 0,
        step_secs: 1,
    };
    [
        PhasePlan {
            phase: Phase::Warmup,
            duration: Duration::from_secs(cfg.warmup_secs),
            initial_rps: cfg.warmup_start_rps,
            target_rps: cfg.regular_rps,
            step_rps: cfg.warmup_step_rps,
            step_secs: cfg.warmup_step_secs,
        },
        regular,
        PhasePlan {
            phase: Phase::Overload,
            duration: Duration::from_secs(cfg.overload_secs),
            initial_rps: cfg.regular_rps,
            target_rps: cfg.overload_rps,
            step_rps: cfg.overload_step_rps,
            step_secs: cfg.overload_step_secs,
        },
        PhasePlan {
            phase: Phase::Observe,
            duration: Duration::from_secs(cfg.observe_secs),
            ..regular
        },
    ]
}

struct PhaseRun {
    plan: PhasePlan,
    started: Instant,
    started_ms: u128,
}

impl PhaseRun {
    /// Set the load before publishing the phase or starting its recovery clock.
    fn begin(
        plan: PhasePlan,
        target_rps: &AtomicUsize,
        phase_tx: &watch::Sender<String>,
        workload: &WorkloadStats,
    ) -> Self {
        target_rps.store(plan.initial_rps, Ordering::Relaxed);
        workload.begin_phase();
        let started = Instant::now();
        let started_ms = unix_ms();
        phase_tx.send_replace(plan.phase.as_label().to_string());
        Self {
            plan,
            started,
            started_ms,
        }
    }

    /// Collect metrics and advance the ramp in this same future. Returning or
    /// dropping it leaves no task that could change a later phase's load.
    async fn collect<T>(
        &self,
        target_rps: &AtomicUsize,
        windows: &mut mpsc::Receiver<T>,
        mut on_window: impl FnMut(&T) -> Result<()>,
    ) -> Result<()> {
        let deadline = self.started + self.plan.duration;
        loop {
            let elapsed = self.started.elapsed().min(self.plan.duration);
            let rate = self.plan.rate_at(elapsed);
            target_rps.store(rate, Ordering::Relaxed);
            if Instant::now() >= deadline {
                return Ok(());
            }
            let next = if rate < self.plan.target_rps {
                let next_secs = (elapsed.as_secs() / self.plan.step_secs + 1)
                    .saturating_mul(self.plan.step_secs);
                self.started + Duration::from_secs(next_secs).min(self.plan.duration)
            } else {
                deadline
            };
            tokio::select! {
                biased;
                _ = sleep_until(next) => {}
                window = windows.recv() => {
                    let window = window.ok_or_else(|| anyhow::anyhow!("metrics stream closed during {}", self.plan.phase.as_label()))?;
                    on_window(&window)?;
                }
            }
        }
    }
}

/// Dropping the runner on success or error stops polling and offered load.
struct BackgroundTasks {
    handles: Vec<JoinHandle<()>>,
    target_rps: Arc<AtomicUsize>,
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        self.target_rps.store(0, Ordering::Relaxed);
        for handle in &self.handles {
            handle.abort();
        }
    }
}

pub async fn run_experiment(strategy: &str) -> Result<()> {
    run_experiment_with_scenario(strategy, None).await
}

pub async fn run_experiment_with_scenario(
    strategy: &str,
    scenario: Option<Scenario>,
) -> Result<()> {
    let cfg = ExperimentSettings::from_env_for(scenario)?;
    let mut outage = NodeOutage::new(HttpNodeControl {
        node: cfg.outage_node,
        strategy: strategy.into(),
        client: Client::builder().timeout(Duration::from_secs(10)).build()?,
    });
    // Dropping run_protocol stops offered load and polling before restoration starts.
    let result = tokio::select! {
        result = run_protocol(strategy, &cfg, &mut outage) => result,
        signal = cancellation_signal() => signal.and_then(|()| anyhow::bail!("experiment cancelled")),
    };
    outage.finish(result).await
}

async fn cancellation_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_protocol(
    strategy: &str,
    cfg: &ExperimentSettings,
    outage: &mut NodeOutage<HttpNodeControl>,
) -> Result<()> {
    let [warmup, regular, overload, observe] = phase_plans(&cfg);
    let nodes: Vec<String> = NODES.iter().map(|s| s.to_string()).collect();
    let client = Arc::new(
        Client::builder()
            .pool_max_idle_per_host(200)
            .timeout(Duration::from_millis(cfg.request_timeout_ms))
            .build()?,
    );
    let preflight_client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    cluster::verify_node_strategies(&nodes, strategy, &preflight_client).await?;

    let csv_path = results_path_prefix(strategy, cfg.scenario.as_str())?;
    let mut sidecar_path = csv_path.clone();
    let stem = csv_path.file_stem().unwrap().to_string_lossy();
    sidecar_path.set_file_name(format!("{stem}_summary.json"));
    println!("📊 [{strategy}] Metrics → {}", csv_path.display());
    println!("📋 [{strategy}] Summary → {}", sidecar_path.display());
    let journal_path = csv_path.with_file_name(format!("{stem}_measurements.jsonl"));
    println!(
        "📒 [{strategy}] Full measurements → {}",
        journal_path.display()
    );
    let mut journal = MeasurementJournal::new(&journal_path, strategy, cfg)?;
    let mut csv = MetricsWriter::new(&csv_path)?;
    let mut measurements = Vec::new();

    let target_rps = Arc::new(AtomicUsize::new(cfg.warmup_start_rps));
    let mut tasks = BackgroundTasks {
        handles: Vec::new(),
        target_rps: target_rps.clone(),
    };
    let workload = WorkloadStats::new(nodes.len());
    let ring = shared_ring(
        twin_ring_core::DualRing::new(cfg.ring_members.clone())
            .expect("settings validated ring membership"),
    );
    let (phase_tx, phase_rx) = watch::channel(Phase::Warmup.as_label().to_string());
    let (window_tx, mut window_rx) = mpsc::channel(256);
    let poller = StatsPoller::new(nodes.clone(), cfg.poll_interval_secs, workload.clone())?;
    tasks
        .handles
        .push(tokio::spawn(poller.run(phase_rx, window_tx)));
    tasks.handles.push(spawn_scheduler(
        client.clone(),
        nodes.clone(),
        ring.clone(),
        target_rps.clone(),
        cfg.max_in_flight,
        cfg.key_space,
        workload.clone(),
    ));

    let result: Result<()> = async {
        println!(
            "\n[{strategy}] WARMUP {}s — {}→{} offered requests/s",
            cfg.warmup_secs, cfg.warmup_start_rps, cfg.regular_rps
        );
        PhaseRun::begin(warmup, &target_rps, &phase_tx, &workload)
            .collect(&target_rps, &mut window_rx, |w| {
                record_round(w, &mut csv, &mut journal, &mut measurements)
            })
            .await?;
        ensure!(
            workload.any_success(),
            "no successful reads during warmup ({} failed logical reads)",
            workload.failed()
        );

        println!(
            "\n[{strategy}] REGULAR WORK {}s — {} offered requests/s",
            cfg.regular_work_secs, cfg.regular_rps
        );
        let regular_run = PhaseRun::begin(regular, &target_rps, &phase_tx, &workload);
        regular_run
            .collect(&target_rps, &mut window_rx, |round| {
                record_round(round, &mut csv, &mut journal, &mut measurements)
            })
            .await?;
        let baseline = Baseline::from_regular_tail_at_rate(
            &measurements,
            unix_ms(),
            cfg.baseline_window_secs,
            &cfg.recovery,
            cfg.max_measurement_secs(),
            cfg.regular_rps,
            cfg.baseline_offered_rps_tolerance,
        )?;
        println!(
            "\n[{strategy}] Regular baseline: {:.0} successful reads/s; typical client p95={:.0}µs",
            baseline.successful_rps, baseline.client_p95_us
        );
        println!("   Recovery references use the lowest regular-work throughput and highest regular-work latency; recovery criteria begin after disruption.");
        let mut tracker = RecoveryTracker::for_scenario(
            baseline,
            cfg.recovery.clone(),
            cfg.max_measurement_secs(),
            cfg.scenario,
            NODES[cfg.outage_node - 1].to_string(),
        );
        tracker.expect_observation_rate(
            cfg.regular_rps,
            cfg.baseline_offered_rps_tolerance,
        );

        println!(
            "\n[{strategy}] OVERLOAD {}s — {}→{} offered requests/s; all nodes stay running",
            cfg.overload_secs, cfg.regular_rps, cfg.overload_rps
        );
        PhaseRun::begin(overload, &target_rps, &phase_tx, &workload)
            .collect(&target_rps, &mut window_rx, |w| {
                record_round(w, &mut csv, &mut journal, &mut measurements)?;
                tracker.observe(w);
                Ok(())
            })
            .await?;

        // The node-outage scenario stops a node AFTER elevated load has run.
        if cfg.scenario == Scenario::NodeOutage {
            let elevated = PhasePlan {
                initial_rps: cfg.overload_rps,
                target_rps: cfg.overload_rps,
                ..regular
            };
            PhaseRun::begin(
                PhasePlan {
                    phase: Phase::StoppingNode,
                    ..elevated
                },
                &target_rps,
                &phase_tx,
                &workload,
            );
            outage.stop().await?;
            println!(
                "\n[{strategy}] NODE OUTAGE {}s — node {} stopped; {} offered requests/s",
                cfg.outage_secs, cfg.outage_node, cfg.overload_rps
            );
            PhaseRun::begin(
                PhasePlan {
                    phase: Phase::NodeOutage,
                    duration: Duration::from_secs(cfg.outage_secs),
                    ..elevated
                },
                &target_rps,
                &phase_tx,
                &workload,
            )
            .collect(&target_rps, &mut window_rx, |round| {
                record_round(round, &mut csv, &mut journal, &mut measurements)?;
                tracker.observe(round);
                Ok(())
            })
            .await?;
        }
        let mut regular_load_restored_ms = None;
        if cfg.scenario != Scenario::Overload {
            regular_load_restored_ms = Some(
                restart_at_regular_load(outage, regular, &target_rps, &phase_tx, &workload).await?,
            );
        }
        let observation = PhaseRun::begin(observe, &target_rps, &phase_tx, &workload);
        // For outage, count startup/readiness time from the start acknowledgement.
        // Readiness probes run alongside measurement, never before the recovery clock.
        let recovery_started_ms = outage
            .events
            .restart_acknowledged_ms
            .unwrap_or(observation.started_ms);
        if cfg.scenario == Scenario::Overload {
            regular_load_restored_ms = Some(observation.started_ms);
        }
        tracker.begin_observation(recovery_started_ms);
        println!(
            "\n[{strategy}] OBSERVE {}s — {} offered requests/s",
            cfg.observe_secs, cfg.regular_rps
        );
        let collect = observation.collect(&target_rps, &mut window_rx, |round| {
            record_round(round, &mut csv, &mut journal, &mut measurements)?;
            tracker.observe(round);
            Ok(())
        });
        if cfg.scenario != Scenario::Overload {
            tokio::try_join!(collect, outage.confirm_ready())?;
        } else {
            collect.await?;
        }
        let outcome = tracker.outcome(unix_ms());
        println!("\n[{strategy}] {}", tracker.outcome_description(outcome));
        if let Some(seconds) = tracker.time_to_recovery_secs() {
            println!(
                "   Sustained recovery confirmed {seconds:.1}s after the recovery observation reference time"
            );
        }
        println!("   Recorded relapses: {}", tracker.relapse_times_ms.len());
        write_summary(
            &sidecar_path,
            &Summary {
                strategy,
                tracker: &tracker,
                settings: &cfg,
                recovery_started_ms,
                regular_load_restored_ms,
                outage_events: &outage.events,
                outcome,
                measurements: &measurements,
            },
        )?;
        Ok(())
    }
    .await;
    drop(tasks);
    if let Err(error) = result {
        if let Err(report_error) = write_failed_summary(
            &sidecar_path,
            strategy,
            cfg,
            &outage.events,
            &measurements,
            &error,
        ) {
            return Err(error.context(format!(
                "also failed to save diagnostic summary: {report_error:#}"
            )));
        }
        return Err(error);
    }
    generate_charts(strategy, &csv_path);
    Ok(())
}

/// Restore regular offered rate and publish the phase before calling the control API.
async fn restart_at_regular_load<C: NodeControl>(
    outage: &mut NodeOutage<C>,
    regular: PhasePlan,
    target_rps: &AtomicUsize,
    phase_tx: &watch::Sender<String>,
    workload: &WorkloadStats,
) -> Result<u128> {
    let restart = PhaseRun::begin(
        PhasePlan {
            phase: Phase::RestartingNode,
            ..regular
        },
        target_rps,
        phase_tx,
        workload,
    );
    outage.start().await?;
    Ok(restart.started_ms)
}

fn record_round(
    round: &MeasurementRound,
    csv: &mut MetricsWriter,
    journal: &mut MeasurementJournal,
    history: &mut Vec<MeasurementRound>,
) -> Result<()> {
    journal.write(round)?;
    print_window(round);
    for node in &round.nodes {
        csv.write(node)?;
    }
    history.push(round.clone());
    Ok(())
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[cfg(test)]
#[path = "protocol_tests.rs"]
mod protocol_tests;
