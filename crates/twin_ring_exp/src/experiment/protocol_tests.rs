use super::*;
use crate::measurements::NodeWindow;

fn settings() -> ExperimentSettings {
    ExperimentSettings::from_reader(|_| Ok(None)).unwrap()
}

#[test]
fn plans_use_the_same_regular_load_before_and_after_overload() {
    let cfg = settings();
    let plans = phase_plans(&cfg);
    assert_eq!(
        plans.map(|p| p.phase),
        [
            Phase::Warmup,
            Phase::RegularWork,
            Phase::Overload,
            Phase::Observe
        ]
    );
    assert_eq!(plans[0].rate_at(plans[0].duration), cfg.regular_rps);
    assert_eq!(plans[1].rate_at(Duration::ZERO), cfg.regular_rps);
    assert_eq!(plans[2].rate_at(plans[2].duration), cfg.overload_rps);
    assert_eq!(plans[3].rate_at(Duration::ZERO), cfg.regular_rps);
    assert_eq!(plans[3].rate_at(plans[3].duration), cfg.regular_rps);
}

#[test]
fn ramp_starts_low_changes_on_schedule_and_caps_the_final_step() {
    let plan = PhasePlan {
        phase: Phase::Warmup,
        duration: Duration::from_secs(30),
        initial_rps: 15,
        target_rps: 50,
        step_rps: 15,
        step_secs: 10,
    };
    assert_eq!(plan.rate_at(Duration::from_secs(9)), 15);
    assert_eq!(plan.rate_at(Duration::from_secs(10)), 30);
    assert_eq!(plan.rate_at(Duration::from_secs(20)), 45);
    assert_eq!(plan.rate_at(Duration::from_secs(30)), 50);
    assert_eq!(plan.rate_at(Duration::from_secs(100)), 50);
}

#[tokio::test(start_paused = true)]
async fn regular_work_starts_after_warmup_reaches_regular_load() {
    let ceiling = Arc::new(AtomicUsize::new(0));
    let (phase_tx, phase_rx) = watch::channel(String::new());
    let (_window_tx, mut windows) = mpsc::channel::<NodeWindow>(1);
    let warmup = PhasePlan {
        phase: Phase::Warmup,
        duration: Duration::from_secs(2),
        initial_rps: 1,
        target_rps: 3,
        step_rps: 1,
        step_secs: 1,
    };
    let c = ceiling.clone();
    let handle = tokio::spawn(async move {
        PhaseRun::begin(warmup, &c, &phase_tx, &WorkloadStats::new(3))
            .collect(&c, &mut windows, |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(c.load(Ordering::Relaxed), 3);
        let regular = PhasePlan {
            phase: Phase::RegularWork,
            initial_rps: 3,
            target_rps: 3,
            step_rps: 0,
            ..warmup
        };
        PhaseRun::begin(regular, &c, &phase_tx, &WorkloadStats::new(3))
            .collect(&c, &mut windows, |_| Ok(()))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    assert_eq!(phase_rx.borrow().as_str(), "warmup");
    assert_eq!(ceiling.load(Ordering::Relaxed), 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(ceiling.load(Ordering::Relaxed), 2);
    assert_eq!(phase_rx.borrow().as_str(), "warmup");
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(ceiling.load(Ordering::Relaxed), 3);
    assert_eq!(phase_rx.borrow().as_str(), "regular_work");
    handle.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn unfinished_overload_ramp_cannot_raise_load_during_observation() {
    let ceiling = Arc::new(AtomicUsize::new(0));
    let (phase_tx, phase_rx) = watch::channel(String::new());
    let (_window_tx, mut windows) = mpsc::channel::<NodeWindow>(1);
    let overload = PhasePlan {
        phase: Phase::Overload,
        duration: Duration::from_secs(3),
        initial_rps: 5,
        target_rps: 500,
        step_rps: 100,
        step_secs: 2,
    };
    let c = ceiling.clone();
    let handle = tokio::spawn(async move {
        PhaseRun::begin(overload, &c, &phase_tx, &WorkloadStats::new(3))
            .collect(&c, &mut windows, |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(c.load(Ordering::Relaxed), 105);
        let observe = PhasePlan {
            phase: Phase::Observe,
            duration: Duration::from_secs(20),
            initial_rps: 5,
            target_rps: 5,
            step_rps: 0,
            step_secs: 1,
        };
        PhaseRun::begin(observe, &c, &phase_tx, &WorkloadStats::new(3))
            .collect(&c, &mut windows, |_| Ok(()))
            .await
            .unwrap();
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert_eq!(ceiling.load(Ordering::Relaxed), 105);
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(phase_rx.borrow().as_str(), "observe");
    assert_eq!(ceiling.load(Ordering::Relaxed), 5);
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(ceiling.load(Ordering::Relaxed), 5);
    handle.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn closed_metrics_stream_is_an_error_not_a_completed_phase() {
    let ceiling = AtomicUsize::new(0);
    let (phase_tx, _phase_rx) = watch::channel(String::new());
    let (window_tx, mut windows) = mpsc::channel::<NodeWindow>(1);
    drop(window_tx);
    let regular = phase_plans(&settings())[1];
    let result = PhaseRun::begin(regular, &ceiling, &phase_tx, &WorkloadStats::new(3))
        .collect(&ceiling, &mut windows, |_| Ok(()))
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("metrics stream closed")
    );
}

#[tokio::test(start_paused = true)]
async fn dropping_background_tasks_stops_load_and_aborts_workers() {
    let ceiling = Arc::new(AtomicUsize::new(150));
    let handle = tokio::spawn(std::future::pending::<()>());
    let abort = handle.abort_handle();
    let tasks = BackgroundTasks {
        handles: vec![handle],
        target_rps: ceiling.clone(),
    };
    drop(tasks);
    tokio::task::yield_now().await;
    assert_eq!(ceiling.load(Ordering::Relaxed), 0);
    assert!(abort.is_finished());
}

#[test]
fn restoring_regular_load_marks_unfinished_overload_reads_as_prior_phase() {
    let cfg = settings();
    let [_, _, overload, observe] = phase_plans(&cfg);
    let ceiling = AtomicUsize::new(0);
    let (phase_tx, phase_rx) = watch::channel(String::new());
    let stats = WorkloadStats::new(3);
    PhaseRun::begin(overload, &ceiling, &phase_tx, &stats);
    ceiling.store(cfg.overload_rps, Ordering::Relaxed);
    let unfinished = stats.begin_read(0);
    assert_eq!(stats.snapshot_and_reset().prior_phase_in_flight, 0);

    PhaseRun::begin(observe, &ceiling, &phase_tx, &stats);
    assert_eq!(ceiling.load(Ordering::Relaxed), cfg.regular_rps);
    assert_eq!(*phase_rx.borrow(), "observe");
    assert_eq!(stats.snapshot_and_reset().prior_phase_in_flight, 1);
    drop(unfinished);
    assert_eq!(stats.snapshot_and_reset().prior_phase_finished, 1);
}

#[test]
fn both_scenarios_elevate_load_and_restore_the_same_regular_ceiling() {
    for scenario in [Scenario::Overload, Scenario::NodeOutage] {
        let mut cfg = settings();
        cfg.scenario = scenario;
        let [_, regular, elevated, observe] = phase_plans(&cfg);
        assert_eq!(elevated.phase, Phase::Overload);
        assert_eq!(elevated.rate_at(elevated.duration), cfg.overload_rps);
        assert_eq!(regular.target_rps, cfg.regular_rps);
        assert_eq!(observe.target_rps, cfg.regular_rps);
    }
}

#[tokio::test]
async fn the_restart_request_sees_regular_load_after_the_node_was_stopped_at_elevated_load() {
    struct Control {
        ceiling: Arc<AtomicUsize>,
        calls: Arc<std::sync::Mutex<Vec<(&'static str, usize)>>>,
    }
    impl NodeControl for Control {
        async fn stop(&self) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(("stop", self.ceiling.load(Ordering::Relaxed)));
            Ok(())
        }
        async fn start(&self) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(("start", self.ceiling.load(Ordering::Relaxed)));
            Ok(())
        }
        async fn ready(&self) -> Result<()> {
            Ok(())
        }
    }
    let cfg = settings();
    let [_, regular, _, _] = phase_plans(&cfg);
    let ceiling = Arc::new(AtomicUsize::new(cfg.overload_rps));
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut outage = NodeOutage::new(Control {
        ceiling: ceiling.clone(),
        calls: calls.clone(),
    });
    let stats = WorkloadStats::new(3);
    let (phase_tx, phase_rx) = watch::channel("node_outage".into());
    outage.stop().await.unwrap();
    let restored = restart_at_regular_load(&mut outage, regular, &ceiling, &phase_tx, &stats)
        .await
        .unwrap();
    assert_eq!(
        *calls.lock().unwrap(),
        [("stop", cfg.overload_rps), ("start", cfg.regular_rps)]
    );
    assert_eq!(*phase_rx.borrow(), "restarting_node");
    assert!(restored <= outage.events.restart_requested_ms.unwrap());
    assert!(outage.events.restart_acknowledged_ms.is_some());
    assert!(outage.events.ready_ms.is_none());
}
