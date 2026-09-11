use super::super::workload::{ClientSample, RequestWindow};
use super::*;
use crate::measurements::{NodeStatsResponse, NodeWindow};

fn baseline() -> Baseline {
    Baseline {
        nodes: NODES
            .iter()
            .map(|node| NodeBaseline {
                node: node.to_string(),
                successful_rps: 100.0,
                successful_rps_lower_bound: 100.0,
                client_p95_us: 2000.0,
                client_p95_upper_bound_us: 2000.0,
                db_p99_us: Some(1000.0),
                db_p99_upper_bound_us: Some(1000.0),
            })
            .collect(),
        successful_rps: 300.0,
        client_p95_us: 2000.0,
        hit_rate: 0.9,
        windows: 4,
    }
}

fn round(sequence: u64, phase: &str) -> MeasurementRound {
    let timestamp_ms = sequence as u128 * 5000;
    let nodes = NODES
        .iter()
        .map(|node| {
            let mut w = NodeWindow::from_response(
                NodeStatsResponse {
                    hits: 450,
                    l1_hits: 450,
                    l2_hits: 0,
                    misses: 50,
                    backup_misses: 0,
                    backup_db_calls: 0,
                    db_hits: 50,
                    db_not_found: 0,
                    db_errors: 0,
                    cache_p50_us: 100,
                    cache_p99_us: 2000,
                    db_p50_us: 500,
                    db_p99_us: 1000,
                    live_entries: 1000,
                },
                node.to_string(),
                phase.to_string(),
                5.0,
            );
            w.timestamp_ms = timestamp_ms;
            w
        })
        .collect();
    let requests = RequestWindow {
        offered: 500,
        admitted: 500,
        shed: 0,
        successful: 500,
        http_errors: 0,
        transport_errors: 0,
        success_latency_p95_us: Some(2000),
    };
    MeasurementRound {
        sequence,
        started_ms: timestamp_ms - 5000,
        timestamp_ms,
        duration_secs: 5.0,
        phase: phase.into(),
        nodes,
        client: ClientSample {
            in_flight: 0,
            prior_phase_in_flight: 0,
            prior_phase_finished: 0,
            by_primary: vec![requests.clone(); 3],
            total: RequestWindow {
                offered: 1500,
                admitted: 1500,
                successful: 1500,
                ..requests
            },
        },
        missing_nodes: Vec::new(),
        unaligned_nodes: Vec::new(),
        aligned: true,
        phase_consistent: true,
    }
}

fn bad(sequence: u64, phase: &str) -> MeasurementRound {
    let mut w = round(sequence, phase);
    w.client.by_primary[1].successful = 100;
    w.client.total.successful = 1100;
    w
}

fn tracker() -> RecoveryTracker {
    RecoveryTracker::new(baseline(), RecoveryCriteria::default(), 15.0)
}

fn recovering_tracker() -> RecoveryTracker {
    let mut t = tracker();
    t.observe(&bad(1, "overload"));
    t.begin_observation(5000);
    t
}

#[test]
fn recovery_requires_three_complete_consecutive_windows() {
    let mut t = recovering_tracker();
    t.observe(&round(2, "observe"));
    t.observe(&round(3, "observe"));
    assert_eq!(t.first_recovered_ms, None);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Recovering);
    t.observe(&round(4, "observe"));
    assert_eq!(t.time_to_recovery_secs(), Some(15.0));
    assert_eq!(t.outcome(20_000), RunOutcome::Recovered);
}

#[test]
fn baseline_checks_that_the_scheduler_delivered_the_configured_rate() {
    let rounds = vec![
        round(1, "regular_work"),
        round(2, "regular_work"),
        round(3, "regular_work"),
    ];
    Baseline::from_regular_tail_at_rate(
        &rounds,
        15_000,
        15,
        &RecoveryCriteria::default(),
        15.0,
        300,
        0.05,
    )
    .unwrap();

    let error = Baseline::from_regular_tail_at_rate(
        &rounds,
        15_000,
        15,
        &RecoveryCriteria::default(),
        15.0,
        400,
        0.05,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("baseline offered rate"), "{error}");
}

#[test]
fn shed_demand_prevents_a_false_recovery() {
    let mut t = recovering_tracker();
    let mut w = round(2, "observe");
    w.client.by_primary[0].shed = 10;
    w.client.by_primary[0].admitted = 490;
    w.client.total.shed = 10;
    w.client.total.admitted = 1490;
    t.observe(&w);
    let assessment = t.assessments.last().unwrap();
    assert_eq!(assessment.state, WindowState::Degraded);
    assert!(
        assessment
            .reasons
            .iter()
            .any(|reason| reason.contains("shed rate"))
    );
}

#[test]
fn an_underdriven_observation_window_cannot_advance_recovery() {
    let mut t = recovering_tracker();
    t.expect_observation_rate(300, 0.05);
    let mut w = round(2, "observe");
    w.client.total.offered = 1_000; // 200/s instead of the required 300/s
    t.observe(&w);
    let assessment = t.assessments.last().unwrap();
    assert_eq!(assessment.state, WindowState::Unknown);
    assert!(assessment.reasons[0].contains("configured regular offered rate"));
}

#[test]
fn one_good_node_cannot_establish_cluster_recovery() {
    let mut t = recovering_tracker();
    for seq in 2..6 {
        t.observe(&bad(seq, "observe"));
    }
    assert_eq!(t.first_recovered_ms, None);
    assert_eq!(
        t.outcome(25_000),
        RunOutcome::DidNotRecoverWithinObservation
    );
}

#[test]
fn an_error_storm_is_not_high_successful_throughput() {
    let mut t = recovering_tracker();
    for seq in 2..6 {
        let mut w = round(seq, "observe");
        w.client.by_primary[0].successful = 0;
        w.client.by_primary[0].http_errors = 100_000;
        w.client.by_primary[0].success_latency_p95_us = None;
        t.observe(&w);
    }
    assert_eq!(t.first_recovered_ms, None);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Degraded);
}

#[test]
fn missing_nodes_break_a_streak_even_when_other_nodes_are_healthy() {
    let mut t = recovering_tracker();
    t.observe(&round(2, "observe"));
    t.observe(&round(3, "observe"));
    let mut missing = round(4, "observe");
    missing.nodes.pop();
    missing.missing_nodes.push(NODES[2].into());
    t.observe(&missing);
    assert_eq!(t.outcome(20_000), RunOutcome::Inconclusive);
    t.observe(&round(5, "observe"));
    t.observe(&round(6, "observe"));
    assert_eq!(t.first_recovered_ms, None);
    t.observe(&round(7, "observe"));
    assert_eq!(t.first_recovered_ms, Some(35_000));
}

#[test]
fn duplicate_or_skipped_rounds_cannot_complete_a_streak() {
    for sequence in [3, 5] {
        let mut t = recovering_tracker();
        t.observe(&round(2, "observe"));
        t.observe(&round(3, "observe"));
        t.observe(&round(sequence, "observe"));
        assert_eq!(t.assessments.last().unwrap().state, WindowState::Unknown);
        assert_eq!(t.first_recovered_ms, None);
    }
}

#[test]
fn successful_reads_without_latency_samples_are_unknown() {
    let mut t = recovering_tracker();
    let mut w = round(2, "observe");
    w.client.by_primary[0].success_latency_p95_us = None;
    t.observe(&w);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Unknown);
}

#[test]
fn database_latency_is_unobserved_when_there_are_no_calls() {
    let mut t = recovering_tracker();
    for seq in 2..5 {
        let mut w = round(seq, "observe");
        for node in &mut w.nodes {
            node.db_hits = 0;
            node.db_p99_us = 0;
            node.db_p50_us = 0;
        }
        t.observe(&w);
    }
    assert_eq!(t.outcome(20_000), RunOutcome::Recovered);
}

#[test]
fn database_calls_without_latency_samples_are_unknown() {
    let mut t = recovering_tracker();
    let mut w = round(2, "observe");
    w.nodes[0].db_p99_us = 0;
    t.observe(&w);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Unknown);
}

#[test]
fn each_nodes_request_latency_and_database_health_must_return() {
    for kind in 0..4 {
        let mut t = recovering_tracker();
        for seq in 2..5 {
            let mut w = round(seq, "observe");
            match kind {
                0 => w.client.by_primary[2].success_latency_p95_us = Some(9000),
                1 => w.client.by_primary[2].transport_errors = 100,
                2 => w.nodes[2].db_errors = 50,
                _ => w.nodes[2].db_p99_us = 9000,
            }
            t.observe(&w);
        }
        assert_eq!(t.first_recovered_ms, None);
    }
}

#[test]
fn relapse_is_recorded_and_does_not_erase_the_first_recovery() {
    let mut t = recovering_tracker();
    for seq in 2..5 {
        t.observe(&round(seq, "observe"));
    }
    t.observe(&bad(5, "observe"));
    t.observe(&bad(6, "observe"));
    assert_eq!(t.relapse_times_ms, vec![25_000]);
    assert_eq!(t.first_recovered_ms, Some(20_000));
    assert_eq!(t.outcome(30_000), RunOutcome::RecoveredThenRelapsed);
    for seq in 7..10 {
        t.observe(&round(seq, "observe"));
    }
    assert_eq!(t.outcome(45_000), RunOutcome::Recovered);
    assert_eq!(t.relapse_times_ms.len(), 1);
}

#[test]
fn a_gap_does_not_hide_a_later_relapse() {
    let mut t = recovering_tracker();
    for seq in 2..5 {
        t.observe(&round(seq, "observe"));
    }
    let mut w = round(5, "observe");
    w.aligned = false;
    t.observe(&w);
    t.observe(&bad(6, "observe"));
    assert_eq!(t.relapse_times_ms, vec![30_000]);
    assert_eq!(t.outcome(30_000), RunOutcome::RecoveredThenRelapsed);
}

#[test]
fn stale_final_measurements_cannot_report_recovery() {
    let mut t = recovering_tracker();
    for seq in 2..5 {
        t.observe(&round(seq, "observe"));
    }
    assert_eq!(t.outcome(40_000), RunOutcome::Inconclusive);
    assert_eq!(t.first_recovered_ms, Some(20_000));
}

#[test]
fn prevention_is_distinct_from_recovery() {
    let mut t = tracker();
    for seq in 1..4 {
        t.observe(&round(seq, "overload"));
    }
    t.begin_observation(15_000);
    for seq in 4..7 {
        t.observe(&round(seq, "observe"));
    }
    assert_eq!(t.outcome(30_000), RunOutcome::ResistedDegradation);
    assert_eq!(t.time_to_recovery_secs(), None);
}

#[test]
fn insufficient_overload_evidence_is_inconclusive() {
    let mut t = tracker();
    t.begin_observation(0);
    for seq in 1..4 {
        t.observe(&round(seq, "observe"));
    }
    assert_eq!(t.outcome(15_000), RunOutcome::Inconclusive);
}

#[test]
fn transitions_and_long_rounds_do_not_advance_the_streak() {
    for transition in [true, false] {
        let mut t = recovering_tracker();
        t.observe(&round(2, "observe"));
        t.observe(&round(3, "observe"));
        let mut w = round(4, "observe");
        if transition {
            w.phase_consistent = false;
        } else {
            w.duration_secs = 20.0;
        }
        t.observe(&w);
        assert_eq!(t.first_recovered_ms, None);
        assert_eq!(t.streak, 0);
    }
}

#[test]
fn baseline_uses_successful_cluster_traffic_and_actual_elapsed_time() {
    let mut rounds: Vec<_> = (1..5).map(|seq| round(seq, "regular_work")).collect();
    let baseline =
        Baseline::from_regular_tail(&rounds, 20_000, 20, &RecoveryCriteria::default(), 15.0)
            .unwrap();
    assert_eq!(baseline.successful_rps, 300.0);
    for r in &mut rounds {
        r.duration_secs = 10.0;
    }
    let baseline =
        Baseline::from_regular_tail(&rounds, 20_000, 20, &RecoveryCriteria::default(), 15.0)
            .unwrap();
    assert_eq!(baseline.successful_rps, 150.0);
}

#[test]
fn baseline_still_rejects_missing_data_zero_success_errors_and_gaps() {
    for kind in 0..7 {
        let mut rounds: Vec<_> = (1..5).map(|seq| round(seq, "regular_work")).collect();
        match kind {
            0 => {
                rounds[1].nodes.pop();
            }
            1 => rounds[1].client.by_primary[1].successful = 0,
            2 => rounds[1].client.by_primary[1].http_errors = 50,
            3 => rounds[1].nodes[1].db_errors = 50,
            4 => rounds[1].client.by_primary[1].success_latency_p95_us = None,
            5 => rounds[1].nodes[1].db_p99_us = 0,
            _ => rounds[1].sequence = 10,
        }
        assert!(
            Baseline::from_regular_tail(&rounds, 20_000, 20, &RecoveryCriteria::default(), 15.0)
                .is_err()
        );
    }
}

#[test]
fn baseline_quality_errors_name_the_measurement_and_cause() {
    let mut rounds: Vec<_> = (1..5).map(|seq| round(seq, "regular_work")).collect();
    rounds[2].nodes.clear();
    rounds[2].missing_nodes = NODES.iter().map(|node| node.to_string()).collect();
    let error =
        Baseline::from_regular_tail(&rounds, 20_000, 20, &RecoveryCriteria::default(), 15.0)
            .unwrap_err()
            .to_string();
    assert!(error.contains("measurement 3"), "{error}");
    assert!(error.contains("missing node measurements"), "{error}");
}

#[test]
fn baseline_error_limits_apply_to_the_complete_period_not_each_window() {
    let mut rounds: Vec<_> = (1..5).map(|seq| round(seq, "regular_work")).collect();
    // These are individually above 1% in one five-second window, but below 1%
    // after all four valid baseline windows are combined.
    rounds[1].client.by_primary[1].http_errors = 10;
    rounds[1].nodes[1].db_errors = 2;
    let baseline =
        Baseline::from_regular_tail(&rounds, 20_000, 20, &RecoveryCriteria::default(), 15.0)
            .unwrap();
    assert_eq!(baseline.windows, 4);
}

#[test]
fn aggregate_baseline_error_reports_include_counts_rate_and_limit() {
    for database in [false, true] {
        let mut rounds: Vec<_> = (1..5).map(|seq| round(seq, "regular_work")).collect();
        if database {
            rounds[1].nodes[1].db_errors = 3;
        } else {
            rounds[1].client.by_primary[1].http_errors = 25;
        }
        let error =
            Baseline::from_regular_tail(&rounds, 20_000, 20, &RecoveryCriteria::default(), 15.0)
                .unwrap_err()
                .to_string();
        assert!(error.contains(if database {
            "database error rate"
        } else {
            "request error rate"
        }));
        assert!(error.contains("errors across"));
        assert!(error.contains("maximum allowed is 1.000%"));
    }
}

#[test]
fn baseline_excludes_warmup_and_windows_crossing_the_tail_boundary() {
    let mut rounds: Vec<_> = (1..5).map(|seq| round(seq, "regular_work")).collect();
    rounds[0].phase = "warmup".into();
    let baseline =
        Baseline::from_regular_tail(&rounds, 20_000, 15, &RecoveryCriteria::default(), 15.0)
            .unwrap();
    assert_eq!(baseline.windows, 3);
    assert!(
        Baseline::from_regular_tail(&rounds, 20_000, 14, &RecoveryCriteria::default(), 15.0)
            .is_err()
    );
}

#[test]
fn new_database_traffic_needs_a_measured_database_reference() {
    let mut base = baseline();
    base.nodes[0].db_p99_us = None;
    let mut t = RecoveryTracker::new(base, RecoveryCriteria::default(), 15.0);
    t.begin_observation(0);
    for seq in 1..=3 {
        t.observe(&round(seq, "observe"));
    }
    assert_eq!(t.outcome(15_000), RunOutcome::Inconclusive);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Unknown);
}

#[test]
fn overload_reads_cannot_hide_inside_three_healthy_observation_rounds() {
    use super::super::workload::{RequestOutcome, WorkloadStats};
    use std::time::Duration;
    let stats = WorkloadStats::new(3);
    let slow = stats.begin_read(0);
    stats.begin_phase();
    let mut t = recovering_tracker();
    for seq in 2..=4 {
        let mut w = round(seq, "observe");
        let sample = stats.snapshot_and_reset();
        w.client.prior_phase_in_flight = sample.prior_phase_in_flight;
        w.client.prior_phase_finished = sample.prior_phase_finished;
        assert!(!w.within_one_phase());
        t.observe(&w);
    }
    assert_eq!(t.first_recovered_ms, None);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Transition);
    slow.finish(RequestOutcome::Success, Duration::from_secs(20));
    let mut completion = round(5, "observe");
    completion.client.prior_phase_finished = stats.snapshot_and_reset().prior_phase_finished;
    t.observe(&completion);
    assert_eq!(t.first_recovered_ms, None);
    for seq in 6..=7 {
        t.observe(&round(seq, "observe"));
        assert_eq!(t.first_recovered_ms, None);
    }
    t.observe(&round(8, "observe"));
    assert_eq!(t.time_to_recovery_secs(), Some(35.0));
    assert_eq!(t.outcome(40_000), RunOutcome::Recovered);
}

#[test]
fn warmup_reads_cannot_become_part_of_the_regular_baseline() {
    let mut rounds: Vec<_> = (1..5).map(|seq| round(seq, "regular_work")).collect();
    rounds[1].client.prior_phase_finished = 1;
    assert!(
        Baseline::from_regular_tail(&rounds, 20_000, 20, &RecoveryCriteria::default(), 15.0)
            .is_err()
    );
}

fn outage_round(sequence: u64, node: usize) -> MeasurementRound {
    let mut w = round(sequence, "node_outage");
    w.nodes.retain(|row| row.node != NODES[node]);
    w.missing_nodes.push(NODES[node].into());
    w.unaligned_nodes.push(NODES[node].into());
    w.aligned = false;
    w
}

fn outage_tracker(node: usize) -> RecoveryTracker {
    RecoveryTracker::for_scenario(
        baseline(),
        RecoveryCriteria::default(),
        15.0,
        Scenario::NodeOutage,
        NODES[node].into(),
    )
}

#[test]
fn the_selected_stopped_node_is_expected_but_another_missing_node_is_not() {
    for node in 0..3 {
        let mut t = outage_tracker(node);
        t.observe(&outage_round(1, node));
        assert_eq!(
            t.assessments.last().unwrap().state,
            WindowState::ExpectedOutage
        );
        let other = (node + 1) % 3;
        let mut w = outage_round(2, node);
        w.nodes.retain(|row| row.node != NODES[other]);
        w.missing_nodes.push(NODES[other].into());
        t.observe(&w);
        assert_eq!(t.assessments.last().unwrap().state, WindowState::Unknown);
    }
}

#[test]
fn a_stopped_nodes_client_failures_still_show_service_degradation() {
    let mut t = outage_tracker(0);
    let mut w = outage_round(1, 0);
    w.client.by_primary[0].successful = 0;
    w.client.by_primary[0].http_errors = 500;
    w.client.by_primary[0].success_latency_p95_us = None;
    t.observe(&w);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Degraded);
    t.begin_observation(5000);
    for seq in 2..=4 {
        t.observe(&round(seq, "observe"));
    }
    assert_eq!(t.outcome(20_000), RunOutcome::Recovered);
    assert_eq!(t.time_to_recovery_secs(), Some(15.0));
}

#[test]
fn after_restart_all_three_nodes_and_fresh_counters_are_required() {
    let mut t = outage_tracker(0);
    t.observe(&outage_round(1, 0));
    t.begin_observation(5000);
    let mut absent = outage_round(2, 0);
    absent.phase = "observe".into();
    t.observe(&absent);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Unknown);
    let mut first_poll = round(3, "observe");
    first_poll.aligned = false;
    first_poll.unaligned_nodes.push(NODES[0].into());
    t.observe(&first_poll);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Unknown);
    assert_eq!(t.first_recovered_ms, None);
}

#[test]
fn surviving_nodes_must_have_aligned_measurements_during_the_outage() {
    let mut t = outage_tracker(0);
    let mut w = outage_round(1, 0);
    w.unaligned_nodes.push(NODES[1].into());
    t.observe(&w);
    assert_eq!(t.assessments.last().unwrap().state, WindowState::Unknown);
}

#[test]
fn serving_through_the_outage_is_distinct_from_recovering_after_it() {
    let mut t = outage_tracker(0);
    for seq in 1..=3 {
        t.observe(&round(seq, "overload"));
    }
    for seq in 4..=6 {
        t.observe(&outage_round(seq, 0));
    }
    t.begin_observation(30_000);
    for seq in 7..=9 {
        t.observe(&round(seq, "observe"));
    }
    assert_eq!(t.outcome(45_000), RunOutcome::ResistedDegradation);
    assert_eq!(t.time_to_recovery_secs(), None);
}

#[test]
fn restart_transition_windows_cannot_advance_recovery() {
    let mut t = outage_tracker(0);
    t.observe(&outage_round(1, 0));
    t.begin_observation(5000);
    t.observe(&round(2, "restarting_node"));
    assert_eq!(t.assessments.len(), 1);
    assert_eq!(t.outcome(10_000), RunOutcome::Inconclusive);
}

// Rates reconstructed from node-side CSV tails of the two zero-error failed
// runs. Client per-partition counters were not saved, so these reproduce the
// throughput shape, not an exact replay of those experiments.
fn variable_baseline(rates: &[[f64; 3]], seconds: u64) -> Baseline {
    let rounds: Vec<_> = rates
        .iter()
        .enumerate()
        .map(|(i, rates)| {
            let mut r = round(i as u64 + 1, "regular_work");
            r.duration_secs = seconds as f64;
            r.started_ms = i as u128 * seconds as u128 * 1000;
            r.timestamp_ms = r.started_ms + seconds as u128 * 1000;
            for (requests, rate) in r.client.by_primary.iter_mut().zip(rates) {
                requests.successful = (rate * seconds as f64).round() as u64;
            }
            r.client.total.successful = r.client.by_primary.iter().map(|w| w.successful).sum();
            r
        })
        .collect();
    Baseline::from_regular_tail(
        &rounds,
        rounds.last().unwrap().timestamp_ms,
        seconds * rates.len() as u64,
        &RecoveryCriteria::default(),
        seconds as f64 * 3.0,
    )
    .expect("valid but variable measurements must remain available for a diagnostic run")
}

#[test]
fn observed_five_and_fifteen_second_variation_defines_normal_throughput_bounds() {
    for (seconds, rates) in [
        (
            5,
            [
                [5373.6, 2878.4, 2615.3],
                [6021.4, 3035.2, 2835.6],
                [4716.3, 2669.9, 2343.2],
            ],
        ),
        (
            15,
            [
                [6070.0, 3241.0, 2850.0],
                [5448.6, 2960.8, 2631.7],
                [4853.2, 2696.2, 2387.3],
            ],
        ),
    ] {
        let b = variable_baseline(&rates, seconds);
        assert_eq!(b.windows, 3);
        assert!((b.nodes[0].successful_rps_lower_bound - rates[2][0]).abs() < 0.1);
        assert!(b.nodes[0].successful_rps > b.nodes[0].successful_rps_lower_bound);
    }
}

#[test]
fn recovery_criteria_are_applied_after_disruption_to_normal_bounds() {
    let b = variable_baseline(&[[100.0; 3], [120.0; 3], [80.0; 3]], 5);
    assert_eq!(b.nodes[0].successful_rps_lower_bound, 80.0);
    let mut t = RecoveryTracker::new(b, RecoveryCriteria::default(), 15.0);
    t.observe(&bad(1, "overload"));
    t.begin_observation(5_000);
    for seq in 2..=4 {
        let mut restored = round(seq, "observe");
        // 75/s is below the typical 100/s, but above 90% of the observed
        // regular-work lower bound (72/s).
        for requests in &mut restored.client.by_primary {
            requests.successful = 375;
        }
        t.observe(&restored);
    }
    assert_eq!(t.outcome(20_000), RunOutcome::Recovered);
}

#[test]
fn latency_variation_defines_an_upper_recovery_reference() {
    let mut rounds: Vec<_> = (1..=3).map(|seq| round(seq, "regular_work")).collect();
    rounds[2].client.by_primary[1].success_latency_p95_us = Some(9000);
    let b = Baseline::from_regular_tail(&rounds, 15_000, 15, &RecoveryCriteria::default(), 15.0)
        .unwrap();
    assert_eq!(b.nodes[1].client_p95_us, 2000.0);
    assert_eq!(b.nodes[1].client_p95_upper_bound_us, 9000.0);

    let mut t = RecoveryTracker::new(b, RecoveryCriteria::default(), 15.0);
    t.observe(&bad(1, "overload"));
    t.begin_observation(5_000);
    for seq in 2..=4 {
        let mut restored = round(seq, "observe");
        restored.client.by_primary[1].success_latency_p95_us = Some(17_000);
        t.observe(&restored);
    }
    assert_eq!(t.outcome(20_000), RunOutcome::Recovered);
}

#[test]
fn diagnostics_preserve_complete_client_measurements_before_the_run_finishes() {
    use super::super::{outage::OutageEvents, output::*, settings::ExperimentSettings};
    let cfg = ExperimentSettings::from_reader(|_| Ok(None)).unwrap();
    let folder = std::env::temp_dir().join(format!("twin-ring-diagnostics-{}", std::process::id()));
    std::fs::create_dir_all(&folder).unwrap();
    let path = folder.join("measurements.jsonl");
    let mut log = MeasurementJournal::new(&path, "lru", &cfg).unwrap();
    let r = round(1, "regular_work");
    log.write(&r).unwrap();
    // Read while the writer is still alive: data must already be flushed.
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<serde_json::Value> = content
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["settings"]["regular_rps"], cfg.regular_rps);
    assert_eq!(
        lines[1]["measurement"]["client"]["by_primary"][0]["successful"],
        500
    );
    assert_eq!(
        lines[1]["measurement"]["client"]["by_primary"][0]["success_latency_p95_us"],
        2000
    );
    let report_path = folder.join("summary.json");
    write_failed_summary(
        &report_path,
        "lru",
        &cfg,
        &OutageEvents::default(),
        &[r],
        &anyhow::anyhow!("baseline database errors exceed the limit"),
    )
    .unwrap();
    let report: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(report_path).unwrap()).unwrap();
    assert_eq!(report["run_status"], "aborted");
    assert_eq!(report["outcome"], "inconclusive");
    assert!(report["time_to_recovery_secs"].is_null());
    assert!(
        report["error"]
            .as_str()
            .unwrap()
            .contains("database errors")
    );
    assert_eq!(report["measurements"].as_array().unwrap().len(), 1);
    drop(log);
    std::fs::remove_dir_all(folder).unwrap();
}

#[test]
fn completed_summary_records_the_baseline_and_recovery_reference_methods() {
    use super::super::{outage::OutageEvents, output::*, settings::ExperimentSettings};
    let cfg = ExperimentSettings::from_reader(|_| Ok(None)).unwrap();
    let b = variable_baseline(&[[100.0; 3], [120.0; 3], [80.0; 3]], 5);
    let t = RecoveryTracker::new(b, cfg.recovery.clone(), 15.0);
    let report = summary_value(&Summary {
        strategy: "lru",
        tracker: &t,
        settings: &cfg,
        recovery_started_ms: 0,
        regular_load_restored_ms: Some(0),
        outage_events: &OutageEvents::default(),
        outcome: t.outcome(0),
        measurements: &[],
    });
    assert_eq!(report["run_status"], "completed");
    assert_eq!(report["baseline_usable_for_verdict"], true);
    assert_eq!(
        report["baseline_validation"],
        "complete_fixed_rate_regular_work_and_basic_service_health_v2"
    );
    assert_eq!(report["workload_model"], "fixed_arrival_rate_v1");
    assert_eq!(
        report["recovery_threshold_reference"],
        "observed_regular_work_bounds_v1"
    );
    assert!(report["baseline"]["nodes"][0]["successful_rps_lower_bound"].is_number());
    assert!(report["baseline"]["nodes"][0]["client_p95_upper_bound_us"].is_number());
    assert!(report["time_to_recovery_secs"].is_null());
}
