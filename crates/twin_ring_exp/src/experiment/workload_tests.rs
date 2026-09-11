use super::*;

#[test]
fn only_a_complete_http_200_read_is_successful() {
    assert!(matches!(
        response_outcome(200, true),
        RequestOutcome::Success
    ));
    for status in [404, 500, 503, 204] {
        assert!(matches!(
            response_outcome(status, true),
            RequestOutcome::HttpError
        ));
    }
    assert!(matches!(
        response_outcome(200, false),
        RequestOutcome::TransportError
    ));
}

#[test]
fn fast_http_errors_do_not_inflate_successful_throughput_or_lower_success_latency() {
    let stats = WorkloadStats::new(3);
    for _ in 0..100 {
        stats.record(0, RequestOutcome::HttpError, Duration::from_micros(1));
    }
    stats.record(0, RequestOutcome::Success, Duration::from_millis(20));
    stats.record(
        1,
        RequestOutcome::TransportError,
        Duration::from_millis(500),
    );
    let sample = stats.snapshot_and_reset();
    assert_eq!(sample.total.successful, 1);
    assert_eq!(sample.total.http_errors, 100);
    assert_eq!(sample.total.transport_errors, 1);
    assert!(sample.total.success_latency_p95_us.unwrap() >= 20_000);
    assert!(sample.total.error_rate().unwrap() > 0.99);
    assert_eq!(sample.by_primary[2].success_latency_p95_us, None);
}

#[test]
fn snapshots_reset_window_counters_but_preserve_the_warmup_success_check() {
    let stats = WorkloadStats::new(3);
    stats.record(0, RequestOutcome::Success, Duration::from_millis(1));
    assert_eq!(stats.snapshot_and_reset().total.successful, 1);
    let empty = stats.snapshot_and_reset();
    assert_eq!(empty.total.completed(), 0);
    assert_eq!(empty.total.error_rate(), None);
    assert_eq!(empty.total.success_latency_p95_us, None);
    assert!(stats.any_success());
}

#[test]
fn an_error_only_warmup_is_not_successful() {
    let stats = WorkloadStats::new(3);
    stats.record(0, RequestOutcome::HttpError, Duration::from_millis(1));
    assert!(!stats.any_success());
    assert_eq!(stats.failed(), 1);
}

#[test]
fn a_slow_overload_read_marks_every_window_until_it_finishes() {
    let stats = WorkloadStats::new(3);
    stats.begin_phase(); // overload
    let slow = stats.begin_read(0);
    stats.begin_phase(); // observation
    for _ in 0..3 {
        let regular = stats.begin_read(1);
        regular.finish(RequestOutcome::Success, Duration::from_millis(2));
        let window = stats.snapshot_and_reset();
        assert_eq!(window.total.successful, 1);
        assert_eq!(window.prior_phase_in_flight, 1);
        assert_eq!(window.prior_phase_finished, 0);
    }
    slow.finish(RequestOutcome::Success, Duration::from_secs(20));
    let completion_window = stats.snapshot_and_reset();
    assert_eq!(completion_window.total.successful, 1);
    assert_eq!(completion_window.prior_phase_in_flight, 0);
    assert_eq!(completion_window.prior_phase_finished, 1);
    let clean_window = stats.snapshot_and_reset();
    assert_eq!(clean_window.prior_phase_in_flight, 0);
    assert_eq!(clean_window.prior_phase_finished, 0);
}

#[test]
fn unfinished_reads_in_the_current_phase_do_not_block_recovery() {
    let stats = WorkloadStats::new(3);
    stats.begin_phase();
    let read = stats.begin_read(0);
    let window = stats.snapshot_and_reset();
    assert_eq!(window.prior_phase_in_flight, 0);
    assert_eq!(window.prior_phase_finished, 0);
    drop(read);
    assert!(stats.phase.lock().unwrap().in_flight.is_empty());
}

#[test]
fn cancellation_retires_old_reads_without_creating_a_success() {
    let stats = WorkloadStats::new(3);
    let read = stats.begin_read(0);
    stats.begin_phase();
    stats.begin_phase(); // a request can outlive more than one phase
    drop(read);
    let window = stats.snapshot_and_reset();
    assert_eq!(window.total.completed(), 0);
    assert_eq!(window.prior_phase_in_flight, 0);
    assert_eq!(window.prior_phase_finished, 1);
    assert_eq!(stats.snapshot_and_reset().prior_phase_finished, 0);
}

#[tokio::test(start_paused = true)]
async fn an_overloaded_primary_timeout_does_not_issue_a_backup_read() {
    let mut calls = Vec::new();
    let outcome = read_with_fallback(|backup| {
        calls.push(backup);
        async move {
            if backup {
                Ok(200) // A healthy backup must not hide a timed-out primary.
            } else {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Err(RequestFailure::Timeout)
            }
        }
    })
    .await;
    assert!(matches!(outcome, RequestOutcome::TransportError));
    assert_eq!(calls, vec![false]);
}

#[tokio::test]
async fn http_errors_and_body_failures_do_not_issue_backup_reads() {
    for response in [Ok(503), Ok(404), Err(RequestFailure::Other)] {
        let mut calls = Vec::new();
        let outcome = read_with_fallback(|backup| {
            calls.push(backup);
            std::future::ready(if backup { Ok(200) } else { response })
        })
        .await;
        assert!(!matches!(outcome, RequestOutcome::Success));
        assert_eq!(calls, vec![false]);
    }
}

#[tokio::test(start_paused = true)]
async fn a_connection_failure_tries_one_backup_and_records_one_logical_read() {
    let stats = WorkloadStats::new(3);
    let read = stats.begin_read(0);
    let started = tokio::time::Instant::now();
    let mut calls = Vec::new();
    let outcome = read_with_fallback(|backup| {
        calls.push(backup);
        async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if backup {
                Ok(200)
            } else {
                Err(RequestFailure::Connection)
            }
        }
    })
    .await;
    read.finish(outcome, started.elapsed());
    let sample = stats.snapshot_and_reset();
    assert_eq!(calls, vec![false, true]);
    assert_eq!(sample.total.completed(), 1);
    assert_eq!(sample.by_primary[0].successful, 1);
    assert_eq!(sample.by_primary[1].completed(), 0);
    assert!(sample.total.success_latency_p95_us.unwrap() >= 20_000);
}

#[tokio::test]
async fn a_failed_backup_is_terminal() {
    let mut calls = Vec::new();
    let outcome = read_with_fallback(|backup| {
        calls.push(backup);
        std::future::ready(Err(RequestFailure::Connection))
    })
    .await;
    assert!(matches!(outcome, RequestOutcome::TransportError));
    assert_eq!(calls, vec![false, true]);
}

#[test]
fn wall_clock_pacing_preserves_the_requested_rate() {
    let mut pacer = ArrivalPacer::default();
    let arrivals: usize = (0..1000)
        .map(|_| pacer.arrivals(9_500, Duration::from_millis(1)))
        .sum();
    assert_eq!(arrivals, 9_500);
}

#[test]
fn a_rate_change_does_not_carry_fractional_credit_into_the_next_phase() {
    let mut pacer = ArrivalPacer::default();
    assert_eq!(pacer.arrivals(150, Duration::from_millis(1)), 0);
    assert_eq!(pacer.arrivals(1_000, Duration::from_millis(1)), 1);
}

#[test]
fn admitted_and_shed_demand_are_distinct_from_completions() {
    let stats = WorkloadStats::new(3);
    let read = stats.begin_admitted_read(0);
    stats.record_shed(0);
    let offered = stats.snapshot_and_reset();
    assert_eq!(offered.total.offered, 2);
    assert_eq!(offered.total.admitted, 1);
    assert_eq!(offered.total.shed, 1);
    assert_eq!(offered.total.completed(), 0);
    assert_eq!(offered.in_flight, 1);

    read.finish(RequestOutcome::Success, Duration::from_millis(10));
    let completed = stats.snapshot_and_reset();
    assert_eq!(completed.total.offered, 0);
    assert_eq!(completed.total.successful, 1);
    assert_eq!(completed.in_flight, 0);
}
