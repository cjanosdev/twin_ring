use super::*;

fn settings(values: &[(&str, &str)]) -> Result<ExperimentSettings> {
    ExperimentSettings::from_reader(|key| {
        Ok(values
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value.to_string()))
    })
}

#[test]
fn defaults_define_a_complete_fixed_rate_protocol() {
    let cfg = settings(&[]).unwrap();
    assert_eq!(cfg.regular_rps, 7_000);
    assert_eq!(cfg.overload_rps, 20_000);
    assert_eq!(cfg.max_in_flight, 12_000);
    assert_eq!(cfg.warmup_ramp_secs().unwrap(), 60);
    assert_eq!(cfg.warmup_secs, 90);
    assert_eq!(cfg.poll_secs(), 5.0);
}

#[test]
fn old_worker_settings_fail_with_a_migration_message() {
    for key in [
        "TR_REGULAR_WORKERS",
        "TR_WARMUP_START_WORKERS",
        "TR_WARMUP_RAMP_STEP",
        "TR_OVERLOAD_WORKERS",
        "TR_FAULT_WORKERS",
    ] {
        let error = settings(&[(key, "70")]).unwrap_err().to_string();
        assert!(error.contains(key), "{error}");
        assert!(error.contains("closed-loop worker model"), "{error}");
    }
}

#[test]
fn malformed_values_do_not_silently_use_defaults() {
    for (key, value) in [
        ("TR_KEY_SPACE", "many"),
        ("TR_REGULAR_RPS", ""),
        ("TR_WARMUP_SECS", "-1"),
        ("TR_MAX_IN_FLIGHT", "many"),
        ("TR_POLL_INTERVAL_SECS", "five"),
        ("TR_REQUEST_TIMEOUT_MS", "500ms"),
    ] {
        let error = settings(&[(key, value)]).unwrap_err().to_string();
        assert!(error.contains(key), "{error}");
    }
}

#[test]
fn partial_final_ramp_step_requires_a_full_interval() {
    let cfg = settings(&[
        ("TR_REGULAR_RPS", "3500"),
        ("TR_WARMUP_START_RPS", "1000"),
        ("TR_WARMUP_RAMP_STEP_RPS", "1000"),
    ])
    .unwrap();
    assert_eq!(cfg.warmup_ramp_secs().unwrap(), 30);
}

#[test]
fn starting_at_regular_rate_needs_no_ramp() {
    let cfg = settings(&[("TR_WARMUP_START_RPS", "7000")]).unwrap();
    assert_eq!(cfg.warmup_ramp_secs().unwrap(), 0);
}

#[test]
fn zero_counts_and_durations_are_rejected() {
    for key in [
        "TR_KEY_SPACE",
        "TR_REGULAR_RPS",
        "TR_WARMUP_START_RPS",
        "TR_WARMUP_RAMP_STEP_RPS",
        "TR_MAX_IN_FLIGHT",
        "TR_WARMUP_SECS",
        "TR_WARMUP_RAMP_STEP_SECS",
        "TR_BASELINE_WINDOW_SECS",
        "TR_OVERLOAD_SECS",
        "TR_OVERLOAD_RAMP_STEP_SECS",
        "TR_REGULAR_WORK_SECS",
        "TR_OVERLOAD_RAMP_STEP_RPS",
        "TR_OBSERVE_SECS",
        "TR_POLL_INTERVAL_SECS",
        "TR_REQUEST_TIMEOUT_MS",
    ] {
        assert!(settings(&[(key, "0")]).is_err(), "accepted {key}=0");
    }
}

#[test]
fn inconsistent_load_levels_are_rejected() {
    assert!(settings(&[("TR_WARMUP_START_RPS", "9501")]).is_err());
    assert!(settings(&[("TR_OVERLOAD_RPS", "7000")]).is_err());
    assert!(settings(&[("TR_OVERLOAD_RPS", "6500")]).is_err());
}

#[test]
fn baseline_must_fit_and_contain_complete_polling_intervals() {
    assert!(settings(&[("TR_BASELINE_WINDOW_SECS", "4")]).is_err());
    assert!(settings(&[("TR_BASELINE_WINDOW_SECS", "90")]).is_err());
    let cfg = settings(&[]).unwrap();
    assert_eq!(cfg.regular_work_secs, 90);
    assert_eq!(cfg.baseline_window_secs, 60);
}

#[test]
fn invalid_membership_is_rejected() {
    for value in ["", "0", "0,0", "0,1,1", "0,1,3", "0,x,2", "0,,2", "0,1,"] {
        let error = settings(&[("TR_RING_MEMBERS", value)])
            .unwrap_err()
            .to_string();
        assert!(error.contains("TR_RING_MEMBERS"), "{error}");
    }
    assert_eq!(
        settings(&[("TR_RING_MEMBERS", "2, 0, 1")])
            .unwrap()
            .ring_members,
        vec![2, 0, 1]
    );
}

#[test]
fn overload_duration_keeps_its_old_duration_alias() {
    assert_eq!(
        settings(&[("TR_FAULT_DOWN_SECS", "30")])
            .unwrap()
            .overload_secs,
        30
    );
    assert_eq!(
        settings(&[("TR_OVERLOAD_SECS", "40"), ("TR_FAULT_DOWN_SECS", "bad")])
            .unwrap()
            .overload_secs,
        40
    );
}

#[test]
fn ramp_arithmetic_overflow_is_reported() {
    let too_long = u64::MAX.to_string();
    assert!(
        settings(&[("TR_WARMUP_RAMP_STEP_SECS", &too_long)])
            .unwrap_err()
            .to_string()
            .contains("too large")
    );
}

#[test]
fn environment_read_errors_are_preserved() {
    let error = ExperimentSettings::from_reader(|_| anyhow::bail!("cannot read environment"))
        .unwrap_err()
        .to_string();
    assert_eq!(error, "cannot read environment");
}

#[test]
fn recovery_thresholds_are_validated_before_starting_load() {
    for (name, value) in [
        ("TR_RECOVERY_MIN_GOODPUT_RATIO", "0"),
        ("TR_RECOVERY_MIN_GOODPUT_RATIO", "1.1"),
        ("TR_RECOVERY_MAX_LATENCY_RATIO", "0.9"),
        ("TR_RECOVERY_MAX_REQUEST_ERROR_RATE", "NaN"),
        ("TR_RECOVERY_MAX_SHED_RATE", "1.1"),
        ("TR_RECOVERY_MAX_DB_ERROR_RATE", "-0.1"),
        ("TR_RECOVERY_CONSECUTIVE_WINDOWS", "0"),
        ("TR_RECOVERY_LATENCY_FLOOR_US", "0"),
        ("TR_BASELINE_OFFERED_RPS_TOLERANCE", "1"),
    ] {
        let error = settings(&[(name, value)]).unwrap_err().to_string();
        assert!(error.contains(name), "{error}");
    }
}

#[test]
fn scenarios_are_explicit_and_outage_settings_are_validated() {
    assert_eq!(settings(&[]).unwrap().scenario, Scenario::Overload);
    assert!(settings(&[("TR_SCENARIO", "both")]).is_err());
    let cfg = settings(&[
        ("TR_SCENARIO", "node-outage"),
        ("TR_OUTAGE_NODE", "3"),
        ("TR_OUTAGE_SECS", "45"),
        ("TR_OVERLOAD_SECS", "105"),
    ])
    .unwrap();
    assert_eq!(cfg.outage_node, 3);
    assert_eq!(cfg.outage_secs, 45);
}

#[test]
fn outage_waits_for_the_elevated_rate_ramp_to_finish() {
    assert!(settings(&[("TR_SCENARIO", "node-outage"), ("TR_OVERLOAD_SECS", "39")]).is_err());
    assert!(settings(&[("TR_SCENARIO", "node-outage"), ("TR_OVERLOAD_SECS", "40")]).is_ok());
}
