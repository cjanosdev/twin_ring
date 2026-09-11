# Experiment scenarios

Each run uses one cache strategy and one scenario, with its own regular-work
baseline. LRU is the control. Neither scenario changes routing membership or
moves replicas in the experiment runner. Cache implementations are unchanged.

Both scenarios begin with warmup (default 90 seconds) and regular work
(default 90 seconds, using complete windows in the final 60 seconds for the
baseline). The workload is arrival-rate controlled: it ramps from 1,000 to
7,000 offered requests/s during warmup, holds 7,000 requests/s during regular
work, and ramps to 20,000 requests/s during the default 90-second overload.
Response latency does not reduce the configured arrival rate.

- `overload`: restore regular load, then observe recovery for 180 seconds.
- `node-outage`: stop the selected cache node after elevated load has run; hold
  elevated load while it is down for `TR_OUTAGE_SECS` (default 60 seconds);
  restore regular load **before** requesting restart; then observe recovery.

Run one scenario against nodes already running the matching strategy:

```sh
cargo run -p twin_ring_exp --bin exp -- --strategy lru --scenario overload
cargo run -p twin_ring_exp --bin exp -- --strategy lru --scenario node-outage
```

`TR_SCENARIO` also selects the scenario; an explicit `--scenario` takes
precedence. The default is `overload`. `TR_OUTAGE_NODE` selects the one-based
node ID (1, 2, or 3; default 1). For example:

```sh
TR_OUTAGE_NODE=2 TR_OUTAGE_SECS=45 cargo run -p twin_ring_exp --bin exp -- --strategy lru --scenario node-outage
```

The shell runner accepts the same settings. For example:

```sh
TR_REGULAR_RPS=7000 TR_OVERLOAD_RPS=20000 TR_SCENARIO=overload bash run_experiments.sh lru
TR_REGULAR_RPS=7000 TR_OVERLOAD_RPS=20000 TR_SCENARIO=node-outage bash run_experiments.sh lru
```

`TR_MAX_IN_FLIGHT` is a safety bound (default 12,000). An arrival above that
bound is recorded as shed demand instead of disappearing. The console and JSON
measurements distinguish offered, admitted, shed, completed, and successful
requests. A system has not recovered while its shed rate exceeds 1%, even if
the smaller admitted workload has low errors and latency.

The former `TR_REGULAR_WORKERS`, `TR_OVERLOAD_WORKERS`,
`TR_WARMUP_START_WORKERS`, `TR_WARMUP_RAMP_STEP`, and `TR_FAULT_WORKERS`
settings belonged to the removed closed-loop generator and are rejected. There
is no exact worker-to-rate conversion because the old offered rate changed with
response latency.

The dashboard offers Overload and Overload + Node Outage entries for each
strategy, including node and outage-duration controls. These are the same two
scenarios as the command-line choices.

## Recovery evidence

Client reads are still attributed to their primary key partition during an
outage. A non-timeout connection error permits one backup attempt; timeouts and
HTTP errors do not. The expected stopped node's missing statistics are allowed
only during `node_outage`. Both survivors must supply aligned measurements, and
client success, latency and errors are still checked for all three partitions.
Expected missing statistics alone do not prove service degradation.

After restart, all three nodes must supply fresh measurements again. Recovery
requires three complete consecutive healthy windows using the recorded
throughput, latency and error thresholds. Requests left over from an earlier
phase cannot advance that streak. Unknown measurements remain explicit.

For overload, the recovery clock starts when regular load is restored. For node
outage, it starts when the control API acknowledges restart; readiness probing
runs alongside observation, so startup time remains part of recovery time.
The summary records both regular-load restoration and stop/restart/readiness
timestamps. A successful restart requires the node to report the expected
strategy, not merely return any HTTP response.

The result distinguishes recovery, relapse, no degradation observed, no recovery
within the observation interval, and insufficient evidence. The finite run does
not establish that recovery is impossible forever.

Warmup measurements never contribute to the baseline. The baseline uses only
complete windows wholly inside the configured tail of `regular_work`, after the
cache has warmed and while the offered request rate remains fixed. Missing or delayed
measurements, zero successful reads, missing latency, and excessive request or
database errors abort before disruption.

Request and database error limits are calculated over the complete baseline
period for each node. A brief window may exceed 1% as long as the aggregate
baseline rate remains at or below 1%. A rejected baseline reports its error
count, total completed reads or database calls, measured rate, and limit.

Valid regular-work variation becomes part of the reference. For each node the
summary records typical throughput (duration-weighted mean), typical client p95
and DB p99 latency (medians), the lowest observed throughput, and the highest
observed latencies. Recovery criteria are not applied to baseline windows.
After disruption, throughput must reach 90% of the observed regular-work lower
bound, while latency must stay within 2x the observed regular-work upper bound.
The 1% request and DB error limits continue to apply directly. All nodes must
meet those conditions, admit at least 99% of offered demand, and do so for three
consecutive complete observation windows.

Baseline quality also checks that each complete window's measured offered rate
is within 5% of `TR_REGULAR_RPS`. This verifies that the load generator itself
kept up. It does not compare baseline performance to recovery thresholds.

## Cleanup and outputs

The runner attempts restart and readiness verification after an error, Ctrl+C,
or SIGTERM if it may have stopped a node. A lost stop response still requires
cleanup. Forced process termination such as SIGKILL cannot run this cleanup.
Restoration errors are reported alongside the original error.

Outputs use `<strategy>_<scenario>_<time>.csv`, a `_summary.json` sidecar,
and `_measurements.jsonl`. The JSONL begins with the strategy and full settings,
then flushes each complete measurement round (including client counters,
latencies and phase alignment) as it arrives. It remains useful after an error
or cancellation, even without a completed summary. This fixes the previous loss
of client measurements whenever baseline validation aborted.

Errors after measurement starts also write an aborted, inconclusive summary
with the error, settings and measurements collected so far. Preflight failures
create no run files. Cancellation or forced termination may leave only the CSV
and JSONL. An aborted summary's outage events describe the state before the
outer cleanup attempt; its restart outcome is reported by the runner.
The summary records the scenario, actual outage events, fallback policy and
recovery-evaluation version. Generate charts for different scenarios separately;
the chart tool rejects mixed scenarios or duplicate strategy labels to avoid
merging independent measurements.
