# twin_ring

A research project for studying **metastable failures** in distributed caching systems.

Metastable failure: a system enters a degraded state (high latency, low cache hit rate) and **cannot self-recover** even after the original fault is resolved. The feedback loop:

```
cold cache → every request hits Cassandra → Cassandra saturates
           → cache fills too slowly to keep up with TTL expiry
           → cache stays cold → repeat forever
```

---

## Architecture

```
Experiment binary (cargo run)
    │  HTTP GET /get/{key}
    ▼
cache_node_1 :8001  ─┐
cache_node_2 :8002  ─┤  key-based routing (key % 6 → primary node)  →  Cassandra :9042
cache_node_3 :8003  ─┘  in-memory TTL cache, 300ms Cassandra driver timeout

control :9000   (POST /kill/{node_id}, POST /start/{node_id}, GET /cassandra-mem)
```

**Crates:**

| Crate | Role |
|---|---|
| `twin_ring_core` | Pluggable cache backends (`CacheBackend` trait + 5 impls), CSV logger, experiment output paths |
| `twin_ring_node` | Cache node HTTP server (Actix-web) + Cassandra client + `/stats` + `/replicate` endpoints; control API |
| `twin_ring_exp` | Experiment binaries + shared `metrics` module (StatsPoller, MetricsWriter, run_phase) |

---

## Cache Strategies

Five interchangeable cache backends, selected per cluster via `--cache-strategy`:

| Strategy | Flag | What it studies |
|---|---|---|
| LRU (control) | `lru` | Baseline metastable behavior — uniform TTL, LRU eviction |
| Dual-ring | `dual-ring` | Proactive hot-key replication to backup nodes. On failure, backup promotes its replicas into main cache — collapses miss storms. |
| TTL-tiered | `ttl-tiered` | One shared LRU store. Cold (0–1 hits): TTL; Warm (2–7 hits): 2×TTL; Hot (8+ hits): 4×TTL. Deadlines are anchored to the last fill; replacements reset popularity. |
| Leased | `leased` | Per-key fill lease: one request fetches Cassandra per key; concurrent requests receive a retryable response and do not query Cassandra. Prevents thundering-herd amplification. |
| Combined | `combined` | Hot-key replication + access-count tier signal. Lease behavior is disabled pending a dedicated rewrite, so this is not yet an all-mitigations strategy. |

All five implement the same `CacheBackend` trait. Experiment binaries work over HTTP and never see the cache type directly.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Rust toolchain](https://rustup.rs/)
- Node.js ≥ 18 (for the chart tool and dashboard)

---

## Running experiments end-to-end

### Step 1 — Start the cluster

**First time, or after wiping data** (runs schema init + preloads 100k keys into Cassandra):

```bash
docker compose -f docker/docker-compose-baseline.yml down -v
docker compose -f docker/docker-compose-baseline.yml up --build -d
```

**Subsequent runs** (reuses the existing volume — Cassandra data already there):

```bash
docker compose -f docker/docker-compose-baseline.yml up -d
```

Cassandra takes 60–90 seconds to start. The cache nodes and preload service wait automatically. Watch readiness with:

```bash
docker compose -f docker/docker-compose-baseline.yml logs -f cassandra-init
# exits when schema and preload are done
```

Verify the cluster is healthy:

```bash
curl http://localhost:8001          # → "TwinRing node alive!"
curl http://localhost:8001/get/key42
curl http://localhost:9000/cassandra-mem
```

---

### Step 2 — Choose a cache strategy and restart the cluster nodes

The docker-compose file starts nodes with `--cache-strategy lru` by default. To test a different strategy, set `CACHE_STRATEGY` before bringing the cluster up, or edit the compose file. Each experiment binary must match the strategy the cluster is running.

For **dual-ring** or **combined**, the nodes also need `NODE_INDEX`, `CLUSTER_MEMBERS`, and `PEER_URLS` environment variables (already wired in docker-compose for those strategies). `CLUSTER_MEMBERS` is the shared consistent-hash membership snapshot, such as `0,1,2`; `CLUSTER_SIZE` remains a backwards-compatible default when it is absent. The experiment client uses the matching `TR_RING_MEMBERS` value.

---

### Step 3 — Run the per-strategy experiment

Each binary runs the full **warmup → fault_inject → observe** protocol autonomously — no separate baseline step needed.

```bash
# LRU control (cluster must be running --cache-strategy lru)
cargo run -p twin_ring_exp --bin exp_lru

# Dual-ring
cargo run -p twin_ring_exp --bin exp_dual

# TTL-tiered
cargo run -p twin_ring_exp --bin exp_ttl

# Leased
cargo run -p twin_ring_exp --bin exp_leased

# Combined (all three mitigations)
cargo run -p twin_ring_exp --bin exp_combined
```

Each run writes two files to `experiment_results/runs/<date>/`:

```
lru_metastable_143022.csv              # per-window metrics (all phases)
lru_metastable_143022_summary.json     # inline baseline + recovery timestamps
```

**Config via env vars** (all optional — defaults shown):

```bash
TR_WARMUP_SECS=60          # warmup phase duration
TR_FAULT_DOWN_SECS=60      # how long node 1 stays down
TR_FAULT_WORKERS=2000      # worker surge during fault
TR_OBSERVE_SECS=120        # observe phase duration
TR_KEY_SPACE=100000        # number of distinct keys
TR_POLL_INTERVAL_SECS=5    # stats polling frequency
TR_CASSANDRA_MEM_THRESHOLD_PCT=80.0
```

---

### Step 4 — Generate charts

Install deps once:

```bash
cd charts && npm install
```

**Single run** — charts go into a subfolder named after the CSV:

```bash
node charts/chart.mjs --csv experiment_results/runs/2026-05-18/lru_metastable_143022.csv
# → experiment_results/runs/2026-05-18/lru_metastable_143022/
#     throughput.png
#     cache_latency.png
#     db_latency.png
#     recovery_bands.png
#     recovery_bar.png
```

**Cross-strategy comparison** — pass multiple `--csv` flags; charts go into a timestamped `comparison_HHMMSS/` folder:

```bash
node charts/chart.mjs \
  --csv experiment_results/runs/2026-05-18/lru_metastable_143022.csv \
  --csv experiment_results/runs/2026-05-18/dual-ring_metastable_143500.csv \
  --csv experiment_results/runs/2026-05-18/ttl-tiered_metastable_144000.csv \
  --csv experiment_results/runs/2026-05-18/leased_metastable_144500.csv \
  --csv experiment_results/runs/2026-05-18/combined_metastable_145000.csv \
  --out experiment_results/runs/2026-05-18/comparison/
```

**Charts produced:**

| File | What it shows |
|---|---|
| `throughput.png` | RPS (all nodes combined) over time; dashed lines at phase transitions |
| `cache_latency.png` | `cache_p50_us` over time |
| `db_latency.png` | `db_p50_us` over time — spikes mark Cassandra saturation |
| `recovery_bands.png` | Horizontal state bands per strategy: red=down, yellow=degraded, green=recovered |
| `recovery_bar.png` | Time-to-recovery in seconds per strategy (requires sidecar `_summary.json`) |

---

### Optional: Legacy experiments

These older binaries still work and don't require the per-strategy cluster:

```bash
# Find Cassandra's raw saturation point (no cache layer)
cargo run -p twin_ring_exp --bin baseline_no_cache
# → experiment_results/baseline_no_cache.json

# Measure the healthy cached baseline (requires baseline.json to exist first)
cargo run -p twin_ring_exp --bin baseline
# → experiment_results/baseline.json

# Original metastable experiment (reads baseline.json)
cargo run -p twin_ring_exp --bin simple_metastable
# → experiment_results/runs/<date>/simple_metastable_HHMMSS.csv
```

---

### Optional: Dashboard

Live charts, topology view, config panel, and runs browser:

```bash
bash dashboard/run.sh
# → http://localhost:5173
```

The dashboard can also launch experiment binaries and stream their output live. All 5 per-strategy experiments appear in the registry picker.

---

## Experiment Protocol

Each per-strategy experiment (`exp_lru`, `exp_dual`, etc.) runs three phases:

**Phase 1 — WARMUP (default 60s)**
Workers ramp from 15 to 150 (+15 every 10s). Caches fill via Zipfian access (s=0.95 over 100k keys). The last 20s of this phase is used as the **inline baseline** — median `cache_p50`, median `db_p50`, mean `hit_rate`, mean `throughput_rps`.

**Phase 2 — FAULT_INJECT (default 60s)**
Node 1 is killed. Workers surge to 2,000. Node 1 stays down for the full window (multiple TTL cycles), forcing nodes 2+3 to decay via TTL expiry. The Cassandra memory threshold is monitored; if it hits 80%, the fault phase drains out the remainder of the window.

**Phase 3 — OBSERVE (default 120s)**
Node 1 restarts cold. All three nodes are cold simultaneously. Recovery state is tracked per polling window:
- **down**: `db_errors > 0` AND `hit_rate < 0.30`
- **degraded**: `hit_rate < baseline − 0.20` OR `db_p50 > baseline × 3`
- **recovered**: `hit_rate ≥ baseline − 0.10` AND `db_p50 ≤ baseline × 2`

First-transition timestamps are latched (not reset on dip back into degraded). `time_to_recovery_secs` is written to the sidecar JSON.

**Metastable verdict**: `db_p50_ratio ≥ 20×` AND `hit_rate drop ≥ 30pp`

---

## Key Design Decisions

**Key-based routing (not shard-based)**
Each key maps to a primary node via `key % 6` (6 virtual slots, 2 per node). The key itself determines routing — any worker can send any key. On node failure, traffic fails over to the next node in the slot's failover order.

**Failover on connection error only, not on 503**
HTTP 503 = node alive but Cassandra is slow → worker stays on the same node (preserves locality during slowdowns). TCP error = node truly down → try next in failover order.

**`cache_p50` as the metastable signal, not `cache_p99`**
At ≥90% hit rate, `cache_p50` is almost always a cache hit (~1ms). During metastable failure at ~60% hit rate, `cache_p50` flips to miss latency (~50ms+). Ratio: 50×+, well above the 20× threshold. `cache_p99` is already dominated by miss latency at baseline, giving only 2–3× during failure.

**Inline baseline (no external baseline.json)**
Each experiment measures its own warmup tail and computes baseline metrics inline. No separate pre-run step required.

**300ms driver timeout (apples-to-apples)**
Same timeout in both the cache node and `baseline_no_cache`. When Cassandra takes >300ms, the driver returns `Err` → `DbResult::Error` → HTTP 503.

**REQUEST_TIMEOUT_MS (4000ms) > driver timeout (300ms)**
Prevents the experiment client (reqwest) from timing out before the node returns its 503. If reqwest times out first, the worker sees a connection error and incorrectly fails over.

---

## Experiment Metrics (CSV columns)

Each row = one polling window (default 5s) from one node:

| Column | Description |
|---|---|
| `timestamp_ms` | Unix timestamp in milliseconds |
| `phase` | `warmup`, `fault_inject`, or `observe` |
| `node` | Node URL (e.g. `http://localhost:8001`) |
| `hits` / `misses` | Cache hits and misses in this window |
| `db_hits` / `db_not_found` / `db_errors` | Cassandra outcomes — `db_errors` spiking = saturation = metastable signal |
| `hit_rate` | `hits / (hits + misses)` |
| `throughput_rps` | Total GET requests per second |
| `db_call_rate` | Cassandra queries per second — spikes when cache is cold |
| `cache_p50_us` / `cache_p99_us` | Full request latency percentiles (µs) |
| `db_p50_us` / `db_p99_us` | Cassandra query latency percentiles (µs) |
| `live_entries` | Non-expired entries currently in the cache |

---

## Infrastructure Config

| Setting | Value | Why |
|---|---|---|
| Cache TTL | 30s | Short enough to decay during 60s fault window; long enough for ≥90% hit rate |
| max-entries per node | 33,000 | ~33k keys/node across 100k keyspace with 3 nodes |
| Driver timeout | 300ms | `Err` when Cassandra slow → `db_error` counted; same in cache node and no-cache baseline |
| Cassandra `concurrent_reads` | 4 | Limits read thread pool — saturates under fault load |
| Cassandra heap | 256M / 128M new | GC pressure → slower queries under load |
| Fault workers | 2,000 | Pushes Cassandra past the saturation point found in `baseline_no_cache` |

---

## Common Issues

**Cassandra takes too long to start**
Cassandra takes 60–90 seconds. The `cassandra-init` service waits automatically via `depends_on: condition: service_healthy`.

**Start fresh (wipe Cassandra data)**
```bash
docker compose -f docker/docker-compose-baseline.yml down -v
docker compose -f docker/docker-compose-baseline.yml up --build -d
```

**Port already in use**
```bash
lsof -i :8001
kill <PID>
```

**`canvas` native build fails (chart tool)**
The chart tool requires Cairo. Install with: `brew install cairo pango pkg-config`
