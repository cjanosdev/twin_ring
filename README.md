# twin_ring

A research project for studying **metastable failures** in distributed caching systems.

Metastable failure: a system enters a degraded state (high latency, low throughput, low cache hit rate) and **cannot self-recover** even after the original fault is resolved. The classic feedback loop:

```
cold cache → thundering herd on Cassandra → Cassandra saturates →
cache can't warm up → more thundering herd → (repeat)
```

## Architecture

```
Experiment binary (cargo run)
    │  HTTP GET /get/{key}
    ▼
cache_node_1 :8001  ─┐  shard 0 (keys 0 .. 33,333)
cache_node_2 :8002  ─┤  shard 1 (keys 33,334 .. 66,666)   (in-memory TTL cache, TTL=60s)
cache_node_3 :8003  ─┘  shard 2 (keys 66,667 .. 99,999)
                          │ cache miss → Cassandra query (800ms timeout)
                          ▼
                     Cassandra :9042
                     (cpu-capped to saturate under thundering herd)

control :9000   (POST /kill/{node_id}, POST /start/{node_id} — Docker control)
```

**Crates:**

| Crate | Role |
|---|---|
| `twin_ring_core` | In-memory TTL cache, CSV logger, experiment output path utilities |
| `twin_ring_node` | Cache node HTTP server (Actix-web) + Cassandra client + `/stats` endpoint |
| `twin_ring_exp` | Experiment binaries + shared `metrics` module (StatsPoller, MetricsWriter, run_phase) |

## How the Experiment Works

`simple_metastable` runs a 3-phase experiment to demonstrate metastable failure. Run `baseline` first to capture a stable baseline (used to detect metastability automatically).

### Phase 1 — Warmup

150 workers (50 per shard) ramp up gradually from 15 → 150 over 150s. Workers are sticky to their shard's home node. During the gradual ramp Cassandra is lightly loaded, so cache fills complete quickly and hit rate climbs to ~95%+. db_errors ≈ 0.

**Key design**: workers only fail over to the next node on a TCP connection error (node completely down). A `503` response — the "Cassandra timeout" signal — is treated as "node alive but busy, stay on it." This preserves shard isolation: during warmup, nodes never accidentally cache another shard's keys.

### Phase 2 — Fault Injection

Node 1 is killed. Its 50 workers (shard-0) fail over to nodes 2 and 3, which have **never cached a single shard-0 key** — 100% miss on all redirected traffic. An additional surge brings total concurrent workers to 250. With Cassandra already running at 0.3 CPUs, the thundering herd saturates it — fill requests start timing out at 800ms and returning 503, causing db_errors to spike. Node 1 stays down for 120s.

Node 1 then restarts with a cold cache into a still-saturated Cassandra.

### Phase 3 — Observe

The fault is resolved (node 1 is running again). The question: can the system recover?

**Metastable outcome**: Cassandra is too saturated to serve fills fast enough. Fill rate < expiry rate → the hot zone can't be reseeded → every request misses → Cassandra stays saturated → stuck. cache_p99 stays ≥ 20× baseline AND hit_rate stays ≥ 30pp below baseline.

**Recovery outcome**: fill rate > expiry rate → hot zone reseeds → hit rate climbs back to ~95% → db_errors drop to 0.

### The Metastability Math

The system is metastable when:

```
fill_rate < expiry_rate

fill_rate  = concurrent_fills × (1000 / fill_ms_under_load)
expiry_rate = hot_zone_keys / ttl_seconds
           = (KEY_SPACE/NUM_SHARDS × 0.20) / TTL
```

Current config (Cassandra cpus=0.3, TTL=60s, Zipfian 80/20):
- `expiry_rate` = (100,000 / 3 × 0.20) / 60 ≈ 111 keys/s
- Under thundering herd, Cassandra saturates quickly — fills queue up and time out at 800ms
- db_errors spike = the feedback loop is active

### Metastable Detection Criterion

The experiment compares against a pre-recorded `baseline.json`:
- `cache_p99 ≥ 20× baseline cache_p99` **AND**
- `hit_rate drop ≥ 30pp below baseline`

Run `baseline` before each experiment to capture current stable-state numbers.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine + Compose)
- [Rust toolchain](https://rustup.rs/) (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)

## Running

```bash
# 1. Tear down any previous run and start fresh
docker compose -f docker/docker-compose-baseline.yml down -v
docker compose -f docker/docker-compose-baseline.yml up --build

# 2. Wait until all nodes are ready (takes 60–90s for Cassandra to become healthy)
#    You should see: "TwinRing node alive!" on curl http://localhost:8001

# 3. Record a stable baseline (required before running the experiment)
cargo run -p twin_ring_exp --bin baseline

# 4. Run the metastable failure experiment
cargo run -p twin_ring_exp --bin simple_metastable
```

Results are written to `experiment_results/runs/<date>/simple_metastable_HHMMSS.csv`.

Alternatively, use the dashboard for a live view:

```bash
bash dashboard/run.sh   # starts backend (:8080) and frontend (:5173)
```

## Verifying the Cluster

```bash
# Each node returns "TwinRing node alive!"
curl http://localhost:8001
curl http://localhost:8002
curl http://localhost:8003

# First GET is a Cassandra miss (cache cold), subsequent hits are cache hits
curl http://localhost:8001/get/key42

# /stats returns per-window hit/miss/error counts and latency percentiles (resets on read)
curl http://localhost:8001/stats

# Control API: kill and restart a node
curl -X POST http://localhost:9000/kill/1
curl -X POST http://localhost:9000/start/1
```

## Useful Commands

| Command | What it does |
|---|---|
| `docker compose -f docker/docker-compose-baseline.yml down -v` | Stop and wipe all data (start fresh) |
| `docker compose -f docker/docker-compose-baseline.yml up --build` | Rebuild images and start |
| `docker compose -f docker/docker-compose-baseline.yml up --build -d` | Same, detached (background) |
| `docker compose -f docker/docker-compose-baseline.yml logs -f cache_node_1` | Follow logs for one service |
| `docker compose -f docker/docker-compose-baseline.yml ps` | Show running services and status |

## Experiment Metrics (CSV columns)

Each row represents one 5-second polling window from one node:

| Column | Description |
|---|---|
| `timestamp_ms` | Unix timestamp in milliseconds |
| `phase` | `warmup`, `fault_inject`, or `observe` |
| `node` | Node URL (e.g. `http://localhost:8001`) |
| `hits` / `misses` | Cache hit and miss counts in this window |
| `db_hits` / `db_not_found` / `db_errors` | Cassandra outcomes — `db_errors` spiking = Cassandra saturated = metastable signal |
| `hit_rate` | `hits / (hits + misses)` — primary health signal |
| `throughput_rps` | Total GET requests per second |
| `db_call_rate` | Cassandra calls per second — spikes when cache is cold |
| `cache_p50_us` / `cache_p99_us` | Cache request latency percentiles (microseconds) |
| `db_p50_us` / `db_p99_us` | Cassandra call latency percentiles (microseconds) |

## Infrastructure Config

Cassandra is deliberately constrained to make saturation reproducible:

| Setting | Value | Why |
|---|---|---|
| `cpus` | 0.3 | Slow enough that concurrent fills under thundering herd saturate the CPU |
| `MAX_HEAP_SIZE` | 128M | Keeps GC pressure high, slows reads further |
| `CONCURRENT_READS` | 4 | Limits Cassandra's read thread pool |
| `CONCURRENT_WRITES` | 4 | Limits Cassandra's write thread pool |
| Cache node TTL | 60s | Short TTL = faster expiry rate; combined with low fill rate → metastable condition |
| Cassandra timeout | 800ms | Node returns 503 after this; experiment client timeout (2s) is set higher so workers see the 503 rather than a TCP error |

## Common Issues

**Cassandra takes too long to start**

Cassandra takes 60–90 seconds to become healthy. The `cassandra-init` and `preload` services wait automatically. If `preload` exits with an error it will retry (`restart: on-failure`).

**`baseline.json` not found**

The experiment requires a baseline recorded from a stable cluster. Run `cargo run -p twin_ring_exp --bin baseline` before `simple_metastable`.

**Port already in use**

```bash
lsof -i :8001   # find what is using port 8001
kill <PID>
```

**Start fresh (wipe Cassandra data)**

```bash
docker compose -f docker/docker-compose-baseline.yml down -v
docker compose -f docker/docker-compose-baseline.yml up --build
```
