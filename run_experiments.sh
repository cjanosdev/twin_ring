#!/usr/bin/env bash
# Run cache strategy experiments sequentially.
#
# Usage:
#   ./run_experiments.sh                          # all 5 strategies
#   ./run_experiments.sh lru dual-ring combined   # specific strategies
#
# Each strategy:
#   1. Restarts the 3 cache nodes with CACHE_STRATEGY set (Cassandra untouched)
#   2. Waits for all nodes to be healthy
#   3. Runs the selected TR_SCENARIO (overload or node-outage); charts follow.
#
# After all strategies complete, generates a cross-strategy comparison chart.

set -euo pipefail

COMPOSE_FILE="docker/docker-compose-baseline.yml"

# Docker Desktop on macOS uses a user-scoped socket, not /var/run/docker.sock.
# Export DOCKER_SOCK so the compose file can mount the right path into the control container.
if [[ "$(uname)" == "Darwin" ]]; then
  export DOCKER_SOCK="${HOME}/.docker/run/docker.sock"
else
  export DOCKER_SOCK="/var/run/docker.sock"
fi
ALL_STRATEGIES=("lru" "dual-ring" "ttl-tiered" "leased" "combined")
STRATEGIES=("${@:-${ALL_STRATEGIES[@]}}")

# ── Helpers ───────────────────────────────────────────────────────────────────

validate_strategy() {
  case "$1" in
    lru|dual-ring|ttl-tiered|leased|combined) ;;
    *) echo "Unknown strategy: $1" >&2; exit 1 ;;
  esac
}

wait_for_node() {
  local url="$1"
  local deadline=$((SECONDS + 30))
  echo -n "    Waiting for $url ..."
  while [ $SECONDS -lt $deadline ]; do
    if curl -sf "$url" > /dev/null 2>&1; then
      echo " ready"
      return 0
    fi
    sleep 1
    echo -n "."
  done
  echo ""
  echo "ERROR: $url did not come up within 30s" >&2
  exit 1
}

# ── Validate strategies ───────────────────────────────────────────────────────

for s in "${STRATEGIES[@]}"; do
  validate_strategy "$s"
done

echo "======================================================================"
echo "  TwinRing experiment runner"
echo "  Strategies: ${STRATEGIES[*]}"
echo "======================================================================"

# ── Build experiment binaries once up front ───────────────────────────────────

echo ""
echo "Building experiment binaries..."
cargo build -p twin_ring_exp --bins --release 2>&1 | grep -E "Compiling|Finished|error"
echo ""

# ── Run each strategy ─────────────────────────────────────────────────────────

CSV_PATHS=()
DATE=$(date +%Y-%m-%d)

for strategy in "${STRATEGIES[@]}"; do
  echo "======================================================================"
  echo "  Strategy: $strategy"
  echo "======================================================================"
  echo ""

  # Start with empty cache processes even when repeating the same strategy.
  # --no-deps leaves the already-initialized Cassandra and control service alone.
  echo "  Restarting cache nodes with CACHE_STRATEGY=$strategy..."
  CACHE_STRATEGY="$strategy" docker compose -f "$COMPOSE_FILE" up -d --force-recreate --no-deps \
    cache_node_1 cache_node_2 cache_node_3

  # Wait for all 3 nodes to be healthy
  wait_for_node "http://localhost:8001"
  wait_for_node "http://localhost:8002"
  wait_for_node "http://localhost:8003"
  echo ""

  # Run the experiment, tee output so we can grep the CSV path
  tmplog=$(mktemp)
  cargo run -p twin_ring_exp --bin exp --release -- --strategy "$strategy" 2>&1 | tee "$tmplog"

  # Extract CSV path from the "Metrics →" line printed by run_experiment()
  csv_path=$(grep -o 'experiment_results/runs/[^ ]*\.csv' "$tmplog" | head -1)
  rm -f "$tmplog"

  if [ -n "$csv_path" ]; then
    CSV_PATHS+=("$csv_path")
    echo ""
    echo "  CSV: $csv_path"
  else
    echo "  WARNING: could not detect CSV path from experiment output" >&2
  fi

  echo ""
done

# ── Comparison chart ──────────────────────────────────────────────────────────

if [ ${#CSV_PATHS[@]} -gt 1 ]; then
  echo "======================================================================"
  echo "  Generating cross-strategy comparison charts"
  echo "======================================================================"

  TIMESTAMP=$(date +%H%M%S)
  OUT_DIR="experiment_results/runs/${DATE}/comparison_${TIMESTAMP}"
  mkdir -p "$OUT_DIR"

  CSV_ARGS=()
  for p in "${CSV_PATHS[@]}"; do
    CSV_ARGS+=("--csv" "$p")
  done

  node charts/chart.mjs "${CSV_ARGS[@]}" --out "$OUT_DIR"
  echo ""
  echo "  Comparison charts → $OUT_DIR"
fi

echo ""
echo "======================================================================"
echo "  Done. Ran: ${STRATEGIES[*]}"
echo "======================================================================"
