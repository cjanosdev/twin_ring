/**
 * Aggregate CSV rows into per-elapsed-second, per-strategy buckets.
 *
 * Input:  array of { rows, strategy, summary? } objects (one per CSV file)
 * Output: { bySecond, faultStart, faultEnd, phaseLines, strategies }
 *
 *   bySecond:   Map<elapsed_sec, Map<strategy, { throughput_rps, cache_p50_us, db_p50_us, hit_rate, db_errors }>>
 *   faultStart: elapsed_sec where overload (or legacy fault_inject) begins (null if not found)
 *   faultEnd:   elapsed_sec where recovery observation begins (null if not found)
 *   phaseLines: [[elapsed_sec, label], ...] for any remaining transitions
 *   strategies: ordered list of distinct strategy names found
 */
export function pivot(datasets) {
  // Compute per-dataset t0 so each experiment's elapsed time starts at 0
  // independently, regardless of when it was run.
  const t0ByStrategy = new Map();
  for (const { rows, strategy } of datasets) {
    let t0 = Infinity;
    for (const row of rows) {
      if (row.timestamp_ms < t0) t0 = row.timestamp_ms;
    }
    t0ByStrategy.set(strategy, t0);
  }

  const raw = new Map();
  let faultStart = null;
  let faultEnd   = null;

  // Phase boundaries come from the complete measurement history when a
  // summary is available. The node-oriented CSV has no row for a polling
  // round in which every node was missing, but that round still has a phase.
  for (const { rows, strategy, summary } of datasets) {
    const t0 = t0ByStrategy.get(strategy);
    let lastPhase = null;
    const phaseRows = Array.isArray(summary?.measurements)
      ? summary.measurements
      : rows;
    for (const row of phaseRows) {
      if (row.phase === lastPhase) continue;
      const sec = Math.round((row.timestamp_ms - t0) / 1000);
      if ((["overload", "node_outage", "stopping_node", "fault_inject"].includes(row.phase)) && faultStart === null) faultStart = sec;
      if (row.phase === "observe" && faultEnd === null) faultEnd = sec;
      lastPhase = row.phase;
    }

    for (const row of rows) {
      const sec = Math.round((row.timestamp_ms - t0) / 1000);
      if (!raw.has(sec)) raw.set(sec, new Map());
      const stratMap = raw.get(sec);
      if (!stratMap.has(strategy)) stratMap.set(strategy, []);
      stratMap.get(strategy).push(row);

    }
  }

  // Aggregate: sum throughput, median latencies, mean hit_rate per (sec, strategy)
  const bySecond = new Map();
  for (const [sec, stratMap] of raw) {
    const agg = new Map();
    for (const [strategy, rowList] of stratMap) {
      const cache_p50s = rowList.map(r => r.cache_p50_us).filter(v => v > 0);
      const db_p50s    = rowList.map(r => r.db_p50_us).filter(v => v > 0);
      agg.set(strategy, {
        throughput_rps: rowList.reduce((s, r) => s + r.throughput_rps, 0),
        cache_p50_us:   median(cache_p50s),
        db_p50_us:      median(db_p50s),
        hit_rate:       mean(rowList.map(r => r.hit_rate)),
        db_errors:      rowList.reduce((s, r) => s + r.db_errors, 0),
      });
    }
    bySecond.set(sec, agg);
  }

  const strategies = [...new Set(datasets.map(d => d.strategy))];

  return { bySecond, faultStart, faultEnd, strategies };
}

function median(arr) {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
}

function mean(arr) {
  if (!arr.length) return 0;
  return arr.reduce((s, v) => s + v, 0) / arr.length;
}

/**
 * Extract a time-series for a single metric from pivoted data.
 * Returns { datasets: [{ strategy, data: [{x, y}] }] }
 * Uses x/y point format so the linear x-axis aligns with numeric annotation coordinates.
 */
export function extractSeries(bySecond, strategies, metric) {
  const secs = [...bySecond.keys()].sort((a, b) => a - b);
  const datasets = strategies.map(strategy => ({
    strategy,
    data: secs
      .map(sec => {
        const val = bySecond.get(sec)?.get(strategy)?.[metric];
        return val != null ? { x: sec, y: val } : null;
      })
      .filter(Boolean),
  }));
  return { datasets };
}
