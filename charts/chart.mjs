#!/usr/bin/env node
/**
 * twin-ring chart tool
 *
 * Usage:
 *   # Single run — charts written to a subfolder named after the CSV:
 *   node chart.mjs --csv experiment_results/runs/20260518/lru_metastable_143022.csv
 *
 *   # Multi-run comparison — charts written to --out dir (or auto-created comparison_HHMMSS/):
 *   node chart.mjs \
 *     --csv .../lru_metastable_143022.csv \
 *     --csv .../dual-ring_metastable_143500.csv \
 *     --out .../comparison/
 *
 * Produces: throughput.png, client_load.png (fixed-rate runs), cache_latency.png,
 * db_latency.png, hit_rate.png, db_errors.png, recovery_bar.png
 */

import * as fs   from "fs";
import * as path from "path";
import { loadCsv }     from "./lib/load_csv.mjs";
import { loadSummary } from "./lib/load_summary.mjs";
import { validateComparableRuns } from "./lib/scenarios.mjs";
import { pivot }       from "./lib/pivot.mjs";
import { renderThroughput }   from "./renderers/throughput.mjs";
import { renderClientLoad }   from "./renderers/client_load.mjs";
import { renderCacheLatency } from "./renderers/cache_latency.mjs";
import { renderDbLatency }    from "./renderers/db_latency.mjs";
import { renderHitRate }      from "./renderers/hit_rate.mjs";
import { renderDbErrors }     from "./renderers/db_errors.mjs";
import { renderRecoveryBar }  from "./renderers/recovery_bar.mjs";

// ── Parse CLI args ─────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const csvPaths = [];
let outDir = null;

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--csv" && args[i + 1]) {
    csvPaths.push(path.resolve(args[++i]));
  } else if (args[i] === "--out" && args[i + 1]) {
    outDir = path.resolve(args[++i]);
  } else if (args[i] === "--help" || args[i] === "-h") {
    printHelp();
    process.exit(0);
  }
}

if (!csvPaths.length) {
  console.error("Error: at least one --csv <path> is required.");
  printHelp();
  process.exit(1);
}

for (const p of csvPaths) {
  if (!fs.existsSync(p)) {
    console.error(`Error: CSV not found: ${p}`);
    process.exit(1);
  }
}

// ── Determine output directory ─────────────────────────────────────────────────

const isComparison = csvPaths.length > 1;

if (!outDir) {
  if (isComparison) {
    const now = new Date();
    const hms = now.toTimeString().slice(0, 8).replace(/:/g, "");
    outDir = path.join(path.dirname(csvPaths[0]), `comparison_${hms}`);
  } else {
    const base = path.basename(csvPaths[0], ".csv");
    outDir = path.join(path.dirname(csvPaths[0]), base);
  }
}

fs.mkdirSync(outDir, { recursive: true });
console.log(`\n📊 Charts → ${outDir}`);
console.log(`   Mode: ${isComparison ? "multi-run comparison" : "single-run"}`);

// ── Load data ──────────────────────────────────────────────────────────────────

const datasets = csvPaths.map(p => {
  const { rows, strategy, filename } = loadCsv(p);
  console.log(`   Loaded ${rows.length} rows  [${strategy}]  ${filename}`);
  return { rows, strategy, csvPath: p, summary: loadSummary(p) };
});

validateComparableRuns(datasets);
const summaries = {};
for (const { strategy, summary } of datasets) {
  summaries[strategy] = summary;
  if (summaries[strategy]) {
    console.log(`   Sidecar loaded for ${strategy}`);
  }
}

// ── Pivot ──────────────────────────────────────────────────────────────────────

const { bySecond, faultStart, faultEnd, strategies } = pivot(datasets);
console.log(`\n   Elapsed range: 0 – ${Math.max(...bySecond.keys())}s`);
console.log(`   Strategies:    ${strategies.join(", ")}`);
if (faultStart !== null) console.log(`   Fault window:  ${faultStart}s – ${faultEnd ?? "?"}s`);

// ── Render ─────────────────────────────────────────────────────────────────────

console.log("\nRendering...");

await renderThroughput(bySecond, faultStart, faultEnd, strategies, summaries, outDir);
await renderClientLoad(faultStart, faultEnd, strategies, summaries, outDir);
await renderCacheLatency(bySecond, faultStart, faultEnd, strategies, summaries, outDir);
await renderDbLatency(bySecond, faultStart, faultEnd, strategies, summaries, outDir);
await renderHitRate(bySecond, faultStart, faultEnd, strategies, summaries, outDir);
await renderDbErrors(bySecond, faultStart, faultEnd, strategies, summaries, outDir);
await renderRecoveryBar(strategies, summaries, outDir);

console.log(`\n✅ Done. ${outDir}`);

// ── Help ───────────────────────────────────────────────────────────────────────

function printHelp() {
  console.log(`
twin-ring chart tool — produce PNGs from experiment CSV files

Usage:
  node chart.mjs --csv <path> [--csv <path> ...] [--out <dir>]

Options:
  --csv <path>   CSV file from an experiment run (repeatable for comparison)
  --out <dir>    Output directory (optional; auto-named if omitted)
  --help         Show this help

Examples:
  # Single run:
  node chart.mjs --csv experiment_results/runs/2026-05-18/lru_metastable_143022.csv

  # Cross-strategy comparison:
  node chart.mjs \\
    --csv .../lru_metastable_143022.csv \\
    --csv .../dual-ring_metastable_143500.csv \\
    --csv .../combined_metastable_144000.csv \\
    --out .../comparison/
`);
}
