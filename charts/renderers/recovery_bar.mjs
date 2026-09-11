import * as fs from "fs";
import * as path from "path";
import { STRATEGY_COLORS } from "../lib/load_csv.mjs";
import { getCanvas } from "../lib/canvas.mjs";
import { recoverySummary } from "../lib/recovery_summary.mjs";

/** First sustained recovery time, with the runner's final outcome under each bar. */
export async function renderRecoveryBar(strategies, summaries, outDir) {
  if (!strategies.some(s => summaries[s] != null)) {
    console.log("  ⚠ recovery_bar.png skipped — no sidecar summaries found");
    return;
  }
  const results = strategies.map(s => recoverySummary(summaries[s]));
  const canvas = getCanvas(Math.max(800, strategies.length * 240), 500);
  const config = {
    type: "bar",
    data: {
      labels: strategies.map((s, i) => [s, results[i].status,
        ...(results[i].relapses ? [`${results[i].relapses} relapse(s)`] : []),
        results[i].seconds === null ? "No measured recovery time" : `${results[i].seconds.toFixed(1)}s`]),
      datasets: [{
        label: "First recovery confirmation (seconds)",
        data: results.map(r => r.seconds),
        backgroundColor: strategies.map(s => STRATEGY_COLORS[s] ?? "#888"),
        borderWidth: 0,
      }],
    },
    options: {
      animation: false,
      plugins: {
        title: { display: true, text: "First recovery confirmation and final outcome" },
        subtitle: { display: true, text: "Empty bar = no measured recovery time; see outcome below" },
        legend: { display: false },
        tooltip: { callbacks: { label: ctx => `${results[ctx.dataIndex].status}: ${ctx.raw}s` } },
      },
      scales: {
        x: { ticks: { autoSkip: false, maxRotation: 0 } },
        y: { title: { display: true, text: "Seconds after recovery observation reference" },
          beginAtZero: true, suggestedMax: Math.max(10, ...results.map(r => r.seconds ?? 0)) },
      },
    },
  };
  const buf = await canvas.renderToBuffer(config);
  const outPath = path.join(outDir, "recovery_bar.png");
  fs.writeFileSync(outPath, buf);
  console.log(`  ✓ ${outPath}`);
}
