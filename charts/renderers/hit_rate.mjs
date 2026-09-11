import * as fs from "fs";
import * as path from "path";
import { extractSeries } from "../lib/pivot.mjs";
import { STRATEGY_COLORS } from "../lib/load_csv.mjs";
import { buildAnnotations } from "../lib/annotations.mjs";
import { getCanvas } from "../lib/canvas.mjs";

export async function renderHitRate(bySecond, faultStart, faultEnd, strategies, summaries, outDir) {
  const { datasets } = extractSeries(bySecond, strategies, "hit_rate");

  // Convert 0–1 hit_rate to percentage
  const pctDatasets = datasets.map(d => ({
    ...d,
    data: d.data.map(pt => ({ x: pt.x, y: +(pt.y * 100).toFixed(1) })),
  }));

  // Baseline horizontal lines at each strategy's baseline hit rate (as %)
  const pctSummaries = {};
  for (const [strategy, summary] of Object.entries(summaries)) {
    if (!summary) { pctSummaries[strategy] = null; continue; }
    pctSummaries[strategy] = {
      ...summary,
      baseline_hit_rate_pct: +(summary.baseline_hit_rate * 100).toFixed(1),
    };
  }

  const annotations = buildAnnotations(
    faultStart, faultEnd, pctSummaries, "baseline_hit_rate_pct", "hit rate baseline"
  );

  const config = {
    type: "line",
    data: {
      datasets: pctDatasets.map(d => ({
        label: d.strategy,
        data: d.data,
        borderColor: STRATEGY_COLORS[d.strategy] ?? "#888",
        backgroundColor: "transparent",
        borderWidth: 2,
        pointRadius: 0,
        spanGaps: true,
      })),
    },
    options: {
      animation: false,
      plugins: {
        title: { display: true, text: "Cache hit rate over time (%)" },
        legend: { position: "top" },
        annotation: { annotations },
      },
      scales: {
        x: { type: "linear", title: { display: true, text: "Elapsed seconds" } },
        y: {
          title: { display: true, text: "Hit rate (%)" },
          min: 0, max: 100,
        },
      },
    },
  };

  const buf = await getCanvas(1200, 500).renderToBuffer(config);
  const outPath = path.join(outDir, "hit_rate.png");
  fs.writeFileSync(outPath, buf);
  console.log(`  ✓ ${outPath}`);
}
