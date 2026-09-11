import * as fs from "fs";
import * as path from "path";
import { extractSeries } from "../lib/pivot.mjs";
import { STRATEGY_COLORS } from "../lib/load_csv.mjs";
import { buildAnnotations } from "../lib/annotations.mjs";
import { getCanvas } from "../lib/canvas.mjs";

export async function renderCacheLatency(bySecond, faultStart, faultEnd, strategies, summaries, outDir) {
  const { datasets } = extractSeries(bySecond, strategies, "cache_p50_us");
  const annotations = buildAnnotations(faultStart, faultEnd, summaries, "baseline_cache_p50_us", "cache_p50 baseline");

  const config = {
    type: "line",
    data: {
      datasets: datasets.map(d => ({
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
        title: { display: true, text: "Cache latency p50 over time (µs)" },
        legend: { position: "top" },
        annotation: { annotations },
      },
      scales: {
        x: { type: "linear", title: { display: true, text: "Elapsed seconds" } },
        y: { title: { display: true, text: "cache_p50 (µs)" }, beginAtZero: true },
      },
    },
  };

  const buf = await getCanvas(1200, 500).renderToBuffer(config);
  const outPath = path.join(outDir, "cache_latency.png");
  fs.writeFileSync(outPath, buf);
  console.log(`  ✓ ${outPath}`);
}
