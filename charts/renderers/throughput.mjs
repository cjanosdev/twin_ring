import * as fs from "fs";
import * as path from "path";
import { extractSeries } from "../lib/pivot.mjs";
import { STRATEGY_COLORS } from "../lib/load_csv.mjs";
import { buildAnnotations } from "../lib/annotations.mjs";
import { getCanvas } from "../lib/canvas.mjs";

export async function renderThroughput(bySecond, faultStart, faultEnd, strategies, summaries, outDir) {
  const { datasets } = extractSeries(bySecond, strategies, "throughput_rps");
  const annotations = buildAnnotations(faultStart, faultEnd, summaries, "baseline_throughput_rps", "throughput baseline");

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
        title: { display: true, text: "Throughput over time (RPS, all nodes combined)" },
        legend: { position: "top" },
        annotation: { annotations },
      },
      scales: {
        x: { type: "linear", title: { display: true, text: "Elapsed seconds" } },
        y: { title: { display: true, text: "Requests / second" }, beginAtZero: true },
      },
    },
  };

  const buf = await getCanvas(1200, 500).renderToBuffer(config);
  const outPath = path.join(outDir, "throughput.png");
  fs.writeFileSync(outPath, buf);
  console.log(`  ✓ ${outPath}`);
}
