import * as fs from "fs";
import * as path from "path";
import { extractSeries } from "../lib/pivot.mjs";
import { STRATEGY_COLORS } from "../lib/load_csv.mjs";
import { buildAnnotations } from "../lib/annotations.mjs";
import { getCanvas } from "../lib/canvas.mjs";

export async function renderDbErrors(bySecond, faultStart, faultEnd, strategies, summaries, outDir) {
  const { datasets } = extractSeries(bySecond, strategies, "db_errors");
  const annotations = buildAnnotations(faultStart, faultEnd, summaries, null, null);

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
        fill: false,
      })),
    },
    options: {
      animation: false,
      plugins: {
        title: { display: true, text: "DB errors per window (Cassandra saturation signal)" },
        legend: { position: "top" },
        annotation: { annotations },
      },
      scales: {
        x: { type: "linear", title: { display: true, text: "Elapsed seconds" } },
        y: { title: { display: true, text: "DB errors" }, beginAtZero: true },
      },
    },
  };

  const buf = await getCanvas(1200, 500).renderToBuffer(config);
  const outPath = path.join(outDir, "db_errors.png");
  fs.writeFileSync(outPath, buf);
  console.log(`  ✓ ${outPath}`);
}
