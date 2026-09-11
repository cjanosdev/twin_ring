import * as fs from "fs";
import * as path from "path";
import { buildAnnotations } from "../lib/annotations.mjs";
import { getCanvas } from "../lib/canvas.mjs";
import { clientLoadSeries } from "../lib/client_load.mjs";

export async function renderClientLoad(faultStart, faultEnd, strategies, summaries, outDir) {
  const datasets = [];
  for (const strategy of strategies) {
    const series = clientLoadSeries(summaries[strategy]);
    if (!series) continue;
    for (const [field, color, dash] of [
      ["offered", "#111827", [8, 4]],
      ["admitted", "#2563eb", [4, 3]],
      ["successful", "#16a34a", []],
      ["shed", "#dc2626", []],
    ]) {
      datasets.push({
        label: `${strategy} ${field}`,
        data: series[field],
        borderColor: color,
        backgroundColor: "transparent",
        borderWidth: field === "successful" ? 2 : 1.5,
        borderDash: dash,
        pointRadius: 0,
        spanGaps: true,
      });
    }
  }
  if (!datasets.length) return;

  const config = {
    type: "line",
    data: { datasets },
    options: {
      animation: false,
      plugins: {
        title: { display: true, text: "Client demand and completed service" },
        legend: { position: "top" },
        annotation: { annotations: buildAnnotations(faultStart, faultEnd, {}, "", "") },
      },
      scales: {
        x: { type: "linear", title: { display: true, text: "Elapsed seconds" } },
        y: { title: { display: true, text: "Requests / second" }, beginAtZero: true },
      },
    },
  };

  const buffer = await getCanvas(1200, 500).renderToBuffer(config);
  const outPath = path.join(outDir, "client_load.png");
  fs.writeFileSync(outPath, buffer);
  console.log(`  ✓ ${outPath}`);
}
