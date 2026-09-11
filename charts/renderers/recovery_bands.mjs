import * as fs from "fs";
import * as path from "path";
import { getCanvas } from "../lib/canvas.mjs";

export async function renderRecoveryBands(bySecond, faultStart, faultEnd, strategies, summaries, outDir) {
  const allSecs = [...bySecond.keys()].sort((a, b) => a - b);
  if (!allSecs.length) return;

  const height = 500;

  const STATE_COLORS = { down: "#f87171", degraded: "#fbbf24", recovered: "#4ade80", normal: "#e2e8f0" };

  const bandData = strategies.map(strategy => {
    const summary = summaries[strategy];
    const baseHR  = summary?.baseline_hit_rate  ?? null;
    const baseDB  = summary?.baseline_db_p50_us ?? null;

    return allSecs.map(sec => {
      const point = bySecond.get(sec)?.get(strategy);
      if (!point) return "normal";
      const { hit_rate, db_p50_us, db_errors } = point;
      if (db_errors > 0 && hit_rate < 0.30) return "down";
      if (baseHR !== null && (hit_rate < baseHR - 0.20 || (baseDB && db_p50_us > baseDB * 3))) return "degraded";
      if (baseHR !== null && hit_rate >= baseHR - 0.10 && (baseDB === null || db_p50_us <= baseDB * 2)) return "recovered";
      return "normal";
    });
  });

  const annotations = {};
  if (faultStart !== null && faultEnd !== null) {
    annotations["fault_window"] = {
      type: "box",
      xMin: faultStart,
      xMax: faultEnd,
      backgroundColor: "rgba(248, 113, 113, 0.12)",
      borderColor: "rgba(248, 113, 113, 0.4)",
      borderWidth: 1,
      label: {
        enabled: true,
        content: "node 1 down",
        font: { size: 11 },
        color: "#b91c1c",
        backgroundColor: "rgba(255,255,255,0)",
      },
    };
  }
  if (faultEnd !== null) {
    annotations["node_restart"] = {
      type: "line",
      xMin: faultEnd, xMax: faultEnd,
      borderColor: "#475569",
      borderWidth: 1,
      borderDash: [4, 4],
      label: { enabled: true, content: "node 1 restart", position: "start", font: { size: 11 }, color: "#475569" },
    };
  }

  const chartDatasets = strategies.map((strategy, si) => ({
    label: strategy,
    data: allSecs.map((sec, i) => ({ x: sec, y: si, state: bandData[si][i] })),
    backgroundColor: allSecs.map((_, i) => STATE_COLORS[bandData[si][i]] ?? "#e2e8f0"),
    borderWidth: 0,
    barThickness: 28,
  }));

  const config = {
    type: "bar",
    data: { labels: allSecs, datasets: chartDatasets },
    options: {
      animation: false,
      indexAxis: "y",
      plugins: {
        title: { display: true, text: "Recovery state timeline (red=down, yellow=degraded, green=recovered)" },
        legend: { display: false },
        tooltip: { enabled: false },
        annotation: { annotations },
      },
      scales: {
        x: { title: { display: true, text: "Elapsed seconds" }, stacked: false },
        y: { type: "category", labels: strategies, title: { display: true, text: "Strategy" } },
      },
    },
  };

  const buf = await getCanvas(1200, 500).renderToBuffer(config);
  const outPath = path.join(outDir, "recovery_bands.png");
  fs.writeFileSync(outPath, buf);
  console.log(`  ✓ ${outPath}`);
}
