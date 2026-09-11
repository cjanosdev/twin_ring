import { STRATEGY_COLORS } from "./load_csv.mjs";

/**
 * Build Chart.js annotation objects for:
 *   - Red shaded box across the overload or legacy fault window
 *   - Dashed vertical line at the start of recovery observation
 *   - Horizontal dashed baseline reference lines (one per strategy)
 *
 * @param {number|null} faultStart  - elapsed_sec where fault begins
 * @param {number|null} faultEnd    - elapsed_sec where recovery observation begins
 * @param {object}      summaries   - Map of strategy → sidecar summary object (or null)
 * @param {string}      baselineKey - key in summary to use for the horizontal line
 *                                    e.g. "baseline_db_p50_us", "baseline_cache_p50_us", "baseline_throughput_rps"
 * @param {string}      baselineLabel - suffix for the annotation label, e.g. "db_p50 baseline"
 * @returns {object} annotations object suitable for chartjs-plugin-annotation
 */
export function buildAnnotations(faultStart, faultEnd, summaries, baselineKey, baselineLabel) {
  const annotations = {};

  // Fault window — red shaded box
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
        content: Object.values(summaries).some(s => s?.scenario === "node-outage") ? "overload + node outage" : "overload / fault",
        font: { size: 11 },
        color: "#b91c1c",
        backgroundColor: "rgba(255,255,255,0)",
      },
    };
  }

  // Observation start — dashed vertical line
  if (faultEnd !== null) {
    annotations["node_restart"] = {
      type: "line",
      xMin: faultEnd,
      xMax: faultEnd,
      borderColor: "#475569",
      borderWidth: 1,
      borderDash: [4, 4],
      label: {
        enabled: true,
        content: "observation begins",
        position: "start",
        font: { size: 11 },
        color: "#475569",
      },
    };
  }

  // Baseline horizontal lines — one per strategy that has a sidecar summary
  for (const [strategy, summary] of Object.entries(summaries)) {
    if (!summary || summary[baselineKey] == null) continue;
    const color = STRATEGY_COLORS[strategy] ?? "#888";
    annotations[`baseline_${strategy}`] = {
      type: "line",
      yMin: summary[baselineKey],
      yMax: summary[baselineKey],
      borderColor: color,
      borderWidth: 1,
      borderDash: [6, 3],
      label: {
        enabled: true,
        content: `${strategy} ${baselineLabel}`,
        position: "end",
        font: { size: 10 },
        color,
        backgroundColor: "rgba(255,255,255,0.7)",
        padding: 2,
      },
    };
  }

  return annotations;
}
