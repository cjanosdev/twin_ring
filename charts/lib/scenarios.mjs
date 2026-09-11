export function runScenario({ rows, summary }) {
  if (summary?.scenario) return summary.scenario;
  if (rows.some(r => r.phase === "node_outage")) return "node-outage";
  if (rows.some(r => r.phase === "fault_inject")) return "legacy-fault";
  return "overload";
}

// The chart data is indexed by strategy. Mixing scenarios or repeated strategies
// would silently merge separate experiments under one label.
export function validateComparableRuns(datasets) {
  if (new Set(datasets.map(runScenario)).size > 1) {
    throw new Error("These runs use different scenarios. Generate their charts separately.");
  }
  if (new Set(datasets.map(d => d.strategy)).size !== datasets.length) {
    throw new Error("Choose one run per strategy to avoid merging independent measurements.");
  }
}
