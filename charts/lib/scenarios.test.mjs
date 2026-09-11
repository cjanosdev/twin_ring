import test from "node:test";
import assert from "node:assert/strict";
import { runScenario, validateComparableRuns } from "./scenarios.mjs";
import { pivot } from "./pivot.mjs";
import { buildAnnotations } from "./annotations.mjs";

const run = (strategy, scenario) => ({ strategy, summary: { scenario }, rows: [] });
test("different scenarios and duplicate strategies cannot silently share chart buckets", () => {
  assert.throws(() => validateComparableRuns([run("lru", "overload"), run("leased", "node-outage")]), /different scenarios/);
  assert.throws(() => validateComparableRuns([run("lru", "overload"), run("lru", "overload")]), /one run per strategy/);
  assert.doesNotThrow(() => validateComparableRuns([run("lru", "node-outage"), run("leased", "node-outage")]));
});
test("historical phases keep their own scenario identity", () => {
  assert.equal(runScenario({ rows: [{ phase: "fault_inject" }] }), "legacy-fault");
  assert.equal(runScenario({ rows: [{ phase: "node_outage" }] }), "node-outage");
});
test("outage timeline begins at elevated load and ends at post-restart observation", () => {
  const phases = ["warmup", "regular_work", "overload", "stopping_node", "node_outage", "restarting_node", "observe"];
  const rows = phases.map((phase, i) => ({phase, timestamp_ms: i * 10_000, throughput_rps: 1, cache_p50_us: 1, db_p50_us: 1, hit_rate: 1, db_errors: 0}));
  const result = pivot([{strategy: "lru", rows}]);
  assert.equal(result.faultStart, 20);
  assert.equal(result.faultEnd, 60);
  const labels = buildAnnotations(20, 60, {lru: {scenario: "node-outage"}}, "baseline_hit_rate", "baseline");
  assert.equal(labels.fault_window.label.content, "overload + node outage");
});
