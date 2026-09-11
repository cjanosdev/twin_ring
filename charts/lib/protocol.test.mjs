import test from "node:test";
import assert from "node:assert/strict";
import { pivot } from "./pivot.mjs";
import { buildAnnotations } from "./annotations.mjs";

const row = (phase, timestamp_ms) => ({ phase, timestamp_ms, throughput_rps: 100,
  cache_p50_us: 10, db_p50_us: 100, hit_rate: 0.9, db_errors: 0 });

test("overload-only CSV marks overload and restoration of regular load", () => {
  const result = pivot([{ strategy: "lru", rows: [row("warmup", 0),
    row("regular_work", 90_000), row("overload", 150_000), row("observe", 240_000)] }]);
  assert.equal(result.faultStart, 150);
  assert.equal(result.faultEnd, 240);
});

test("historical fault CSV remains readable", () => {
  const result = pivot([{ strategy: "lru", rows: [row("warmup", 0),
    row("fault_inject", 60_000), row("observe", 150_000)] }]);
  assert.equal(result.faultStart, 60);
  assert.equal(result.faultEnd, 150);
});

test("summary measurements preserve the observation boundary when all nodes are missing", () => {
  const rows = [row("warmup", 0), row("regular_work", 90_000), row("overload", 150_000)];
  const measurements = [...rows, { phase: "observe", timestamp_ms: 240_000,
    nodes: [], missing_nodes: ["node1", "node2", "node3"] }];
  const result = pivot([{ strategy: "lru", rows, summary: { measurements } }]);
  assert.equal(result.faultStart, 150);
  assert.equal(result.faultEnd, 240);
  const annotations = buildAnnotations(result.faultStart, result.faultEnd, {}, null, null);
  assert.equal(annotations.fault_window.xMin, 150);
  assert.equal(annotations.fault_window.xMax, 240);
});

test("phase annotations do not claim a node was stopped or restarted", () => {
  const annotations = buildAnnotations(150, 240, {}, "baseline_hit_rate", "baseline");
  assert.equal(annotations.fault_window.label.content, "overload / fault");
  assert.equal(annotations.node_restart.label.content, "observation begins");
});
