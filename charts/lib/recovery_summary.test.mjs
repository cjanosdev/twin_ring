import test from "node:test";
import assert from "node:assert/strict";
import { recoverySummary } from "./recovery_summary.mjs";

test("no recovery time is fabricated for prevention, incomplete evidence, or failure", () => {
  for (const outcome of ["resisted_degradation", "inconclusive", "did_not_recover_within_observation"]) {
    const result = recoverySummary({ outcome, time_to_recovery_secs: null });
    assert.equal(result.seconds, null);
    assert.doesNotMatch(result.status, /never/i);
  }
  assert.equal(recoverySummary({ outcome: "resisted_degradation", time_to_recovery_secs: 0 }).seconds, null);
});

test("a first recovery remains visible with the final outcome and relapse count", () => {
  const result = recoverySummary({ outcome: "recovered_then_relapsed",
    time_to_recovery_secs: 35, relapse_times_ms: [100_000] });
  assert.deepEqual(result, { seconds: 35, status: "Recovered, then relapsed", relapses: 1 });
  assert.equal(recoverySummary({ outcome: "inconclusive", time_to_recovery_secs: 35 }).status, "Inconclusive");
});

test("historical and missing summaries are explicitly distinguished", () => {
  assert.deepEqual(recoverySummary(null), { seconds: null, status: "No summary", relapses: 0 });
  assert.equal(recoverySummary({ time_to_recovery_secs: -1 }).seconds, null);
  assert.equal(recoverySummary({ time_to_recovery_secs: 10 }).status, "Legacy recovery criteria");
});
