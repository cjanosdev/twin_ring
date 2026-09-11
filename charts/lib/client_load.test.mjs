import assert from "node:assert/strict";
import test from "node:test";
import { clientLoadSeries } from "./client_load.mjs";

test("client load keeps offered, admitted, successful, and shed rates distinct", () => {
  const series = clientLoadSeries({ measurements: [
    { timestamp_ms: 10_000, duration_secs: 5, client: { total: {
      offered: 100, admitted: 80, successful: 70, shed: 20,
    } } },
    { timestamp_ms: 15_000, duration_secs: 5, client: { total: {
      offered: 200, admitted: 200, successful: 190, shed: 0,
    } } },
  ] });
  assert.deepEqual(series.offered, [{ x: 0, y: 20 }, { x: 5, y: 40 }]);
  assert.deepEqual(series.admitted, [{ x: 0, y: 16 }, { x: 5, y: 40 }]);
  assert.deepEqual(series.successful, [{ x: 0, y: 14 }, { x: 5, y: 38 }]);
  assert.deepEqual(series.shed, [{ x: 0, y: 4 }, { x: 5, y: 0 }]);
});

test("older summaries without full client measurements are skipped", () => {
  assert.equal(clientLoadSeries(null), null);
  assert.equal(clientLoadSeries({ measurements: [] }), null);
  assert.equal(clientLoadSeries({ measurements: [
    { timestamp_ms: 1, duration_secs: 1, client: { total: { successful: 1 } } },
  ] }), null);
});
