export function clientLoadSeries(summary) {
  const measurements = Array.isArray(summary?.measurements) ? summary.measurements : [];
  if (!measurements.length || !measurements.some((row) => row.client?.total?.offered != null)) {
    return null;
  }
  const t0 = Math.min(...measurements.map((row) => row.timestamp_ms));
  const point = (row, field) => ({
    x: Math.round((row.timestamp_ms - t0) / 1000),
    y: Number(row.client?.total?.[field] ?? 0) / Number(row.duration_secs),
  });
  const valid = measurements.filter((row) => Number(row.duration_secs) > 0 && row.client?.total);
  return {
    offered: valid.map((row) => point(row, "offered")),
    admitted: valid.map((row) => point(row, "admitted")),
    successful: valid.map((row) => point(row, "successful")),
    shed: valid.map((row) => point(row, "shed")),
  };
}
