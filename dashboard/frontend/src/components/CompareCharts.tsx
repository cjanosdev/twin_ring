import { useRef, useCallback } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { CsvRow, NoCacheRow } from "../types";
import { downloadChartPng, downloadAllCharts } from "../chartDownload";

// ---- Public types ----

export interface CompareData {
  metastable: Array<{ label: string; rows: CsvRow[]; startMs: number | null }>;
  baseline_steady: Array<{ label: string; rows: CsvRow[]; startMs: number | null }>;
  baseline_no_cache: Array<{ label: string; rows: NoCacheRow[] }>;
}

interface Props {
  sources: CompareData;
}

// ---- Color palettes per run-type ----

const METASTABLE_COLORS = ["#ef4444", "#f97316", "#eab308"];
const BASELINE_STEADY_COLORS = ["#4ade80", "#22d3ee", "#a3e635"];
const NO_CACHE_COLORS = ["#fb923c", "#fbbf24", "#f472b6"];

function cycleColor(palette: string[], idx: number): string {
  return palette[idx % palette.length];
}

// ---- Data helpers ----

/** Average a CsvRow metric across nodes per elapsed-second bucket for one run. */
function avgByElapsed(
  rows: CsvRow[],
  startMs: number | null,
  getValue: (r: CsvRow) => number
): Array<{ elapsed: number; value: number }> {
  if (!startMs || rows.length === 0) return [];

  const sums = new Map<number, { sum: number; count: number }>();
  for (const row of rows) {
    const elapsed = Math.round((row.timestamp_ms - startMs) / 1000);
    const prev = sums.get(elapsed) ?? { sum: 0, count: 0 };
    sums.set(elapsed, { sum: prev.sum + getValue(row), count: prev.count + 1 });
  }

  return [...sums.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([elapsed, { sum, count }]) => ({ elapsed, value: sum / count }));
}

/**
 * Merge multiple per-run averaged series into a single array of chart points.
 * Each point has `{ elapsed, <runKey>: value, ... }`.
 * runKey is `run_${idx}` so it's a valid dataKey.
 */
interface MergedPoint {
  elapsed: number;
  [key: string]: number | undefined;
}

function mergeTimeSeries(
  series: Array<Array<{ elapsed: number; value: number }>>
): MergedPoint[] {
  const allElapsed = new Set<number>();
  for (const s of series) for (const p of s) allElapsed.add(p.elapsed);

  const sorted = [...allElapsed].sort((a, b) => a - b);
  return sorted.map((elapsed) => {
    const point: MergedPoint = { elapsed };
    series.forEach((s, i) => {
      const found = s.find((p) => p.elapsed === elapsed);
      if (found !== undefined) point[`run_${i}`] = found.value;
    });
    return point;
  });
}

/** No-cache runs: merge by workers, each run is a column `run_${idx}`. */
interface NoCachePoint {
  workers: number;
  [key: string]: number | undefined;
}

function mergeNoCacheSeries(
  runs: Array<NoCacheRow[]>,
  getValue: (r: NoCacheRow) => number
): NoCachePoint[] {
  const allWorkers = new Set<number>();
  for (const rows of runs) for (const r of rows) allWorkers.add(r.workers);

  const sorted = [...allWorkers].sort((a, b) => a - b);
  return sorted.map((workers) => {
    const point: NoCachePoint = { workers };
    runs.forEach((rows, i) => {
      const found = rows.find((r) => r.workers === workers);
      if (found !== undefined) point[`run_${i}`] = getValue(found);
    });
    return point;
  });
}

// ---- Shared sub-components ----

const TOOLTIP_STYLE: React.CSSProperties = {
  background: "#1e293b",
  border: "1px solid #334155",
  borderRadius: 6,
  fontSize: 11,
};

const EMPTY_PLACEHOLDER = (
  <div style={{
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    height: 180,
    color: "#334155",
    fontSize: 12,
    fontStyle: "italic",
  }}>No sources selected</div>
);

/** A time-series sub-chart (metastable + baseline_steady lines merged). */
function TimeSeriesSubChart({
  title,
  unit,
  yDomain,
  metastableSources,
  baselineSteadySources,
  getValue,
  chartRef,
}: {
  title: string;
  unit: string;
  yDomain?: [number | "auto", number | "auto"];
  metastableSources: CompareData["metastable"];
  baselineSteadySources: CompareData["baseline_steady"];
  getValue: (r: CsvRow) => number;
  chartRef: React.MutableRefObject<HTMLDivElement | null>;
}) {
  const metaSeries = metastableSources.map((s) =>
    avgByElapsed(s.rows, s.startMs, getValue)
  );
  const steadySeries = baselineSteadySources.map((s) =>
    avgByElapsed(s.rows, s.startMs, getValue)
  );

  // All series combined: meta first, then steady
  // Keyed: meta runs as run_0..run_N-1, steady as run_N..
  const allSeries = [...metaSeries, ...steadySeries];
  const data = mergeTimeSeries(allSeries);

  const hasData = data.length > 0;

  // Build legend items
  const legendItems: Array<{ key: string; label: string; color: string }> = [
    ...metastableSources.map((s, i) => ({
      key: `run_${i}`,
      label: s.label,
      color: cycleColor(METASTABLE_COLORS, i),
    })),
    ...baselineSteadySources.map((s, i) => ({
      key: `run_${metaSeries.length + i}`,
      label: s.label,
      color: cycleColor(BASELINE_STEADY_COLORS, i),
    })),
  ];

  return (
    <div ref={chartRef} style={{ background: "#0f172a", flex: 1, minWidth: 0 }}>
      {hasData ? (
        <ResponsiveContainer width="100%" height={180}>
          <LineChart data={data} margin={{ top: 4, right: 8, left: -12, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
            <XAxis
              dataKey="elapsed"
              tick={{ fontSize: 9, fill: "#64748b" }}
              tickFormatter={(v) => `${v}s`}
            />
            <YAxis
              domain={yDomain}
              tick={{ fontSize: 9, fill: "#64748b" }}
            />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              labelFormatter={(v) => `${v}s`}
              formatter={(value: number, name: string) => {
                const item = legendItems.find((li) => li.key === name);
                return [`${value.toFixed(2)} ${unit}`, item?.label ?? name];
              }}
            />
            <Legend
              wrapperStyle={{ fontSize: 9, paddingTop: 4 }}
              formatter={(value) => {
                const item = legendItems.find((li) => li.key === value);
                return (
                  <span style={{ color: item?.color ?? "#94a3b8", fontSize: 9 }}>
                    {item?.label ?? value}
                  </span>
                );
              }}
            />
            {legendItems.map((item) => (
              <Line
                key={item.key}
                type="monotone"
                dataKey={item.key}
                stroke={item.color}
                strokeWidth={2}
                dot={false}
                connectNulls
                isAnimationActive={false}
                name={item.key}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      ) : (
        EMPTY_PLACEHOLDER
      )}
      <div style={subChartLabelStyle}>Time-series (elapsed s) — {unit}</div>
    </div>
  );
}

/** A no-cache sub-chart (x = workers). */
function NoCacheSubChart({
  unit,
  runs,
  getValue,
  chartRef,
}: {
  unit: string;
  runs: CompareData["baseline_no_cache"];
  getValue: (r: NoCacheRow) => number;
  chartRef: React.MutableRefObject<HTMLDivElement | null>;
}) {
  const data = mergeNoCacheSeries(
    runs.map((r) => r.rows),
    getValue
  );

  const hasData = data.length > 0;

  const legendItems = runs.map((r, i) => ({
    key: `run_${i}`,
    label: r.label,
    color: cycleColor(NO_CACHE_COLORS, i),
  }));

  return (
    <div ref={chartRef} style={{ background: "#0f172a", flex: 1, minWidth: 0 }}>
      {hasData ? (
        <ResponsiveContainer width="100%" height={180}>
          <LineChart data={data} margin={{ top: 4, right: 8, left: -12, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
            <XAxis
              dataKey="workers"
              tick={{ fontSize: 9, fill: "#64748b" }}
              tickFormatter={(v) => `${v}w`}
            />
            <YAxis tick={{ fontSize: 9, fill: "#64748b" }} />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              labelFormatter={(v) => `${v} workers`}
              formatter={(value: number, name: string) => {
                const item = legendItems.find((li) => li.key === name);
                return [`${value.toFixed(2)} ${unit}`, item?.label ?? name];
              }}
            />
            <Legend
              wrapperStyle={{ fontSize: 9, paddingTop: 4 }}
              formatter={(value) => {
                const item = legendItems.find((li) => li.key === value);
                return (
                  <span style={{ color: item?.color ?? "#94a3b8", fontSize: 9 }}>
                    {item?.label ?? value}
                  </span>
                );
              }}
            />
            {legendItems.map((item) => (
              <Line
                key={item.key}
                type="monotone"
                dataKey={item.key}
                stroke={item.color}
                strokeWidth={2}
                dot={{ r: 2 }}
                connectNulls
                isAnimationActive={false}
                name={item.key}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      ) : (
        EMPTY_PLACEHOLDER
      )}
      <div style={subChartLabelStyle}>No-cache (workers) — {unit}</div>
    </div>
  );
}

// ---- Panel wrappers ----

/** Panel with two side-by-side sub-charts: time-series (left) + no-cache (right). */
function SplitPanel({
  title,
  unit,
  yDomain,
  sources,
  getValueCsv,
  getValueNoCache,
  panelRef,
  timeRef,
  noCacheRef,
  onDownloadPng,
}: {
  title: string;
  unit: string;
  yDomain?: [number | "auto", number | "auto"];
  sources: CompareData;
  getValueCsv: (r: CsvRow) => number;
  getValueNoCache: (r: NoCacheRow) => number;
  panelRef: React.MutableRefObject<HTMLDivElement | null>;
  timeRef: React.MutableRefObject<HTMLDivElement | null>;
  noCacheRef: React.MutableRefObject<HTMLDivElement | null>;
  onDownloadPng: () => void;
}) {
  const hasAnything =
    sources.metastable.some((s) => s.rows.length > 0) ||
    sources.baseline_steady.some((s) => s.rows.length > 0) ||
    sources.baseline_no_cache.some((s) => s.rows.length > 0);

  return (
    <div ref={panelRef} style={panelStyle}>
      <div style={panelHeaderStyle}>
        <span style={panelTitleStyle}>
          {title}
          <span style={unitSpanStyle}>({unit})</span>
        </span>
        <button style={dlBtnStyle} onClick={onDownloadPng}>↓ PNG</button>
      </div>
      {hasAnything ? (
        <div style={{ display: "flex", gap: 8 }}>
          <TimeSeriesSubChart
            title={title}
            unit={unit}
            yDomain={yDomain}
            metastableSources={sources.metastable}
            baselineSteadySources={sources.baseline_steady}
            getValue={getValueCsv}
            chartRef={timeRef}
          />
          <NoCacheSubChart
            unit={unit}
            runs={sources.baseline_no_cache}
            getValue={getValueNoCache}
            chartRef={noCacheRef}
          />
        </div>
      ) : (
        EMPTY_PLACEHOLDER
      )}
    </div>
  );
}

/** Panel with only time-series sub-chart (no no-cache equivalent). */
function TimeOnlyPanel({
  title,
  unit,
  yDomain,
  sources,
  getValue,
  panelRef,
  chartRef,
  onDownloadPng,
}: {
  title: string;
  unit: string;
  yDomain?: [number | "auto", number | "auto"];
  sources: CompareData;
  getValue: (r: CsvRow) => number;
  panelRef: React.MutableRefObject<HTMLDivElement | null>;
  chartRef: React.MutableRefObject<HTMLDivElement | null>;
  onDownloadPng: () => void;
}) {
  const hasAnything =
    sources.metastable.some((s) => s.rows.length > 0) ||
    sources.baseline_steady.some((s) => s.rows.length > 0);

  return (
    <div ref={panelRef} style={panelStyle}>
      <div style={panelHeaderStyle}>
        <span style={panelTitleStyle}>
          {title}
          <span style={unitSpanStyle}>({unit})</span>
        </span>
        <button style={dlBtnStyle} onClick={onDownloadPng}>↓ PNG</button>
      </div>
      {hasAnything ? (
        <TimeSeriesSubChart
          title={title}
          unit={unit}
          yDomain={yDomain}
          metastableSources={sources.metastable}
          baselineSteadySources={sources.baseline_steady}
          getValue={getValue}
          chartRef={chartRef}
        />
      ) : (
        EMPTY_PLACEHOLDER
      )}
    </div>
  );
}

/** Panel with throughput: time-series left, no-cache ops/sec right. */
function ThroughputPanel({
  sources,
  panelRef,
  timeRef,
  noCacheRef,
  onDownloadPng,
}: {
  sources: CompareData;
  panelRef: React.MutableRefObject<HTMLDivElement | null>;
  timeRef: React.MutableRefObject<HTMLDivElement | null>;
  noCacheRef: React.MutableRefObject<HTMLDivElement | null>;
  onDownloadPng: () => void;
}) {
  const hasAnything =
    sources.metastable.some((s) => s.rows.length > 0) ||
    sources.baseline_steady.some((s) => s.rows.length > 0) ||
    sources.baseline_no_cache.some((s) => s.rows.length > 0);

  return (
    <div ref={panelRef} style={panelStyle}>
      <div style={panelHeaderStyle}>
        <span style={panelTitleStyle}>
          Throughput
          <span style={unitSpanStyle}>(RPS / ops/s)</span>
        </span>
        <button style={dlBtnStyle} onClick={onDownloadPng}>↓ PNG</button>
      </div>
      {hasAnything ? (
        <div style={{ display: "flex", gap: 8 }}>
          <TimeSeriesSubChart
            title="Throughput"
            unit="RPS"
            metastableSources={sources.metastable}
            baselineSteadySources={sources.baseline_steady}
            getValue={(r) => r.throughput_rps}
            chartRef={timeRef}
          />
          <NoCacheSubChart
            unit="ops/s"
            runs={sources.baseline_no_cache}
            getValue={(r) => r.ops_per_sec}
            chartRef={noCacheRef}
          />
        </div>
      ) : (
        EMPTY_PLACEHOLDER
      )}
    </div>
  );
}

// ---- Main export ----

export function CompareCharts({ sources }: Props) {
  // Panel refs (for download — we wrap the whole panel div)
  const dbP50PanelRef = useRef<HTMLDivElement | null>(null);
  const dbP50TimeRef  = useRef<HTMLDivElement | null>(null);
  const dbP50NcRef    = useRef<HTMLDivElement | null>(null);

  const dbP99PanelRef = useRef<HTMLDivElement | null>(null);
  const dbP99TimeRef  = useRef<HTMLDivElement | null>(null);
  const dbP99NcRef    = useRef<HTMLDivElement | null>(null);

  const hitPanelRef  = useRef<HTMLDivElement | null>(null);
  const hitChartRef  = useRef<HTMLDivElement | null>(null);

  const tputPanelRef = useRef<HTMLDivElement | null>(null);
  const tputTimeRef  = useRef<HTMLDivElement | null>(null);
  const tputNcRef    = useRef<HTMLDivElement | null>(null);

  const handleDownloadAll = useCallback(async () => {
    await downloadAllCharts(
      [
        { ref: dbP50PanelRef, name: "db_p50_latency" },
        { ref: dbP99PanelRef, name: "db_p99_latency" },
        { ref: hitPanelRef,   name: "hit_rate" },
        { ref: tputPanelRef,  name: "throughput" },
      ],
      "compare"
    );
  }, []);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {/* Toolbar */}
      <div style={toolbarStyle}>
        <button style={dlAllBtnStyle} onClick={handleDownloadAll}>↓ Download All</button>
      </div>

      {/* Legend */}
      <CompareLegend sources={sources} />

      {/* 2-column grid of panels */}
      <div style={gridStyle}>
        {/* Chart 1: DB p50 */}
        <SplitPanel
          title="DB p50 Latency"
          unit="ms"
          sources={sources}
          getValueCsv={(r) => r.db_p50_us / 1000}
          getValueNoCache={(r) => r.db_p50_us / 1000}
          panelRef={dbP50PanelRef}
          timeRef={dbP50TimeRef}
          noCacheRef={dbP50NcRef}
          onDownloadPng={() => downloadChartPng(dbP50PanelRef, "compare_db_p50_latency")}
        />

        {/* Chart 2: DB p99 */}
        <SplitPanel
          title="DB p99 Latency"
          unit="ms"
          sources={sources}
          getValueCsv={(r) => r.db_p99_us / 1000}
          getValueNoCache={(r) => r.db_p99_us / 1000}
          panelRef={dbP99PanelRef}
          timeRef={dbP99TimeRef}
          noCacheRef={dbP99NcRef}
          onDownloadPng={() => downloadChartPng(dbP99PanelRef, "compare_db_p99_latency")}
        />

        {/* Chart 3: Hit Rate (time-series only) */}
        <TimeOnlyPanel
          title="Hit Rate"
          unit="%"
          yDomain={[0, 100]}
          sources={sources}
          getValue={(r) => parseFloat((r.hit_rate * 100).toFixed(1))}
          panelRef={hitPanelRef}
          chartRef={hitChartRef}
          onDownloadPng={() => downloadChartPng(hitPanelRef, "compare_hit_rate")}
        />

        {/* Chart 4: Throughput */}
        <ThroughputPanel
          sources={sources}
          panelRef={tputPanelRef}
          timeRef={tputTimeRef}
          noCacheRef={tputNcRef}
          onDownloadPng={() => downloadChartPng(tputPanelRef, "compare_throughput")}
        />
      </div>
    </div>
  );
}

// ---- Legend ----

function CompareLegend({ sources }: { sources: CompareData }) {
  const items: Array<{ label: string; color: string; type: string }> = [
    ...sources.metastable.map((s, i) => ({
      label: s.label,
      color: cycleColor(METASTABLE_COLORS, i),
      type: "metastable",
    })),
    ...sources.baseline_steady.map((s, i) => ({
      label: s.label,
      color: cycleColor(BASELINE_STEADY_COLORS, i),
      type: "baseline_steady",
    })),
    ...sources.baseline_no_cache.map((s, i) => ({
      label: s.label,
      color: cycleColor(NO_CACHE_COLORS, i),
      type: "no_cache",
    })),
  ];

  if (items.length === 0) return null;

  return (
    <div style={legendContainerStyle}>
      {items.map((item) => (
        <div key={`${item.type}-${item.label}`} style={legendItemStyle}>
          <div style={{ ...legendSwatchStyle, background: item.color }} />
          <span style={{ color: "#94a3b8", fontSize: 10, fontFamily: "monospace" }}>
            {item.label}
          </span>
          <span style={{ color: "#334155", fontSize: 9, marginLeft: 2 }}>
            [{item.type}]
          </span>
        </div>
      ))}
    </div>
  );
}

// ---- Styles ----

const toolbarStyle: React.CSSProperties = {
  display: "flex",
  justifyContent: "flex-end",
};

const dlAllBtnStyle: React.CSSProperties = {
  fontSize: 10,
  color: "#e2e8f0",
  background: "#1e293b",
  border: "1px solid #334155",
  borderRadius: 4,
  padding: "3px 12px",
  cursor: "pointer",
  fontWeight: 600,
};

const dlBtnStyle: React.CSSProperties = {
  fontSize: 10,
  color: "#60a5fa",
  background: "transparent",
  border: "1px solid #1e293b",
  borderRadius: 4,
  padding: "2px 8px",
  cursor: "pointer",
  flexShrink: 0,
};

const gridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "1fr 1fr",
  gap: 8,
};

const panelStyle: React.CSSProperties = {
  background: "#0f172a",
  border: "1px solid #1e293b",
  borderRadius: 8,
  padding: "10px 12px",
};

const panelHeaderStyle: React.CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  marginBottom: 6,
};

const panelTitleStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 600,
  color: "#94a3b8",
  letterSpacing: 0.5,
};

const unitSpanStyle: React.CSSProperties = {
  color: "#64748b",
  fontWeight: 400,
  fontSize: 10,
  marginLeft: 6,
};

const subChartLabelStyle: React.CSSProperties = {
  fontSize: 8,
  color: "#334155",
  textAlign: "center",
  marginTop: 2,
  letterSpacing: 0.3,
};

const placeholderStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  height: 180,
  color: "#334155",
  fontSize: 12,
  fontStyle: "italic",
};

const legendContainerStyle: React.CSSProperties = {
  display: "flex",
  flexWrap: "wrap",
  gap: "6px 14px",
  padding: "8px 10px",
  background: "#0a1120",
  border: "1px solid #1e293b",
  borderRadius: 6,
};

const legendItemStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 5,
};

const legendSwatchStyle: React.CSSProperties = {
  width: 10,
  height: 10,
  borderRadius: 2,
  flexShrink: 0,
};
