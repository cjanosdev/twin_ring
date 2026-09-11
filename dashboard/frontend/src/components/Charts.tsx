import { useRef, useCallback } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";
import { CsvRow, NODE_COLORS, PHASE_COLORS } from "../types";
import { downloadChartPng, downloadAllCharts } from "../chartDownload";

interface Props {
  rows: CsvRow[];
  experimentStartMs: number | null;
}

interface ChartPoint {
  elapsed: number;
  node1?: number;
  node2?: number;
  node3?: number;
  [key: string]: number | undefined;
}

interface PhaseTransition {
  elapsed: number;
  phase: string;
}

/**
 * Pivot rows into per-elapsed-second chart points keyed by node_short.
 * We bucket by (elapsed_sec, node) so that all 3 nodes appear on the same x-tick.
 */
function pivotRows(rows: CsvRow[], startMs: number | null): ChartPoint[] {
  if (!startMs || rows.length === 0) return [];

  const map = new Map<number, ChartPoint>();

  for (const row of rows) {
    const elapsed = Math.round((row.timestamp_ms - startMs) / 1000);
    if (!map.has(elapsed)) map.set(elapsed, { elapsed });
    const point = map.get(elapsed)!;
    point[row.node_short] = 0; // placeholder — overwritten below
  }

  return Array.from(map.values()).sort((a, b) => a.elapsed - b.elapsed);
}

function buildChartData(
  rows: CsvRow[],
  startMs: number | null,
  getValue: (row: CsvRow) => number
): { data: ChartPoint[]; transitions: PhaseTransition[] } {
  if (!startMs || rows.length === 0) return { data: [], transitions: [] };

  const map = new Map<number, ChartPoint>();
  const phaseByElapsed = new Map<number, string>();

  for (const row of rows) {
    const elapsed = Math.round((row.timestamp_ms - startMs) / 1000);
    if (!map.has(elapsed)) map.set(elapsed, { elapsed });
    const point = map.get(elapsed)!;
    point[row.node_short] = getValue(row);
    phaseByElapsed.set(elapsed, row.phase);
  }

  const data = Array.from(map.values()).sort((a, b) => a.elapsed - b.elapsed);

  // Find phase transitions
  const transitions: PhaseTransition[] = [];
  let lastPhase = "";
  for (const [elapsed, phase] of [...phaseByElapsed.entries()].sort((a, b) => a[0] - b[0])) {
    if (phase !== lastPhase && lastPhase !== "") {
      transitions.push({ elapsed, phase });
    }
    lastPhase = phase;
  }

  return { data, transitions };
}

interface PanelConfig {
  title: string;
  getValue: (row: CsvRow) => number;
  unit: string;
  yDomain?: [number | "auto", number | "auto" | ((dataMax: number) => number)];
  logScale?: boolean;
  formatY?: (v: number) => string;
}

const PANELS: PanelConfig[] = [
  {
    title: "Hit Rate",
    getValue: (r) => parseFloat((r.hit_rate * 100).toFixed(1)),
    unit: "%",
    yDomain: [0, 100],
  },
  {
    title: "Throughput",
    getValue: (r) => parseFloat(r.throughput_rps.toFixed(0)),
    unit: "RPS",
    yDomain: [0, "auto"],
  },
  {
    title: "Cache p99 Latency",
    getValue: (r) => parseFloat((r.cache_p99_us / 1000).toFixed(1)),
    unit: "ms",
    yDomain: [0, "auto"],
  },
  {
    title: "Cache p50 Latency",
    getValue: (r) => parseFloat((r.cache_p50_us / 1000).toFixed(2)),
    unit: "ms",
    yDomain: [0, "auto"],
  },
  {
    title: "DB p99 Latency",
    getValue: (r) => parseFloat((r.db_p99_us / 1000).toFixed(1)),
    unit: "ms",
    yDomain: [0, "auto"],
  },
  {
    title: "DB Errors",
    getValue: (r) => r.db_errors,
    unit: "count",
    yDomain: [0, (dataMax: number) => Math.max(dataMax, 4)],
  },
];

export function Charts({ rows, experimentStartMs }: Props) {
  // One stable ref per panel — created once, never change order (PANELS is module-level const)
  const chartRefs = useRef<Array<React.RefObject<HTMLDivElement | null>>>(
    PANELS.map(() => ({ current: null }))
  );

  const handleDownloadAll = useCallback(async () => {
    await downloadAllCharts(
      PANELS.map((p, i) => ({
        ref: chartRefs.current[i],
        name: p.title.toLowerCase().replace(/ /g, "_"),
      })),
      "metastable"
    );
  }, []);

  if (rows.length === 0) {
    return (
      <div style={emptyStyle}>
        <div style={{ color: "#64748b", fontSize: 13 }}>
          Charts will appear once experiment data starts streaming in...
        </div>
      </div>
    );
  }

  return (
    <div>
      <div style={toolbarStyle}>
        <button style={dlAllBtnStyle} onClick={handleDownloadAll}>↓ Download All</button>
      </div>
      <div style={gridStyle}>
        {PANELS.map((panel, i) => (
          <ChartPanel
            key={panel.title}
            panel={panel}
            rows={rows}
            startMs={experimentStartMs}
            chartRef={chartRefs.current[i]}
          />
        ))}
      </div>
    </div>
  );
}

function ChartPanel({
  panel,
  rows,
  startMs,
  chartRef,
}: {
  panel: PanelConfig;
  rows: CsvRow[];
  startMs: number | null;
  chartRef: React.MutableRefObject<HTMLDivElement | null>;
}) {
  const { data, transitions } = buildChartData(rows, startMs, panel.getValue);
  const nodes = ["node1", "node2", "node3"];

  return (
    <div style={panelStyle}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
        <div style={panelTitleStyle}>
          {panel.title}
          <span style={{ color: "#64748b", fontWeight: 400, fontSize: 10, marginLeft: 6 }}>
            ({panel.unit})
          </span>
        </div>
        <button
          style={dlBtnStyle}
          onClick={() => downloadChartPng(chartRef, `metastable_${panel.title.toLowerCase().replace(/ /g, "_")}`)}
        >
          ↓ PNG
        </button>
      </div>
      <div ref={chartRef} style={{ background: "#0f172a" }}>
      <ResponsiveContainer width="100%" height={160}>
        <LineChart data={data} margin={{ top: 4, right: 8, left: -12, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="elapsed"
            tick={{ fontSize: 9, fill: "#64748b" }}
            tickFormatter={(v) => `${v}s`}
          />
          <YAxis
            domain={panel.yDomain}
            tick={{ fontSize: 9, fill: "#64748b" }}
            tickFormatter={panel.formatY}
          />
          <Tooltip
            contentStyle={{
              background: "#1e293b",
              border: "1px solid #334155",
              borderRadius: 6,
              fontSize: 11,
            }}
            labelFormatter={(v) => `${v}s`}
            formatter={(value: number, name: string) => [
              `${value} ${panel.unit}`,
              name.toUpperCase(),
            ]}
          />
          <Legend
            wrapperStyle={{ fontSize: 10, paddingTop: 4 }}
            formatter={(value) => (
              <span style={{ color: NODE_COLORS[value] ?? "#94a3b8" }}>{value.toUpperCase()}</span>
            )}
          />

          {/* Phase transition lines */}
          {transitions.map((t) => (
            <ReferenceLine
              key={t.elapsed}
              x={t.elapsed}
              stroke={PHASE_COLORS[t.phase] ?? "#64748b"}
              strokeDasharray="4 2"
              strokeWidth={1.5}
              label={{
                value: t.phase.replace("_", " ").toUpperCase(),
                position: "insideTopLeft",
                fill: PHASE_COLORS[t.phase] ?? "#64748b",
                fontSize: 8,
              }}
            />
          ))}

          {nodes.map((nodeId) => (
            <Line
              key={nodeId}
              type="monotone"
              dataKey={nodeId}
              stroke={NODE_COLORS[nodeId]}
              strokeWidth={2}
              dot={false}
              connectNulls={true}
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
      </div>
    </div>
  );
}

const toolbarStyle: React.CSSProperties = {
  display: "flex", justifyContent: "flex-end", marginBottom: 6,
};

const dlAllBtnStyle: React.CSSProperties = {
  fontSize: 10, color: "#e2e8f0", background: "#1e293b", border: "1px solid #334155",
  borderRadius: 4, padding: "3px 12px", cursor: "pointer", fontWeight: 600,
};

const dlBtnStyle: React.CSSProperties = {
  fontSize: 10, color: "#60a5fa", background: "transparent", border: "1px solid #1e293b",
  borderRadius: 4, padding: "2px 8px", cursor: "pointer", flexShrink: 0,
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

const panelTitleStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 600,
  color: "#94a3b8",
  letterSpacing: 0.5,
};

const emptyStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  height: 200,
  background: "#0f172a",
  border: "1px solid #1e293b",
  borderRadius: 8,
};
