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
import { CsvRow, BaselineSummary, NODE_COLORS } from "../types";
import { downloadChartPng, downloadAllCharts } from "../chartDownload";

interface Props {
  rows: CsvRow[];
  experimentStartMs: number | null;
  summary: BaselineSummary | null;
  /** "warmup" or "steady" — shown in the phase label */
  phase: "warmup" | "steady";
}

interface ChartPoint {
  elapsed: number;
  [key: string]: number | undefined;
}

function buildChartData(
  rows: CsvRow[],
  startMs: number | null,
  getValue: (row: CsvRow) => number
): ChartPoint[] {
  if (!startMs || rows.length === 0) return [];
  const map = new Map<number, ChartPoint>();
  for (const row of rows) {
    const elapsed = Math.round((row.timestamp_ms - startMs) / 1000);
    if (!map.has(elapsed)) map.set(elapsed, { elapsed });
    map.get(elapsed)![row.node_short] = getValue(row);
  }
  return Array.from(map.values()).sort((a, b) => a.elapsed - b.elapsed);
}

interface PanelConfig {
  title: string;
  getValue: (row: CsvRow) => number;
  unit: string;
  yDomain?: [number | "auto", number | "auto" | ((max: number) => number)];
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
    title: "Cache p50 Latency",
    getValue: (r) => parseFloat((r.cache_p50_us / 1000).toFixed(2)),
    unit: "ms",
    yDomain: [0, "auto"],
  },
  {
    title: "DB p50 Latency",
    getValue: (r) => parseFloat((r.db_p50_us / 1000).toFixed(1)),
    unit: "ms",
    yDomain: [0, "auto"],
  },
];

export function BaselineCharts({ rows, experimentStartMs, summary, phase }: Props) {
  const nodes = ["node1", "node2", "node3"];
  const chartRefs = useRef<Array<React.MutableRefObject<HTMLDivElement | null>>>(
    PANELS.map(() => ({ current: null }))
  );

  const handleDownloadAll = useCallback(async () => {
    await downloadAllCharts(
      PANELS.map((p, i) => ({
        ref: chartRefs.current[i],
        name: p.title.toLowerCase().replace(/ /g, "_"),
      })),
      `baseline_${phase}`
    );
  }, [phase]);

  if (rows.length === 0) {
    return (
      <div style={emptyStyle}>
        <div style={{ color: "#64748b", fontSize: 13 }}>No data for this run.</div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {phase === "steady" && summary && (
        <div style={statRowStyle}>
          <StatCard label="Cache p50" value={(summary.cache_p50_us / 1000).toFixed(2)} unit="ms" color="#60a5fa" />
          <StatCard label="DB p50" value={(summary.db_p50_us / 1000).toFixed(1)} unit="ms" color="#fb923c" />
          <StatCard label="Hit Rate" value={(summary.hit_rate * 100).toFixed(1)} unit="%" color="#4ade80" />
          <StatCard label="Throughput" value={summary.throughput_rps.toFixed(0)} unit="RPS" color="#a78bfa" />
        </div>
      )}

      <div style={{ display: "flex", justifyContent: "flex-end" }}>
        <button style={dlAllBtnStyle} onClick={handleDownloadAll}>↓ Download All</button>
      </div>

      <div style={gridStyle}>
        {PANELS.map((panel, i) => (
          <ChartPanel
            key={panel.title}
            panel={panel}
            rows={rows}
            startMs={experimentStartMs}
            nodes={nodes}
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
  nodes,
  chartRef,
}: {
  panel: PanelConfig;
  rows: CsvRow[];
  startMs: number | null;
  nodes: string[];
  chartRef: React.MutableRefObject<HTMLDivElement | null>;
}) {
  const ref = chartRef;
  const data = buildChartData(rows, startMs, panel.getValue);

  return (
    <div style={panelStyle}>
      <div style={panelHeaderStyle}>
        <span style={panelTitleStyle}>
          {panel.title}
          <span style={unitStyle}>({panel.unit})</span>
        </span>
        <button
          style={dlBtnStyle}
          onClick={() => downloadChartPng(ref, `baseline_${panel.title.toLowerCase().replace(/ /g, "_")}`)}
        >
          ↓ PNG
        </button>
      </div>
      <div ref={ref} style={{ background: "#0f172a" }}>
        <ResponsiveContainer width="100%" height={160}>
          <LineChart data={data} margin={{ top: 4, right: 8, left: -12, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
            <XAxis dataKey="elapsed" tick={{ fontSize: 9, fill: "#64748b" }} tickFormatter={(v) => `${v}s`} />
            <YAxis domain={panel.yDomain} tick={{ fontSize: 9, fill: "#64748b" }} />
            <Tooltip
              contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 6, fontSize: 11 }}
              labelFormatter={(v) => `${v}s`}
              formatter={(value: number, name: string) => [`${value} ${panel.unit}`, name.toUpperCase()]}
            />
            <Legend
              wrapperStyle={{ fontSize: 10, paddingTop: 4 }}
              formatter={(value) => (
                <span style={{ color: NODE_COLORS[value] ?? "#94a3b8" }}>{value.toUpperCase()}</span>
              )}
            />
            {nodes.map((nodeId) => (
              <Line
                key={nodeId}
                type="monotone"
                dataKey={nodeId}
                stroke={NODE_COLORS[nodeId]}
                strokeWidth={2}
                dot={false}
                connectNulls
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function StatCard({ label, value, unit, color }: { label: string; value: string; unit: string; color: string }) {
  return (
    <div style={{ ...statCardStyle, borderColor: color + "44" }}>
      <div style={{ fontSize: 9, color: "#64748b", marginBottom: 3, letterSpacing: 0.4 }}>{label.toUpperCase()}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color, fontVariantNumeric: "tabular-nums" }}>{value}</div>
      <div style={{ fontSize: 9, color: "#475569" }}>{unit}</div>
    </div>
  );
}

const emptyStyle: React.CSSProperties = {
  display: "flex", alignItems: "center", justifyContent: "center",
  height: 200, background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8,
};

const statRowStyle: React.CSSProperties = {
  display: "flex", gap: 8, flexWrap: "wrap",
};

const statCardStyle: React.CSSProperties = {
  flex: "1 1 0", minWidth: 90, background: "#0f172a",
  border: "1px solid #1e293b", borderRadius: 8, padding: "8px 12px",
};

const gridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "1fr 1fr",
  gap: 8,
};

const panelStyle: React.CSSProperties = {
  background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8, padding: "10px 12px",
};

const panelHeaderStyle: React.CSSProperties = {
  display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6,
};

const panelTitleStyle: React.CSSProperties = {
  fontSize: 11, fontWeight: 600, color: "#94a3b8", letterSpacing: 0.5,
};

const unitStyle: React.CSSProperties = {
  color: "#64748b", fontWeight: 400, fontSize: 10, marginLeft: 6,
};

const dlAllBtnStyle: React.CSSProperties = {
  fontSize: 10, color: "#e2e8f0", background: "#1e293b", border: "1px solid #334155",
  borderRadius: 4, padding: "3px 12px", cursor: "pointer", fontWeight: 600,
};

const dlBtnStyle: React.CSSProperties = {
  fontSize: 10, color: "#60a5fa", background: "transparent", border: "1px solid #1e293b",
  borderRadius: 4, padding: "2px 8px", cursor: "pointer",
};
