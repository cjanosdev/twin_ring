import { useRef, useCallback } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
  LineChart,
  Line,
  Legend,
} from "recharts";
import { NoCacheRow, BaselineNoCacheSummary } from "../types";
import { downloadChartPng, downloadAllCharts } from "../chartDownload";

interface Props {
  rows: NoCacheRow[];
  summary: BaselineNoCacheSummary | null;
}

export function BaselineNoCacheCharts({ rows, summary }: Props) {
  const opsRef = useRef<HTMLDivElement>(null);
  const latRef = useRef<HTMLDivElement>(null);

  const handleDownloadAll = useCallback(async () => {
    await downloadAllCharts(
      [
        { ref: opsRef, name: "throughput" },
        { ref: latRef, name: "latency" },
      ],
      "baseline_no_cache"
    );
  }, []);

  if (rows.length === 0) {
    return (
      <div style={emptyStyle}>
        <div style={{ color: "#64748b", fontSize: 13 }}>No data for this run.</div>
      </div>
    );
  }

  const satWorkers = summary?.saturation_workers ?? null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {summary && (
        <div style={statRowStyle}>
          <StatCard label="Last Clean ops/s" value={summary.last_clean_ops_per_sec.toFixed(0)} unit="ops/s" color="#4ade80" />
          <StatCard label="Last Clean workers" value={String(summary.last_clean_workers)} unit="workers" color="#4ade80" />
          <StatCard label="Last Clean db_p50" value={(summary.last_clean_db_p50_us / 1000).toFixed(1)} unit="ms" color="#60a5fa" />
          <StatCard label="Saturation workers" value={String(summary.saturation_workers)} unit="workers" color="#ef4444" />
          <StatCard label="Saturation ops/s" value={summary.saturation_ops_per_sec.toFixed(0)} unit="ops/s" color="#ef4444" />
        </div>
      )}

      <div style={{ display: "flex", justifyContent: "flex-end" }}>
        <button style={dlAllBtnStyle} onClick={handleDownloadAll}>↓ Download All</button>
      </div>

      {/* Ops/sec bar chart */}
      <div style={panelStyle}>
        <div style={panelHeaderStyle}>
          <span style={panelTitleStyle}>Throughput vs Concurrency<span style={unitStyle}>(ops/s)</span></span>
          <button style={dlBtnStyle} onClick={() => downloadChartPng(opsRef, "baseline_no_cache_throughput")}>↓ PNG</button>
        </div>
        <div ref={opsRef} style={{ background: "#0f172a" }}>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={rows} margin={{ top: 4, right: 8, left: -12, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="workers" tick={{ fontSize: 9, fill: "#64748b" }} label={{ value: "workers", position: "insideBottom", offset: -2, fill: "#64748b", fontSize: 9 }} />
              <YAxis tick={{ fontSize: 9, fill: "#64748b" }} />
              <Tooltip
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 6, fontSize: 11 }}
                formatter={(v: number) => [`${v.toFixed(0)} ops/s`, "throughput"]}
                labelFormatter={(v) => `${v} workers`}
              />
              {satWorkers !== null && (
                <ReferenceLine
                  x={satWorkers}
                  stroke="#ef4444"
                  strokeDasharray="4 2"
                  strokeWidth={1.5}
                  label={{ value: "SATURATION", position: "insideTopLeft", fill: "#ef4444", fontSize: 8 }}
                />
              )}
              <Bar dataKey="ops_per_sec" fill="#60a5fa" radius={[3, 3, 0, 0]} isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Latency line chart */}
      <div style={panelStyle}>
        <div style={panelHeaderStyle}>
          <span style={panelTitleStyle}>DB Latency vs Concurrency<span style={unitStyle}>(ms)</span></span>
          <button style={dlBtnStyle} onClick={() => downloadChartPng(latRef, "baseline_no_cache_latency")}>↓ PNG</button>
        </div>
        <div ref={latRef} style={{ background: "#0f172a" }}>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={rows.map(r => ({ ...r, db_p50_ms: r.db_p50_us / 1000, db_p99_ms: r.db_p99_us / 1000 }))} margin={{ top: 4, right: 8, left: -12, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis dataKey="workers" tick={{ fontSize: 9, fill: "#64748b" }} label={{ value: "workers", position: "insideBottom", offset: -2, fill: "#64748b", fontSize: 9 }} />
              <YAxis tick={{ fontSize: 9, fill: "#64748b" }} />
              <Tooltip
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 6, fontSize: 11 }}
                formatter={(v: number, name: string) => [`${v.toFixed(1)} ms`, name]}
                labelFormatter={(v) => `${v} workers`}
              />
              <Legend wrapperStyle={{ fontSize: 10 }} />
              {satWorkers !== null && (
                <ReferenceLine x={satWorkers} stroke="#ef4444" strokeDasharray="4 2" strokeWidth={1.5} />
              )}
              <Line type="monotone" dataKey="db_p50_ms" name="p50" stroke="#4ade80" strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
              <Line type="monotone" dataKey="db_p99_ms" name="p99" stroke="#fb923c" strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
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
