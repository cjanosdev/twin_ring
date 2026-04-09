import { useEffect, useState } from "react";
import { CsvRow, NODE_COLORS } from "../types";
import { apiGetCassandraMem } from "../api";

interface Props {
  rows: CsvRow[];
  running: boolean;
}

interface NodeState {
  node_short: string;
  hit_rate: number;
  throughput_rps: number;
  db_errors: number;
  phase: string;
}

function getLatestPerNode(rows: CsvRow[]): Map<string, NodeState> {
  const map = new Map<string, NodeState>();
  for (const row of rows) {
    map.set(row.node_short, {
      node_short: row.node_short,
      hit_rate: row.hit_rate,
      throughput_rps: row.throughput_rps,
      db_errors: row.db_errors,
      phase: row.phase,
    });
  }
  return map;
}

function hitRateBg(rate: number, isFaulted: boolean): string {
  if (isFaulted) return "#450a0a";
  if (rate >= 0.9) return "#052e16";
  if (rate >= 0.5) return "#431407";
  return "#3b0764";
}

function hitRateColor(rate: number, isFaulted: boolean): string {
  if (isFaulted) return "#fca5a5";
  if (rate >= 0.9) return "#4ade80";
  if (rate >= 0.5) return "#fb923c";
  return "#c084fc";
}

export function Topology({ rows, running }: Props) {
  const [cassMemPct, setCassMemPct] = useState<number | null>(null);
  const nodeMap = getLatestPerNode(rows);

  useEffect(() => {
    let mounted = true;
    const poll = async () => {
      const mem = await apiGetCassandraMem();
      if (mounted && mem) setCassMemPct(mem.mem_pct);
    };
    poll();
    const id = setInterval(poll, 3000);
    return () => {
      mounted = false;
      clearInterval(id);
    };
  }, []);

  const nodes = ["node1", "node2", "node3"];

  return (
    <div style={containerStyle}>
      <div style={{ fontSize: 11, color: "#64748b", fontWeight: 600, marginBottom: 10, letterSpacing: 1 }}>
        SYSTEM TOPOLOGY
      </div>

      {/* Node row */}
      <div style={{ display: "flex", justifyContent: "space-around", marginBottom: 10 }}>
        {nodes.map((nodeId) => {
          const state = nodeMap.get(nodeId);
          const isFaulting = state?.phase === "fault_inject" && nodeId === "node1";
          const isFaulted = isFaulting && (state?.throughput_rps ?? 0) === 0;
          const isOffline = running && !nodeMap.has(nodeId) && nodeId === "node1";
          const actualFaulted = isFaulted || isOffline;

          return (
            <NodeCard
              key={nodeId}
              nodeId={nodeId}
              state={state ?? null}
              isFaulted={actualFaulted}
              isFaulting={isFaulting && !actualFaulted}
            />
          );
        })}
      </div>

      {/* Connector lines */}
      <svg height={30} style={{ width: "100%", display: "block", marginBottom: 4 }}>
        {/* Three lines converging to center */}
        <line x1="17%" y1="0" x2="50%" y2="28" stroke="#334155" strokeWidth={1.5} />
        <line x1="50%" y1="0" x2="50%" y2="28" stroke="#334155" strokeWidth={1.5} />
        <line x1="83%" y1="0" x2="50%" y2="28" stroke="#334155" strokeWidth={1.5} />
      </svg>

      {/* Cassandra */}
      <div style={{ display: "flex", justifyContent: "center" }}>
        <CassandraCard memPct={cassMemPct} />
      </div>
    </div>
  );
}

function NodeCard({
  nodeId,
  state,
  isFaulted,
  isFaulting,
}: {
  nodeId: string;
  state: NodeState | null;
  isFaulted: boolean;
  isFaulting: boolean;
}) {
  const color = NODE_COLORS[nodeId] ?? "#94a3b8";
  const hitRate = state?.hit_rate ?? 0;
  const bg = state ? hitRateBg(hitRate, isFaulted) : "#1e293b";
  const textColor = state ? hitRateColor(hitRate, isFaulted) : "#64748b";

  const borderColor = isFaulted ? "#ef4444" : isFaulting ? "#f97316" : color;

  return (
    <div
      style={{
        width: 120,
        border: `2px solid ${borderColor}`,
        borderRadius: 8,
        background: bg,
        padding: "8px 10px",
        textAlign: "center",
        animation: isFaulted ? "pulse 1s ease-in-out infinite alternate" : undefined,
        transition: "background 0.5s, border-color 0.3s",
      }}
    >
      <style>{`@keyframes pulse { from { opacity: 1; } to { opacity: 0.5; } }`}</style>
      <div style={{ fontSize: 12, fontWeight: 700, color, marginBottom: 4 }}>
        {nodeId.toUpperCase()}
      </div>
      <div
        style={{
          fontSize: 11,
          fontWeight: 600,
          color: isFaulted ? "#fca5a5" : "#64748b",
          marginBottom: 6,
        }}
      >
        {isFaulted ? "● OFFLINE" : "● ONLINE"}
      </div>
      {state ? (
        <>
          <div style={{ fontSize: 18, fontWeight: 700, color: textColor }}>
            {(hitRate * 100).toFixed(0)}%
          </div>
          <div style={{ fontSize: 9, color: "#64748b", marginBottom: 2 }}>hit rate</div>
          <div style={{ fontSize: 11, color: "#94a3b8" }}>
            {state.throughput_rps.toFixed(0)} RPS
          </div>
          {state.db_errors > 0 && (
            <div style={{ fontSize: 10, color: "#ef4444", marginTop: 2, fontWeight: 600 }}>
              ⚠ {state.db_errors} db_err
            </div>
          )}
        </>
      ) : (
        <div style={{ fontSize: 11, color: "#64748b" }}>waiting…</div>
      )}
    </div>
  );
}

function CassandraCard({ memPct }: { memPct: number | null }) {
  const isCritical = memPct !== null && memPct >= 80;
  const color = isCritical ? "#ef4444" : "#94a3b8";

  return (
    <div
      style={{
        width: 160,
        border: `2px solid ${isCritical ? "#ef4444" : "#334155"}`,
        borderRadius: 8,
        background: isCritical ? "#450a0a" : "#1e293b",
        padding: "8px 12px",
        textAlign: "center",
        transition: "background 0.5s, border-color 0.3s",
      }}
    >
      <div style={{ fontSize: 12, fontWeight: 700, color: "#a78bfa", marginBottom: 4 }}>
        CASSANDRA
      </div>
      {memPct !== null ? (
        <>
          <div style={{ fontSize: 20, fontWeight: 700, color }}>
            {memPct.toFixed(0)}%
          </div>
          <div style={{ fontSize: 9, color: "#64748b" }}>memory</div>
          <div style={{ marginTop: 4, height: 4, borderRadius: 2, background: "#334155" }}>
            <div
              style={{
                width: `${Math.min(memPct, 100)}%`,
                height: "100%",
                background: isCritical ? "#ef4444" : "#a78bfa",
                borderRadius: 2,
                transition: "width 0.5s",
              }}
            />
          </div>
        </>
      ) : (
        <div style={{ fontSize: 11, color: "#64748b" }}>unreachable</div>
      )}
    </div>
  );
}

const containerStyle: React.CSSProperties = {
  background: "#0f172a",
  borderRadius: 8,
  padding: "12px 16px",
  border: "1px solid #1e293b",
};
