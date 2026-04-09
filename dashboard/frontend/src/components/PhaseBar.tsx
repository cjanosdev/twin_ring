import { CsvRow, PHASE_COLORS } from "../types";

interface Props {
  rows: CsvRow[];
  experimentStartMs: number | null;
}

interface Segment {
  phase: string;
  startSec: number;
  endSec: number;
}

export function PhaseBar({ rows, experimentStartMs }: Props) {
  if (rows.length === 0 || !experimentStartMs) {
    return (
      <div style={containerStyle}>
        <span style={{ color: "#94a3b8", fontSize: 13 }}>
          No experiment running — start one to see the phase timeline
        </span>
      </div>
    );
  }

  // Build phase segments from rows
  const segments: Segment[] = [];
  let currentPhase = rows[0].phase;
  let segStart = (rows[0].timestamp_ms - experimentStartMs) / 1000;

  for (const row of rows) {
    const elapsed = (row.timestamp_ms - experimentStartMs) / 1000;
    if (row.phase !== currentPhase) {
      segments.push({ phase: currentPhase, startSec: segStart, endSec: elapsed });
      currentPhase = row.phase;
      segStart = elapsed;
    }
  }
  const lastElapsed = (rows[rows.length - 1].timestamp_ms - experimentStartMs) / 1000;
  segments.push({ phase: currentPhase, startSec: segStart, endSec: lastElapsed });

  const totalSecs = lastElapsed || 1;
  const currentPhaseLabel = currentPhase.replace("_", " ").toUpperCase();
  const color = PHASE_COLORS[currentPhase] ?? "#64748b";

  return (
    <div style={containerStyle}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
        <span style={{ fontSize: 12, color: "#94a3b8", fontWeight: 500 }}>PHASE</span>
        <span
          style={{
            background: color,
            color: "#fff",
            fontSize: 12,
            fontWeight: 700,
            borderRadius: 4,
            padding: "2px 8px",
            letterSpacing: 1,
          }}
        >
          {currentPhaseLabel}
        </span>
        <span style={{ fontSize: 12, color: "#64748b" }}>
          {Math.round(lastElapsed)}s elapsed
        </span>
      </div>

      <div style={{ display: "flex", height: 20, borderRadius: 6, overflow: "hidden", background: "#1e293b" }}>
        {segments.map((seg, i) => (
          <div
            key={i}
            title={`${seg.phase}: ${Math.round(seg.startSec)}s – ${Math.round(seg.endSec)}s`}
            style={{
              width: `${((seg.endSec - seg.startSec) / totalSecs) * 100}%`,
              background: PHASE_COLORS[seg.phase] ?? "#64748b",
              opacity: 0.85,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontSize: 10,
              color: "#fff",
              fontWeight: 600,
              overflow: "hidden",
              whiteSpace: "nowrap",
              minWidth: 0,
            }}
          >
            {seg.phase.toUpperCase()}
          </div>
        ))}
      </div>

      <div style={{ display: "flex", marginTop: 2 }}>
        {segments.map((seg, i) => (
          <div
            key={i}
            style={{
              width: `${((seg.endSec - seg.startSec) / totalSecs) * 100}%`,
              fontSize: 9,
              color: "#64748b",
              overflow: "hidden",
              whiteSpace: "nowrap",
              textAlign: "left",
              paddingLeft: 2,
            }}
          >
            {Math.round(seg.startSec)}s
          </div>
        ))}
      </div>
    </div>
  );
}

const containerStyle: React.CSSProperties = {
  background: "#1e293b",
  borderRadius: 8,
  padding: "10px 14px",
  borderBottom: "1px solid #334155",
};
