import { useEffect, useState } from "react";
import { RunInfo, RunType, CsvRow, NoCacheRow } from "../types";
import { apiListRuns, apiGetRunData, apiGetNoCacheData } from "../api";

export type LoadedRun =
  | { type: "metastable"; rows: CsvRow[]; startMs: number | null }
  | { type: "baseline_warmup" | "baseline_steady"; rows: CsvRow[]; startMs: number | null }
  | { type: "baseline_no_cache"; rows: NoCacheRow[] };

interface Props {
  onLoadRun: (run: LoadedRun) => void;
  liveRunning: boolean;
}

const RUN_TYPE_LABEL: Record<RunType, string> = {
  metastable:        "Metastable",
  baseline_warmup:   "Baseline Warmup",
  baseline_steady:   "Baseline Steady",
  baseline_no_cache: "No-Cache Baseline",
  unknown:           "Other",
};

const RUN_TYPE_COLOR: Record<RunType, string> = {
  metastable:        "#ef4444",
  baseline_warmup:   "#3b82f6",
  baseline_steady:   "#4ade80",
  baseline_no_cache: "#fb923c",
  unknown:           "#64748b",
};

// Display order for groups
const TYPE_ORDER: RunType[] = [
  "metastable",
  "baseline_steady",
  "baseline_warmup",
  "baseline_no_cache",
  "unknown",
];

export function RunsBrowser({ onLoadRun, liveRunning }: Props) {
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const fetchRuns = async () => {
      try {
        const data = await apiListRuns();
        setRuns(data);
      } catch {
        // backend not ready
      }
    };
    fetchRuns();
    const id = setInterval(fetchRuns, 5000);
    return () => clearInterval(id);
  }, []);

  const handleSelect = async (run: RunInfo) => {
    if (liveRunning) return;
    setSelected(run.path);
    setLoading(true);
    try {
      if (run.run_type === "baseline_no_cache") {
        const rows = await apiGetNoCacheData(run.path);
        onLoadRun({ type: "baseline_no_cache", rows });
      } else if (run.run_type === "baseline_warmup" || run.run_type === "baseline_steady") {
        const rows = await apiGetRunData(run.path);
        const startMs = rows.length > 0 ? rows[0].timestamp_ms : null;
        onLoadRun({ type: run.run_type, rows, startMs });
      } else {
        // metastable or unknown — treat as metastable
        const rows = await apiGetRunData(run.path);
        const startMs = rows.length > 0 ? rows[0].timestamp_ms : null;
        onLoadRun({ type: "metastable", rows, startMs });
      }
    } catch (e) {
      console.error("Failed to load run:", e);
    } finally {
      setLoading(false);
    }
  };

  // Group by run_type, then within each group by date
  const byType = runs.reduce<Record<RunType, RunInfo[]>>((acc, run) => {
    if (!acc[run.run_type]) acc[run.run_type] = [];
    acc[run.run_type].push(run);
    return acc;
  }, {} as Record<RunType, RunInfo[]>);

  return (
    <div style={containerStyle}>
      <div style={titleStyle}>PAST RUNS</div>
      {liveRunning && (
        <div style={{ fontSize: 10, color: "#64748b", marginBottom: 8 }}>
          Stop the experiment to load a past run.
        </div>
      )}
      {runs.length === 0 ? (
        <div style={{ fontSize: 11, color: "#475569" }}>No past runs found.</div>
      ) : (
        <div style={{ overflowY: "auto", flex: 1 }}>
          {TYPE_ORDER.filter((t) => byType[t]?.length > 0).map((runType) => (
            <div key={runType} style={{ marginBottom: 12 }}>
              <div style={{ ...typeHeaderStyle, color: RUN_TYPE_COLOR[runType] }}>
                {RUN_TYPE_LABEL[runType]}
              </div>
              {byType[runType].map((run) => (
                <button
                  key={run.path}
                  onClick={() => handleSelect(run)}
                  disabled={liveRunning || loading}
                  style={runBtnStyle(selected === run.path, liveRunning, RUN_TYPE_COLOR[run.run_type])}
                >
                  <div style={{ fontWeight: 600, fontSize: 10 }}>{run.filename}</div>
                  <div style={{ fontSize: 9, color: "#64748b", marginTop: 1 }}>
                    {run.date} · {(run.size_bytes / 1024).toFixed(1)} KB
                  </div>
                </button>
              ))}
            </div>
          ))}
        </div>
      )}
      {loading && (
        <div style={{ fontSize: 11, color: "#60a5fa", marginTop: 6 }}>Loading…</div>
      )}
    </div>
  );
}

const containerStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  height: "100%",
  padding: "10px",
};

const titleStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  color: "#64748b",
  letterSpacing: 1,
  marginBottom: 8,
  flexShrink: 0,
};

const typeHeaderStyle: React.CSSProperties = {
  fontSize: 10,
  fontWeight: 700,
  marginBottom: 4,
  letterSpacing: 0.5,
  textTransform: "uppercase",
};

const runBtnStyle = (selected: boolean, disabled: boolean, accentColor: string): React.CSSProperties => ({
  width: "100%",
  textAlign: "left",
  background: selected ? "#1e293b" : "transparent",
  border: `1px solid ${selected ? accentColor : "#1e293b"}`,
  borderRadius: 5,
  color: "#e2e8f0",
  padding: "6px 8px",
  cursor: disabled ? "not-allowed" : "pointer",
  marginBottom: 4,
  opacity: disabled ? 0.5 : 1,
  transition: "border-color 0.15s",
});
