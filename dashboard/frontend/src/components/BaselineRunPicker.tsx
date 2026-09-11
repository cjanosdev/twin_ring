import { useEffect, useState } from "react";
import { RunInfo, RunType } from "../types";
import { apiListRuns } from "../api";

interface Props {
  runType: "baseline_steady" | "baseline_no_cache";
  selectedPath: string | null;
  onSelect: (run: RunInfo) => void;
}

const TYPE_LABEL: Record<string, string> = {
  baseline_steady:   "Baseline Steady",
  baseline_no_cache: "No-Cache Baseline",
};

const TYPE_COLOR: Record<string, string> = {
  baseline_steady:   "#4ade80",
  baseline_no_cache: "#fb923c",
};

export function BaselineRunPicker({ runType, selectedPath, onSelect }: Props) {
  const [runs, setRuns] = useState<RunInfo[]>([]);

  useEffect(() => {
    const fetch = async () => {
      try {
        const all = await apiListRuns();
        setRuns(all.filter((r) => r.run_type === (runType as RunType)));
      } catch { /* backend not ready */ }
    };
    fetch();
    const id = setInterval(fetch, 5000);
    return () => clearInterval(id);
  }, [runType]);

  const color = TYPE_COLOR[runType];

  // Group by date
  const byDate = runs.reduce<Record<string, RunInfo[]>>((acc, r) => {
    if (!acc[r.date]) acc[r.date] = [];
    acc[r.date].push(r);
    return acc;
  }, {});

  return (
    <div style={containerStyle}>
      <div style={{ ...titleStyle, color }}>{TYPE_LABEL[runType].toUpperCase()}</div>
      <div style={{ fontSize: 10, color: "#475569", marginBottom: 10 }}>Select a run to display:</div>

      {runs.length === 0 ? (
        <div style={{ fontSize: 11, color: "#475569" }}>No runs found.</div>
      ) : (
        <div style={{ overflowY: "auto", flex: 1 }}>
          {Object.entries(byDate).map(([date, dateRuns]) => (
            <div key={date} style={{ marginBottom: 12 }}>
              <div style={dateHeaderStyle}>{date}</div>
              {dateRuns.map((run) => {
                const selected = run.path === selectedPath;
                return (
                  <button
                    key={run.path}
                    onClick={() => onSelect(run)}
                    style={runBtnStyle(selected, color)}
                  >
                    <div style={{ fontWeight: 600, fontSize: 10, color: selected ? color : "#e2e8f0" }}>
                      {run.filename}
                    </div>
                    <div style={{ fontSize: 9, color: "#64748b", marginTop: 2 }}>
                      {(run.size_bytes / 1024).toFixed(1)} KB
                    </div>
                  </button>
                );
              })}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

const containerStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  height: "100%",
  padding: "12px 10px",
  overflow: "hidden",
};

const titleStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  letterSpacing: 1,
  marginBottom: 6,
};

const dateHeaderStyle: React.CSSProperties = {
  fontSize: 9,
  color: "#475569",
  fontWeight: 600,
  textTransform: "uppercase",
  letterSpacing: 0.5,
  marginBottom: 4,
};

const runBtnStyle = (selected: boolean, color: string): React.CSSProperties => ({
  width: "100%",
  textAlign: "left",
  background: selected ? "#1e293b" : "transparent",
  border: `1px solid ${selected ? color : "#1e293b"}`,
  borderRadius: 5,
  color: "#e2e8f0",
  padding: "6px 8px",
  cursor: "pointer",
  marginBottom: 4,
  transition: "border-color 0.15s",
});
