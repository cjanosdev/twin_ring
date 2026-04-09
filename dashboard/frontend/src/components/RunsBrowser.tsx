import { useEffect, useState } from "react";
import { RunInfo, CsvRow } from "../types";
import { apiListRuns, apiGetRunData } from "../api";

interface Props {
  onLoadRun: (rows: CsvRow[], startMs: number | null) => void;
  liveRunning: boolean;
}

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

  const handleSelect = async (path: string) => {
    if (liveRunning) return;
    setSelected(path);
    setLoading(true);
    try {
      const rows = await apiGetRunData(path);
      const startMs = rows.length > 0 ? rows[0].timestamp_ms : null;
      onLoadRun(rows, startMs);
    } catch (e) {
      console.error("Failed to load run:", e);
    } finally {
      setLoading(false);
    }
  };

  const groupedByDate = runs.reduce<Record<string, RunInfo[]>>((acc, run) => {
    if (!acc[run.date]) acc[run.date] = [];
    acc[run.date].push(run);
    return acc;
  }, {});

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
          {Object.entries(groupedByDate).map(([date, dateRuns]) => (
            <div key={date} style={{ marginBottom: 10 }}>
              <div style={dateHeaderStyle}>{date}</div>
              {dateRuns.map((run) => (
                <button
                  key={run.path}
                  onClick={() => handleSelect(run.path)}
                  disabled={liveRunning || loading}
                  style={runBtnStyle(selected === run.path, liveRunning)}
                >
                  <div style={{ fontWeight: 600, fontSize: 10 }}>{run.filename}</div>
                  <div style={{ fontSize: 9, color: "#64748b", marginTop: 1 }}>
                    {(run.size_bytes / 1024).toFixed(1)} KB
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

const dateHeaderStyle: React.CSSProperties = {
  fontSize: 10,
  color: "#475569",
  fontWeight: 600,
  marginBottom: 4,
  textTransform: "uppercase",
  letterSpacing: 0.5,
};

const runBtnStyle = (selected: boolean, disabled: boolean): React.CSSProperties => ({
  width: "100%",
  textAlign: "left",
  background: selected ? "#1d4ed8" : "#1e293b",
  border: `1px solid ${selected ? "#3b82f6" : "#334155"}`,
  borderRadius: 5,
  color: "#e2e8f0",
  padding: "6px 8px",
  cursor: disabled ? "not-allowed" : "pointer",
  marginBottom: 4,
  opacity: disabled ? 0.5 : 1,
  transition: "background 0.2s",
});
