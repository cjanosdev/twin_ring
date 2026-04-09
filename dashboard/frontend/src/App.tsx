import { useState, useEffect } from "react";
import { CsvRow } from "./types";
import { useSSEStream, useStatusPoller } from "./api";
import { PhaseBar } from "./components/PhaseBar";
import { Topology } from "./components/Topology";
import { Charts } from "./components/Charts";
import { ConfigPanel } from "./components/ConfigPanel";
import { LogPanel } from "./components/LogPanel";
import { RunsBrowser } from "./components/RunsBrowser";

export default function App() {
  const status = useStatusPoller(2000);
  const running = status?.running ?? false;

  // SSE rows from live experiment
  const { rows: liveRows, clearRows } = useSSEStream(running);

  // Rows currently shown in charts (live or replayed from past run)
  const [displayRows, setDisplayRows] = useState<CsvRow[]>([]);
  const [displayStartMs, setDisplayStartMs] = useState<number | null>(null);
  const [mode, setMode] = useState<"live" | "replay">("live");

  // When live rows arrive, show them
  useEffect(() => {
    if (mode === "live") {
      setDisplayRows(liveRows);
      if (liveRows.length > 0 && displayStartMs === null) {
        setDisplayStartMs(liveRows[0].timestamp_ms);
      }
    }
  }, [liveRows, mode]);

  // When experiment starts/stops, switch mode
  useEffect(() => {
    if (running) {
      setMode("live");
      setDisplayRows([]);
      setDisplayStartMs(null);
    }
  }, [running]);

  const handleStart = () => {
    clearRows();
    setDisplayRows([]);
    setDisplayStartMs(null);
    setMode("live");
  };

  const handleStop = () => {
    // Keep showing current rows but stop streaming
  };

  const handleLoadRun = (rows: CsvRow[], startMs: number | null) => {
    setMode("replay");
    setDisplayRows(rows);
    setDisplayStartMs(startMs);
  };

  return (
    <div style={appStyle}>
      {/* Top bar */}
      <div style={topBarStyle}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <span style={{ fontSize: 15, fontWeight: 700, color: "#60a5fa", letterSpacing: 1 }}>
            TWIN RING
          </span>
          <span style={{ fontSize: 11, color: "#475569" }}>Metastability Research Dashboard</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {mode === "replay" && (
            <span style={{ fontSize: 10, background: "#a855f7", color: "#fff", borderRadius: 4, padding: "2px 8px", fontWeight: 700 }}>
              REPLAY MODE
            </span>
          )}
          {running && status?.totalSteps && status.totalSteps > 1 && (
            <span style={{ fontSize: 10, color: "#94a3b8" }}>
              Step {(status.currentStep ?? 0) + 1}/{status.totalSteps}
              {status.currentExperiment ? ` — ${status.currentExperiment}` : ""}
            </span>
          )}
          <StatusDot running={running} />
        </div>
      </div>

      {/* Phase bar */}
      <PhaseBar rows={displayRows} experimentStartMs={displayStartMs} />

      {/* Main grid */}
      <div style={mainGridStyle}>
        {/* Left: config panel */}
        <div style={leftPanelStyle}>
          <ConfigPanel
            running={running}
            onStart={handleStart}
            onStop={handleStop}
          />
        </div>

        {/* Center: topology + charts */}
        <div style={centerPanelStyle}>
          <Topology rows={displayRows} running={running} />
          <div style={{ marginTop: 8 }}>
            <Charts rows={displayRows} experimentStartMs={displayStartMs} />
          </div>
        </div>

        {/* Right: runs browser + log */}
        <div style={rightPanelStyle}>
          <div style={{ flex: "0 0 40%", minHeight: 0, display: "flex", flexDirection: "column" }}>
            <RunsBrowser onLoadRun={handleLoadRun} liveRunning={running} />
          </div>
          <div style={{ flex: "1 1 0", minHeight: 0, display: "flex", flexDirection: "column" }}>
            <LogPanel status={status} />
          </div>
        </div>
      </div>
    </div>
  );
}

function StatusDot({ running }: { running: boolean }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
      <div
        style={{
          width: 8,
          height: 8,
          borderRadius: "50%",
          background: running ? "#4ade80" : "#475569",
          animation: running ? "blink 1s ease-in-out infinite alternate" : undefined,
        }}
      />
      <style>{`@keyframes blink { from { opacity: 1; } to { opacity: 0.3; } }`}</style>
      <span style={{ fontSize: 10, color: running ? "#4ade80" : "#475569", fontWeight: 600 }}>
        {running ? "RUNNING" : "IDLE"}
      </span>
    </div>
  );
}

const appStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  height: "100vh",
  background: "#0f1117",
  overflow: "hidden",
};

const topBarStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  padding: "8px 16px",
  background: "#0f172a",
  borderBottom: "1px solid #1e293b",
  flexShrink: 0,
};

const mainGridStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "200px 1fr 220px",
  gap: 0,
  flex: 1,
  minHeight: 0,
  overflow: "hidden",
};

const leftPanelStyle: React.CSSProperties = {
  background: "#0f172a",
  borderRight: "1px solid #1e293b",
  overflowY: "auto",
  height: "100%",
};

const centerPanelStyle: React.CSSProperties = {
  overflowY: "auto",
  padding: 8,
  height: "100%",
};

const rightPanelStyle: React.CSSProperties = {
  background: "#0f172a",
  borderLeft: "1px solid #1e293b",
  display: "flex",
  flexDirection: "column",
  height: "100%",
  overflow: "hidden",
};
