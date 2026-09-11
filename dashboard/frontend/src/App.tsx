import { useState, useEffect, useCallback } from "react";
import { CsvRow, NoCacheRow, BaselineSummary, BaselineNoCacheSummary } from "./types";
import { useSSEStream, useStatusPoller, useInfraPoller, apiGetBaselines, apiListRuns, apiGetRunData, apiGetNoCacheData } from "./api";
import { PhaseBar } from "./components/PhaseBar";
import { Topology } from "./components/Topology";
import { Charts } from "./components/Charts";
import { BaselineCharts } from "./components/BaselineCharts";
import { BaselineNoCacheCharts } from "./components/BaselineNoCacheCharts";
import { ConfigPanel } from "./components/ConfigPanel";
import { InfraPanel } from "./components/InfraPanel";
import { LogPanel } from "./components/LogPanel";
import { RunsBrowser, LoadedRun } from "./components/RunsBrowser";
import { BaselineRunPicker } from "./components/BaselineRunPicker";
import { CompareSourcePicker, SelectedSources } from "./components/CompareSourcePicker";
import { CompareCharts, CompareData } from "./components/CompareCharts";

type Tab = "live" | "baseline" | "no_cache" | "compare";

type DisplayMode =
  | { kind: "live" }
  | { kind: "metastable"; rows: CsvRow[]; startMs: number | null }
  | { kind: "baseline_warmup" | "baseline_steady"; rows: CsvRow[]; startMs: number | null }
  | { kind: "baseline_no_cache"; rows: NoCacheRow[] };

export default function App() {
  const status = useStatusPoller(2000);
  const running = status?.running ?? false;
  const infraStatus = useInfraPoller(3000);
  const nodesUp = infraStatus ? infraStatus.cacheNodes.up === infraStatus.cacheNodes.total : true;

  const { rows: liveRows, clearRows } = useSSEStream(running);

  const [tab, setTab] = useState<Tab>("live");
  const [display, setDisplay] = useState<DisplayMode>({ kind: "live" });

  const [baselineSummary, setBaselineSummary] = useState<BaselineSummary | null>(null);
  const [noCacheSummary, setNoCacheSummary] = useState<BaselineNoCacheSummary | null>(null);

  // Rows auto-loaded for the baseline tabs
  const [baselineSteadyRows, setBaselineSteadyRows] = useState<CsvRow[]>([]);
  const [baselineSteadyStartMs, setBaselineSteadyStartMs] = useState<number | null>(null);
  const [noCacheRows, setNoCacheRows] = useState<NoCacheRow[]>([]);
  const [tabLoading, setTabLoading] = useState(false);

  // Selected run path per baseline tab (for the run picker highlight)
  const [selectedBaselinePath, setSelectedBaselinePath] = useState<string | null>(null);
  const [selectedNoCachePath, setSelectedNoCachePath] = useState<string | null>(null);

  // Compare tab state
  const [compareSources, setCompareSources] = useState<SelectedSources>({
    metastable: [], baseline_steady: [], baseline_no_cache: [],
  });
  const [compareData, setCompareData] = useState<CompareData>({
    metastable: [], baseline_steady: [], baseline_no_cache: [],
  });
  const [compareLoading, setCompareLoading] = useState(false);

  const refreshSummaries = useCallback(() => {
    apiGetBaselines().then(({ baseline, baseline_no_cache }) => {
      setBaselineSummary(baseline);
      setNoCacheSummary(baseline_no_cache);
    }).catch(() => {});
  }, []);

  useEffect(() => { refreshSummaries(); }, [refreshSummaries]);

  // Auto-load the latest matching CSV when switching to a baseline tab
  const loadLatestBaseline = useCallback(async () => {
    setTabLoading(true);
    try {
      const runs = await apiListRuns();
      const steady = runs.find((r) => r.run_type === "baseline_steady");
      if (steady) {
        const rows = await apiGetRunData(steady.path);
        setBaselineSteadyRows(rows);
        setBaselineSteadyStartMs(rows.length > 0 ? rows[0].timestamp_ms : null);
        setSelectedBaselinePath(steady.path);
      }
    } catch { /* backend not ready */ } finally {
      setTabLoading(false);
    }
  }, []);

  const loadLatestNoCache = useCallback(async () => {
    setTabLoading(true);
    try {
      const runs = await apiListRuns();
      const nc = runs.find((r) => r.run_type === "baseline_no_cache");
      if (nc) {
        const rows = await apiGetNoCacheData(nc.path);
        setNoCacheRows(rows);
        setSelectedNoCachePath(nc.path);
      }
    } catch { /* backend not ready */ } finally {
      setTabLoading(false);
    }
  }, []);

  const handleBaselinePickerSelect = useCallback(async (run: import("./types").RunInfo) => {
    setTabLoading(true);
    setSelectedBaselinePath(run.path);
    try {
      const rows = await apiGetRunData(run.path);
      const startMs = rows.length > 0 ? rows[0].timestamp_ms : null;
      setBaselineSteadyRows(rows);
      setBaselineSteadyStartMs(startMs);
      const kind = run.run_type === "baseline_warmup" ? "baseline_warmup" : "baseline_steady";
      setDisplay({ kind, rows, startMs });
    } catch { /* ignore */ } finally {
      setTabLoading(false);
    }
  }, []);

  const handleNoCachePickerSelect = useCallback(async (run: import("./types").RunInfo) => {
    setTabLoading(true);
    setSelectedNoCachePath(run.path);
    try {
      const rows = await apiGetNoCacheData(run.path);
      setNoCacheRows(rows);
      setDisplay({ kind: "baseline_no_cache", rows });
    } catch { /* ignore */ } finally {
      setTabLoading(false);
    }
  }, []);

  // Load rows for all selected compare sources whenever the selection changes
  const handleSourcesChange = useCallback(async (s: SelectedSources) => {
    setCompareSources(s);
    setCompareLoading(true);
    try {
      const [metastableEntries, steadyEntries, noCacheEntries] = await Promise.all([
        Promise.all(s.metastable.map(async (path) => {
          const rows = await apiGetRunData(path);
          return { label: path.split("/").pop()!.replace(/\.csv$/, ""), rows, startMs: rows[0]?.timestamp_ms ?? null };
        })),
        Promise.all(s.baseline_steady.map(async (path) => {
          const rows = await apiGetRunData(path);
          return { label: path.split("/").pop()!.replace(/\.csv$/, ""), rows, startMs: rows[0]?.timestamp_ms ?? null };
        })),
        Promise.all(s.baseline_no_cache.map(async (path) => {
          const rows = await apiGetNoCacheData(path);
          return { label: path.split("/").pop()!.replace(/\.csv$/, ""), rows };
        })),
      ]);
      setCompareData({ metastable: metastableEntries, baseline_steady: steadyEntries, baseline_no_cache: noCacheEntries });
    } catch { /* ignore */ } finally {
      setCompareLoading(false);
    }
  }, []);

  const handleTabChange = (t: Tab) => {
    setTab(t);
    if (t === "live") {
      setDisplay({ kind: "live" });
    } else if (t === "baseline") {
      setDisplay({ kind: "baseline_steady", rows: baselineSteadyRows, startMs: baselineSteadyStartMs });
      if (baselineSteadyRows.length === 0) loadLatestBaseline();
    } else if (t === "no_cache") {
      setDisplay({ kind: "baseline_no_cache", rows: noCacheRows });
      if (noCacheRows.length === 0) loadLatestNoCache();
    }
  };

  // Keep display in sync when auto-loaded rows arrive
  useEffect(() => {
    if (tab === "baseline") {
      setDisplay({ kind: "baseline_steady", rows: baselineSteadyRows, startMs: baselineSteadyStartMs });
    }
  }, [baselineSteadyRows, baselineSteadyStartMs, tab]);

  useEffect(() => {
    if (tab === "no_cache") {
      setDisplay({ kind: "baseline_no_cache", rows: noCacheRows });
    }
  }, [noCacheRows, tab]);

  // When a live experiment starts, switch back to live tab
  useEffect(() => {
    if (running) {
      setTab("live");
      setDisplay({ kind: "live" });
      clearRows();
    }
  }, [running]);

  const handleStart = () => {
    clearRows();
    setTab("live");
    setDisplay({ kind: "live" });
  };

  const handleStop = () => {};

  const handleLoadRun = (run: LoadedRun) => {
    refreshSummaries();
    // Switch to live tab but override display with the loaded run
    // (RunsBrowser picks a specific historical CSV — show it regardless of tab)
    if (run.type === "baseline_no_cache") {
      setTab("no_cache");
      setNoCacheRows(run.rows);
      setDisplay({ kind: "baseline_no_cache", rows: run.rows });
    } else if (run.type === "baseline_warmup") {
      setTab("baseline");
      setBaselineSteadyRows(run.rows);
      setBaselineSteadyStartMs(run.startMs);
      setDisplay({ kind: "baseline_warmup", rows: run.rows, startMs: run.startMs });
    } else if (run.type === "baseline_steady") {
      setTab("baseline");
      setBaselineSteadyRows(run.rows);
      setBaselineSteadyStartMs(run.startMs);
      setDisplay({ kind: "baseline_steady", rows: run.rows, startMs: run.startMs });
    } else {
      setTab("live");
      setDisplay({ kind: "metastable", rows: run.rows, startMs: run.startMs });
    }
  };

  const displayRows: CsvRow[] =
    display.kind === "live"
      ? liveRows
      : display.kind !== "baseline_no_cache"
      ? display.rows
      : [];

  const displayStartMs: number | null =
    display.kind === "live"
      ? liveRows.length > 0 ? liveRows[0].timestamp_ms : null
      : display.kind !== "baseline_no_cache"
      ? display.startMs
      : null;

  function renderCenterCharts() {
    if (display.kind === "baseline_no_cache") {
      return <BaselineNoCacheCharts rows={display.rows} summary={noCacheSummary} />;
    }
    if (display.kind === "baseline_warmup") {
      return (
        <BaselineCharts rows={display.rows} experimentStartMs={display.startMs} summary={null} phase="warmup" />
      );
    }
    if (display.kind === "baseline_steady") {
      return (
        <BaselineCharts rows={display.rows} experimentStartMs={display.startMs} summary={baselineSummary} phase="steady" />
      );
    }
    return <Charts rows={displayRows} experimentStartMs={displayStartMs} />;
  }

  const isReplayMetastable = display.kind === "metastable";

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
          {isReplayMetastable && (
            <span style={replayBadgeStyle}>REPLAY</span>
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

      {/* Tab bar */}
      <div style={tabBarStyle}>
        <TabBtn label="Live / Metastable" active={tab === "live"} color="#60a5fa" onClick={() => handleTabChange("live")} />
        <TabBtn label="Baseline" active={tab === "baseline"} color="#4ade80" onClick={() => handleTabChange("baseline")} />
        <TabBtn label="No-Cache Baseline" active={tab === "no_cache"} color="#fb923c" onClick={() => handleTabChange("no_cache")} />
        <TabBtn label="Compare" active={tab === "compare"} color="#a78bfa" onClick={() => handleTabChange("compare")} />
        {(tabLoading || compareLoading) && <span style={{ fontSize: 10, color: "#60a5fa", marginLeft: 8 }}>Loading…</span>}
      </div>

      {/* Phase bar — only for metastable/live */}
      {(display.kind === "live" || display.kind === "metastable") && (
        <PhaseBar rows={displayRows} experimentStartMs={displayStartMs} />
      )}

      {/* Main grid */}
      <div style={mainGridStyle}>
        <div style={leftPanelStyle}>
          {tab === "compare" ? (
            <CompareSourcePicker selected={compareSources} onChange={handleSourcesChange} />
          ) : tab === "baseline" ? (
            <BaselineRunPicker runType="baseline_steady" selectedPath={selectedBaselinePath} onSelect={handleBaselinePickerSelect} />
          ) : tab === "no_cache" ? (
            <BaselineRunPicker runType="baseline_no_cache" selectedPath={selectedNoCachePath} onSelect={handleNoCachePickerSelect} />
          ) : (
            <>
              <InfraPanel />
              <ConfigPanel running={running} onStart={handleStart} onStop={handleStop} nodesUp={nodesUp} />
            </>
          )}
        </div>

        <div style={centerPanelStyle}>
          {tab === "compare" ? (
            <CompareCharts sources={compareData} />
          ) : (
            <>
              {(display.kind === "live" || display.kind === "metastable") && (
                <Topology rows={displayRows} running={running} />
              )}
              <div style={{ marginTop: 8 }}>
                {renderCenterCharts()}
              </div>
            </>
          )}
        </div>

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

function TabBtn({ label, active, color, onClick }: { label: string; active: boolean; color: string; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      style={{
        fontSize: 11,
        fontWeight: active ? 700 : 400,
        color: active ? color : "#475569",
        background: "transparent",
        border: "none",
        borderBottom: active ? `2px solid ${color}` : "2px solid transparent",
        padding: "4px 14px",
        cursor: "pointer",
        letterSpacing: 0.3,
        transition: "color 0.15s, border-color 0.15s",
      }}
    >
      {label}
    </button>
  );
}

function StatusDot({ running }: { running: boolean }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
      <div
        style={{
          width: 8, height: 8, borderRadius: "50%",
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

const replayBadgeStyle: React.CSSProperties = {
  fontSize: 10, background: "#a855f7", color: "#fff",
  borderRadius: 4, padding: "2px 8px", fontWeight: 700,
};

const appStyle: React.CSSProperties = {
  display: "flex", flexDirection: "column", height: "100vh",
  background: "#0f1117", overflow: "hidden",
};

const topBarStyle: React.CSSProperties = {
  display: "flex", alignItems: "center", justifyContent: "space-between",
  padding: "8px 16px", background: "#0f172a", borderBottom: "1px solid #1e293b", flexShrink: 0,
};

const tabBarStyle: React.CSSProperties = {
  display: "flex", alignItems: "center",
  background: "#0f172a", borderBottom: "1px solid #1e293b",
  padding: "0 8px", flexShrink: 0,
};

const mainGridStyle: React.CSSProperties = {
  display: "grid", gridTemplateColumns: "340px 1fr 320px",
  gap: 0, flex: 1, minHeight: 0, overflow: "hidden",
};

const leftPanelStyle: React.CSSProperties = {
  background: "#0f172a", borderRight: "1px solid #1e293b", overflowY: "auto", height: "100%",
  display: "flex", flexDirection: "column",
};

const centerPanelStyle: React.CSSProperties = {
  overflowY: "auto", padding: 8, height: "100%",
};

const rightPanelStyle: React.CSSProperties = {
  background: "#0f172a", borderLeft: "1px solid #1e293b",
  display: "flex", flexDirection: "column", height: "100%", overflow: "hidden",
};
