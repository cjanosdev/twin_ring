import { useState, useEffect } from "react";
import { ExperimentDef, ExperimentParams, ExperimentStep } from "../types";
import { apiRunExperiment, apiRunSequence, apiStopExperiment } from "../api";

interface Props {
  running: boolean;
  onStart: () => void;
  onStop: () => void;
  /** Whether all 3 cache nodes are currently reachable — undefined/true when unknown. */
  nodesUp?: boolean;
}

// ── Run modes ─────────────────────────────────────────────────────────────────

interface RunMode {
  id: string;
  label: string;
  /** Ordered experiment IDs to run. If single, array has one entry. */
  steps: string[];
}

// Default modes shown in the selector. Built from the known experiment IDs.
// When new experiments are added to the registry, they auto-appear in the
// "Single experiment" section; users can combine them however they want.
// Each experiment computes its own baseline inline from the regular-work tail, so
// there is no separate baseline step to sequence ahead of it.
const DEFAULT_MODES: RunMode[] = [
  { id: "lru_only",    label: "LRU — Overload (control)",        steps: ["exp_lru"] },
  { id: "lru_outage", label: "LRU — Overload + Node Outage (control)", steps: ["exp_lru_outage"] },
  { id: "all_five",    label: "All strategies",       steps: ["exp_lru", "exp_dual", "exp_ttl", "exp_leased", "exp_combined"] },
  { id: "lru_vs_dual", label: "LRU vs Dual-Ring",     steps: ["exp_lru", "exp_dual"] },
];

// ── Param metadata ────────────────────────────────────────────────────────────

const PARAM_META: Record<keyof ExperimentParams, { label: string; description: string }> = {
  // Shared
  key_space:                      { label: "Key Space",              description: "Total number of keys" },
  workers_per_shard:              { label: "Workers/Shard",          description: "Workers per shard (total = ×3)" },
  poll_interval_secs:             { label: "Poll Interval (s)",      description: "Metrics collection interval" },
  request_timeout_ms:             { label: "Request Timeout (ms)",   description: "HTTP attempt timeout; timeouts count as failed reads" },
  // Baseline
  baseline_warmup_secs:           { label: "Warmup Duration (s)",    description: "Total baseline warmup duration" },
  baseline_warmup_start_workers:  { label: "Warmup Start Workers",   description: "Initial worker count during ramp" },
  baseline_warmup_ramp_step:      { label: "Warmup Ramp Step",       description: "Workers added per ramp interval" },
  baseline_warmup_ramp_step_secs: { label: "Warmup Ramp Step (s)",   description: "Interval between ramp steps" },
  baseline_steady_secs:           { label: "Steady Duration (s)",    description: "Steady-state measurement window" },
  // Metastable
  regular_rps:                    { label: "Regular Rate (req/s)",   description: "Requests offered each second before overload and during recovery" },
  overload_rps:                   { label: "Overload Rate (req/s)",  description: "Peak requests offered each second during overload and node outage" },
  max_in_flight:                  { label: "Max In Flight",          description: "Safety bound; excess offered requests are recorded as shed demand" },
  baseline_offered_rps_tolerance: { label: "Rate Tolerance",         description: "Allowed fractional deviation between configured and measured baseline offered rate" },
  warmup_start_rps:               { label: "Warmup Start (req/s)",   description: "Initial offered request rate during cache warmup" },
  warmup_ramp_step_rps:           { label: "Warmup Step (req/s)",    description: "Offered rate added per warmup ramp interval" },
  overload_ramp_step_rps:         { label: "Overload Step (req/s)",  description: "Offered rate added per overload ramp interval" },
  regular_workers:               { label: "Regular Workers",        description: "Total workers before overload and during recovery" },
  regular_work_secs:             { label: "Regular Work (s)",        description: "Hold regular load and measure the baseline before overload" },
  baseline_window_secs:          { label: "Baseline Tail (s)",       description: "Complete regular-work tail used to establish the recovery reference" },
  overload_workers:              { label: "Overload Workers",       description: "Peak workers during overload and while the selected node is down" },
  overload_secs:                 { label: "Overload Duration (s)",   description: "Elevated-load phase before any node outage, including its ramp" },
  outage_secs:                   { label: "Node Down Duration (s)", description: "Keep the selected node down under elevated load; then restore regular load before restart (default 60)" },
  outage_node:                   { label: "Node to Stop", description: "Cache node 1, 2, or 3 (default 1)" },
  overload_ramp_step:            { label: "Overload Ramp Step",      description: "Workers added per overload ramp interval" },
  overload_ramp_step_secs:       { label: "Overload Ramp Step (s)",   description: "Interval between overload ramp steps" },
  warmup_secs:                    { label: "Warmup Duration (s)",    description: "Total warmup phase duration" },
  warmup_start_workers:           { label: "Warmup Start Workers",   description: "Initial worker count during ramp" },
  warmup_ramp_step:               { label: "Warmup Ramp Step",       description: "Workers added per ramp interval" },
  warmup_ramp_step_secs:          { label: "Warmup Ramp Step (s)",   description: "Interval between ramp steps" },
  fault_workers:                  { label: "Fault Surge Workers",    description: "Peak workers during fault inject" },
  fault_down_secs:                { label: "Fault Cap (s)",          description: "Max time in fault inject phase" },
  fault_ramp_step_secs:           { label: "Fault Ramp Step (s)",    description: "Interval between surge ramp steps" },
  observe_secs:                   { label: "Observe Duration (s)",   description: "Observation phase duration" },
  cassandra_mem_threshold_pct:    { label: "Cass Mem Threshold (%)", description: "Memory % to trigger node restart" },
  mem_poll_interval_ms:           { label: "Mem Poll (ms)",          description: "How often to check Cassandra memory" },
};

// Default values per experiment (only the params that experiment owns)
const FIXED_RATE_DEFAULTS: Partial<ExperimentParams> = {
  key_space: 100_000,
  regular_rps: 7_000,
  overload_rps: 20_000,
  max_in_flight: 12_000,
  baseline_offered_rps_tolerance: 0.05,
  poll_interval_secs: 5,
  request_timeout_ms: 500,
  warmup_secs: 90,
  warmup_start_rps: 1_000,
  warmup_ramp_step_rps: 1_000,
  warmup_ramp_step_secs: 10,
  regular_work_secs: 90,
  baseline_window_secs: 60,
  overload_secs: 90,
  overload_ramp_step_rps: 1_000,
  overload_ramp_step_secs: 3,
  observe_secs: 180,
};

const DEFAULTS_BY_EXPERIMENT: Record<string, Partial<ExperimentParams>> = {
  exp_lru: FIXED_RATE_DEFAULTS,
  exp_lru_outage: { ...FIXED_RATE_DEFAULTS, outage_secs: 60, outage_node: 1 },
  exp_dual: FIXED_RATE_DEFAULTS,
  exp_dual_outage: { ...FIXED_RATE_DEFAULTS, outage_secs: 60, outage_node: 1 },
  exp_ttl: FIXED_RATE_DEFAULTS,
  exp_ttl_outage: { ...FIXED_RATE_DEFAULTS, outage_secs: 60, outage_node: 1 },
  exp_leased: FIXED_RATE_DEFAULTS,
  exp_leased_outage: { ...FIXED_RATE_DEFAULTS, outage_secs: 60, outage_node: 1 },
  exp_combined: FIXED_RATE_DEFAULTS,
  exp_combined_outage: { ...FIXED_RATE_DEFAULTS, outage_secs: 60, outage_node: 1 },
  baseline: {
    key_space:                      100_000,
    workers_per_shard:              50,
    poll_interval_secs:             5,
    request_timeout_ms:             2000,
    baseline_warmup_secs:           120,
    baseline_warmup_start_workers:  15,
    baseline_warmup_ramp_step:      15,
    baseline_warmup_ramp_step_secs: 15,
    baseline_steady_secs:           60,
  },
  simple_metastable: {
    key_space:                   100_000,
    workers_per_shard:           50,
    poll_interval_secs:          5,
    request_timeout_ms:          2000,
    warmup_secs:                 150,
    warmup_start_workers:        15,
    warmup_ramp_step:            15,
    warmup_ramp_step_secs:       15,
    fault_workers:               250,
    fault_down_secs:             120,
    fault_ramp_step_secs:        5,
    observe_secs:                120,
    cassandra_mem_threshold_pct: 90,
    mem_poll_interval_ms:        3000,
  },
};

// ── Component ─────────────────────────────────────────────────────────────────

export function ConfigPanel({ running, onStart, onStop, nodesUp = true }: Props) {
  const [registry, setRegistry] = useState<ExperimentDef[]>([]);
  const [selectedModeId, setSelectedModeId] = useState<string>(DEFAULT_MODES[0].id);
  const [customSteps, setCustomSteps] = useState<string[]>([]);
  const [activeTab, setActiveTab] = useState<string>("");

  // Per-experiment param state (keyed by experiment id)
  const [paramsByExp, setParamsByExp] = useState<Record<string, Record<string, number>>>({});

  const [error, setError] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);

  // Fetch registry on mount
  useEffect(() => {
    fetch("/api/experiments/registry")
      .then((r) => r.json())
      .then((defs: ExperimentDef[]) => {
        setRegistry(defs);
        // Initialize param state from defaults
        const initial: Record<string, Record<string, number>> = {};
        for (const def of defs) {
          initial[def.id] = { ...(DEFAULTS_BY_EXPERIMENT[def.id] ?? {}) } as Record<string, number>;
        }
        setParamsByExp(initial);
      })
      .catch(() => {/* backend not ready */});
  }, []);

  // Derive current mode's step list
  const currentMode = DEFAULT_MODES.find((m) => m.id === selectedModeId);
  const isCustom = selectedModeId === "custom";
  const activeSteps: string[] = isCustom ? customSteps : (currentMode?.steps ?? []);

  // Keep activeTab pointing at a valid step
  useEffect(() => {
    if (activeSteps.length > 0 && !activeSteps.includes(activeTab)) {
      setActiveTab(activeSteps[0]);
    }
  }, [activeSteps, activeTab]);

  // Build extra modes from registry for experiments not in DEFAULT_MODES
  const knownIds = new Set(DEFAULT_MODES.flatMap((m) => m.steps));
  const extraModes: RunMode[] = registry
    .filter((d) => !knownIds.has(d.id))
    .map((d) => ({ id: `single_${d.id}`, label: d.label, steps: [d.id] }));

  const allModes: RunMode[] = [
    ...DEFAULT_MODES,
    ...extraModes,
    { id: "custom", label: "Custom sequence…", steps: [] },
  ];

  const handleRun = async () => {
    setError(null);
    setStarting(true);
    try {
      if (activeSteps.length === 0) throw new Error("No experiments selected");

      if (activeSteps.length === 1) {
        const id = activeSteps[0];
        await apiRunExperiment(id, paramsByExp[id] ?? {});
      } else {
        const steps: ExperimentStep[] = activeSteps.map((id) => ({
          experimentId: id,
          params: paramsByExp[id] ?? {},
        }));
        await apiRunSequence(steps);
      }
      onStart();
    } catch (e) {
      setError(String(e));
    } finally {
      setStarting(false);
    }
  };

  const handleStop = async () => {
    await apiStopExperiment();
    onStop();
  };

  const handleResetTab = () => {
    if (!activeTab) return;
    setParamsByExp((prev) => ({
      ...prev,
      [activeTab]: { ...(DEFAULTS_BY_EXPERIMENT[activeTab] ?? {}) } as Record<string, number>,
    }));
  };

  const activeTabDef = registry.find((d) => d.id === activeTab);
  const activeTabParams = paramsByExp[activeTab] ?? {};

  return (
    <div style={containerStyle}>
      <div style={titleStyle}>EXPERIMENT CONFIG</div>

      {/* Mode selector */}
      <div style={{ marginBottom: 10 }}>
        <label style={labelStyle}>Run Mode</label>
        <select
          value={selectedModeId}
          onChange={(e) => setSelectedModeId(e.target.value)}
          disabled={running}
          style={selectStyle(running)}
        >
          {allModes.map((m) => (
            <option key={m.id} value={m.id}>{m.label}</option>
          ))}
        </select>
      </div>

      {/* Custom sequence builder */}
      {isCustom && !running && (
        <CustomSequenceBuilder
          registry={registry}
          steps={customSteps}
          onChange={setCustomSteps}
        />
      )}

      {/* Per-experiment config tabs */}
      {activeSteps.length > 1 && (
        <div style={tabRowStyle}>
          {activeSteps.map((id) => {
            const def = registry.find((d) => d.id === id);
            return (
              <button
                key={id}
                onClick={() => setActiveTab(id)}
                style={tabStyle(id === activeTab)}
              >
                {def?.label ?? id}
              </button>
            );
          })}
        </div>
      )}

      {/* Param fields */}
      <div style={{ paddingRight: 2, marginTop: 4 }}>
        {activeTabDef ? (
          activeTabDef.paramKeys.map((key) => {
            const meta = PARAM_META[key];
            if (!meta) return null;
            return (
              <div key={key} style={fieldStyle}>
                <label style={labelStyle} title={meta.description}>{meta.label}</label>
                <input
                  type="number"
                  value={activeTabParams[key] ?? ""}
                  onChange={(e) =>
                    setParamsByExp((prev) => ({
                      ...prev,
                      [activeTab]: { ...prev[activeTab], [key]: parseFloat(e.target.value) },
                    }))
                  }
                  disabled={running}
                  style={inputStyle(running)}
                />
              </div>
            );
          })
        ) : (
          <div style={{ color: "#475569", fontSize: 11 }}>Select a mode to configure params.</div>
        )}
      </div>

      {!running && !nodesUp && (
        <div style={{ color: "#fbbf24", fontSize: 11, marginTop: 8, wordBreak: "break-word" }}>
          Cache nodes not running — start the stack above first.
        </div>
      )}

      {error && (
        <div style={{ color: "#ef4444", fontSize: 11, marginTop: 8, wordBreak: "break-word" }}>
          {error}
        </div>
      )}

      <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 6 }}>
        {!running ? (
          <>
            <button
              onClick={handleRun}
              disabled={starting || activeSteps.length === 0}
              style={btnStyle("#16a34a", starting || activeSteps.length === 0)}
            >
              {starting ? "Starting…" : activeSteps.length > 1 ? "▶ Run Sequence" : "▶ Run Experiment"}
            </button>
            {activeTabDef && (
              <button onClick={handleResetTab} style={btnStyle("#334155", false)}>
                Reset {activeTabDef.label} Defaults
              </button>
            )}
          </>
        ) : (
          <button onClick={handleStop} style={btnStyle("#dc2626", false)}>
            ■ Stop
          </button>
        )}
      </div>
    </div>
  );
}

// ── Custom sequence builder ───────────────────────────────────────────────────

function CustomSequenceBuilder({
  registry,
  steps,
  onChange,
}: {
  registry: ExperimentDef[];
  steps: string[];
  onChange: (s: string[]) => void;
}) {
  const [adding, setAdding] = useState(registry[0]?.id ?? "");

  useEffect(() => {
    if (registry.length > 0 && !adding) setAdding(registry[0].id);
  }, [registry]);

  return (
    <div style={{ marginBottom: 8, background: "#1e293b", borderRadius: 6, padding: 8 }}>
      <div style={{ fontSize: 10, color: "#94a3b8", marginBottom: 6 }}>Sequence order:</div>
      {steps.length === 0 && (
        <div style={{ fontSize: 10, color: "#475569", marginBottom: 4 }}>No experiments added.</div>
      )}
      {steps.map((id, i) => {
        const def = registry.find((d) => d.id === id);
        return (
          <div key={`${id}-${i}`} style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 4 }}>
            <span style={{ fontSize: 10, color: "#64748b", minWidth: 14 }}>{i + 1}.</span>
            <span style={{ fontSize: 11, color: "#e2e8f0", flex: 1 }}>{def?.label ?? id}</span>
            <button
              onClick={() => onChange(steps.filter((_, j) => j !== i))}
              style={{ ...smallBtnStyle, color: "#ef4444" }}
            >✕</button>
            {i > 0 && (
              <button
                onClick={() => {
                  const next = [...steps];
                  [next[i - 1], next[i]] = [next[i], next[i - 1]];
                  onChange(next);
                }}
                style={smallBtnStyle}
              >↑</button>
            )}
            {i < steps.length - 1 && (
              <button
                onClick={() => {
                  const next = [...steps];
                  [next[i], next[i + 1]] = [next[i + 1], next[i]];
                  onChange(next);
                }}
                style={smallBtnStyle}
              >↓</button>
            )}
          </div>
        );
      })}
      <div style={{ display: "flex", gap: 4, marginTop: 6 }}>
        <select
          value={adding}
          onChange={(e) => setAdding(e.target.value)}
          style={{ ...selectStyle(false), flex: 1, fontSize: 10 }}
        >
          {registry.map((d) => (
            <option key={d.id} value={d.id}>{d.label}</option>
          ))}
        </select>
        <button
          onClick={() => { if (adding) onChange([...steps, adding]); }}
          style={{ ...btnStyle("#1d4ed8", false), padding: "4px 8px", fontSize: 11, width: "auto" }}
        >
          + Add
        </button>
      </div>
    </div>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────

const containerStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  padding: "12px 10px",
};

const titleStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  color: "#64748b",
  letterSpacing: 1,
  marginBottom: 10,
};

const fieldStyle: React.CSSProperties = { marginBottom: 6 };

const labelStyle: React.CSSProperties = {
  display: "block",
  fontSize: 10,
  color: "#94a3b8",
  marginBottom: 2,
  cursor: "help",
};

const inputStyle = (disabled: boolean): React.CSSProperties => ({
  width: "100%",
  background: disabled ? "#0f172a" : "#1e293b",
  border: "1px solid #334155",
  borderRadius: 4,
  color: disabled ? "#64748b" : "#e2e8f0",
  fontSize: 12,
  padding: "4px 8px",
  outline: "none",
  cursor: disabled ? "not-allowed" : "text",
  boxSizing: "border-box",
});

const selectStyle = (disabled: boolean): React.CSSProperties => ({
  width: "100%",
  background: disabled ? "#0f172a" : "#1e293b",
  border: "1px solid #334155",
  borderRadius: 4,
  color: disabled ? "#64748b" : "#e2e8f0",
  fontSize: 11,
  padding: "4px 6px",
  outline: "none",
  cursor: disabled ? "not-allowed" : "pointer",
  boxSizing: "border-box",
});

const btnStyle = (bg: string, disabled: boolean): React.CSSProperties => ({
  background: disabled ? "#1e293b" : bg,
  color: disabled ? "#64748b" : "#fff",
  border: "none",
  borderRadius: 6,
  padding: "8px 12px",
  fontSize: 12,
  fontWeight: 600,
  cursor: disabled ? "not-allowed" : "pointer",
  width: "100%",
});

const tabRowStyle: React.CSSProperties = {
  display: "flex",
  gap: 4,
  marginBottom: 4,
  flexWrap: "wrap",
};

const tabStyle = (active: boolean): React.CSSProperties => ({
  fontSize: 10,
  fontWeight: active ? 700 : 400,
  padding: "3px 8px",
  borderRadius: 4,
  border: `1px solid ${active ? "#3b82f6" : "#334155"}`,
  background: active ? "#1e3a5f" : "#1e293b",
  color: active ? "#93c5fd" : "#94a3b8",
  cursor: "pointer",
});

const smallBtnStyle: React.CSSProperties = {
  fontSize: 10,
  padding: "1px 5px",
  background: "#0f172a",
  border: "1px solid #334155",
  borderRadius: 3,
  color: "#94a3b8",
  cursor: "pointer",
};
