export interface CsvRow {
  timestamp_ms: number;
  phase: string;
  node: string;
  node_short: string;
  hits: number;
  misses: number;
  db_hits: number;
  db_not_found: number;
  db_errors: number;
  hit_rate: number;
  throughput_rps: number;
  db_call_rate: number;
  cache_p50_us: number;
  cache_p99_us: number;
  db_p50_us: number;
  db_p99_us: number;
}

/** All params shared across experiments. Each experiment only reads what it uses. */
export interface ExperimentParams {
  // Shared
  key_space?: number;
  workers_per_shard?: number;
  poll_interval_secs?: number;
  request_timeout_ms?: number;
  // Baseline
  baseline_warmup_secs?: number;
  baseline_warmup_start_workers?: number;
  baseline_warmup_ramp_step?: number;
  baseline_warmup_ramp_step_secs?: number;
  baseline_steady_secs?: number;
  // Metastable
  regular_rps?: number;
  overload_rps?: number;
  max_in_flight?: number;
  baseline_offered_rps_tolerance?: number;
  warmup_start_rps?: number;
  warmup_ramp_step_rps?: number;
  overload_ramp_step_rps?: number;
  // Older standalone experiments still use worker controls.
  regular_workers?: number;
  regular_work_secs?: number;
  baseline_window_secs?: number;
  overload_workers?: number;
  overload_secs?: number;
  outage_secs?: number;
  outage_node?: number;
  overload_ramp_step?: number;
  overload_ramp_step_secs?: number;
  warmup_secs?: number;
  warmup_start_workers?: number;
  warmup_ramp_step?: number;
  warmup_ramp_step_secs?: number;
  fault_down_secs?: number;
  observe_secs?: number;
  fault_workers?: number;
  fault_ramp_step_secs?: number;
  cassandra_mem_threshold_pct?: number;
  mem_poll_interval_ms?: number;
}

/** A registered experiment: human name → cargo binary name + which env vars it consumes. */
export interface ExperimentDef {
  scenario?: "overload" | "node-outage";
  id: string;
  label: string;
  bin: string;
  paramKeys: (keyof ExperimentParams)[];
  /**
   * CACHE_STRATEGY the cache nodes must be running for this experiment to be valid.
   *
   * The experiment binary only decides what goes in the output filename — the actual
   * cache behavior comes from how the *nodes* were started. Before spawning the binary,
   * the runner restarts the 3 cache nodes with this value (same as run_experiments.sh).
   * Omit for experiments that do not depend on the cache strategy (e.g. baseline_no_cache,
   * which bypasses the cache entirely).
   */
  strategy?: CacheStrategy;
}

/** Valid values for the CACHE_STRATEGY env var read by twin_ring_node (main.rs:431). */
export type CacheStrategy = "lru" | "dual-ring" | "ttl-tiered" | "leased" | "combined";

/** One step in a run sequence. */
export interface ExperimentStep {
  experimentId: string;
  params: ExperimentParams;
}

/** Request body for POST /api/experiments/run-sequence */
export interface SequenceRequest {
  steps: ExperimentStep[];
}

export interface ExperimentStatus {
  running: boolean;
  /** Which step (0-indexed) is currently executing, null if idle */
  currentStep: number | null;
  /** Total steps in the sequence, null if idle */
  totalSteps: number | null;
  /** Label of the currently running experiment */
  currentExperiment: string | null;
  csv_path: string | null;
  stdout_tail: string[];
  started_at: number | null;
  exit_code: number | null;
}

export type RunType = "metastable" | "baseline_warmup" | "baseline_steady" | "baseline_no_cache" | "exp_lru" | "exp_dual" | "exp_ttl" | "exp_leased" | "exp_combined" | "unknown";

export interface RunInfo {
  path: string;
  filename: string;
  date: string;
  size_bytes: number;
  run_type: RunType;
}

export interface BaselineSummary {
  cache_p50_us: number;
  db_p50_us: number;
  hit_rate: number;
  throughput_rps: number;
}

export interface BaselineNoCacheSummary {
  saturation_workers: number;
  saturation_ops_per_sec: number;
  last_clean_workers: number;
  last_clean_ops_per_sec: number;
  last_clean_db_p50_us: number;
}

export interface InfraStatus {
  cassandra: "healthy" | "starting" | "unhealthy" | "absent";
  volumePresent: boolean;
  cacheNodes: { up: number; total: number };
  controlApi: boolean;
  /** true while an infra action (init/init-clean/up/down) is in progress */
  busy: boolean;
  log: string[];
}

export interface NoCacheRow {
  step: number;
  workers: number;
  ops_per_sec: number;
  db_p50_us: number;
  db_p99_us: number;
  db_errors: number;
}

// ── Experiment Registry ────────────────────────────────────────────────────────
// Add new experiments here. The dashboard will auto-discover them.

const METASTABLE_PARAM_KEYS: (keyof ExperimentParams)[] = [
  "key_space",
  "regular_rps",
  "max_in_flight",
  "baseline_offered_rps_tolerance",
  "poll_interval_secs",
  "request_timeout_ms",
  "warmup_secs",
  "warmup_start_rps",
  "warmup_ramp_step_rps",
  "warmup_ramp_step_secs",
  "regular_work_secs",
  "baseline_window_secs",
  "overload_rps",
  "overload_secs",
  "overload_ramp_step_rps",
  "overload_ramp_step_secs",
  "observe_secs",

];

const CACHE_EXPERIMENTS: ExperimentDef[] = [
  {
    id: "exp_lru",
    label: "LRU (control)",
    bin: "exp",
    paramKeys: METASTABLE_PARAM_KEYS,
    strategy: "lru",
  },
  {
    id: "exp_dual",
    label: "Dual-Ring",
    bin: "exp",
    paramKeys: METASTABLE_PARAM_KEYS,
    strategy: "dual-ring",
  },
  {
    id: "exp_ttl",
    label: "TTL-Tiered",
    bin: "exp",
    paramKeys: METASTABLE_PARAM_KEYS,
    strategy: "ttl-tiered",
  },
  {
    id: "exp_leased",
    label: "Leased",
    bin: "exp",
    paramKeys: METASTABLE_PARAM_KEYS,
    strategy: "leased",
  },
  {
    id: "exp_combined",
    label: "Combined",
    bin: "exp",
    paramKeys: METASTABLE_PARAM_KEYS,
    strategy: "combined",
  },
];

// Each selection is an independent run with its own baseline and output files.
export const EXPERIMENT_REGISTRY: ExperimentDef[] = CACHE_EXPERIMENTS.flatMap(def => [
  { ...def, label: `${def.label} — Overload`, scenario: "overload" as const },
  { ...def, id: `${def.id}_outage`, label: `${def.label} — Overload + Node Outage`,
    scenario: "node-outage" as const,
    paramKeys: [...def.paramKeys, "outage_secs", "outage_node"] as (keyof ExperimentParams)[] },
]);
