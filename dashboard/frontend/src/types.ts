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

/** All params across all experiments. Each experiment only reads its own keys. */
export interface ExperimentParams {
  // Shared
  key_space?: number;
  workers_per_shard?: number;
  poll_interval_secs?: number;
  request_timeout_ms?: number;
  // Baseline-specific
  baseline_warmup_secs?: number;
  baseline_warmup_start_workers?: number;
  baseline_warmup_ramp_step?: number;
  baseline_warmup_ramp_step_secs?: number;
  baseline_steady_secs?: number;
  // Metastable-specific
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
  fault_workers?: number;
  fault_down_secs?: number;
  fault_ramp_step_secs?: number;
  observe_secs?: number;
  cassandra_mem_threshold_pct?: number;
  mem_poll_interval_ms?: number;
}

/** A registered experiment descriptor from /api/experiments/registry */
export interface ExperimentDef {
  scenario?: "overload" | "node-outage";
  id: string;
  label: string;
  bin: string;
  paramKeys: (keyof ExperimentParams)[];
}

/** One step in a run sequence */
export interface ExperimentStep {
  experimentId: string;
  params: ExperimentParams;
}

export interface ExperimentStatus {
  running: boolean;
  currentStep: number | null;
  totalSteps: number | null;
  currentExperiment: string | null;
  csv_path: string | null;
  stdout_tail: string[];
  started_at: number | null;
  exit_code: number | null;
}

export type RunType = "metastable" | "baseline_warmup" | "baseline_steady" | "baseline_no_cache" | "unknown";

export interface RunInfo {
  path: string;
  filename: string;
  date: string;
  size_bytes: number;
  run_type: RunType;
}

export interface NoCacheRow {
  step: number;
  workers: number;
  ops_per_sec: number;
  db_p50_us: number;
  db_p99_us: number;
  db_errors: number;
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
  busy: boolean;
  log: string[];
}

export interface CassandraMem {
  mem_pct: number;
  used_bytes: number;
  limit_bytes: number;
}

// Node colors: node1=blue, node2=orange, node3=green
export const NODE_COLORS: Record<string, string> = {
  node1: "#60a5fa",
  node2: "#fb923c",
  node3: "#4ade80",
};

export const PHASE_COLORS: Record<string, string> = {
  warmup:       "#3b82f6",
  regular_work: "#22c55e",
  overload:     "#ef4444",
  stopping_node: "#f97316",
  node_outage: "#dc2626",
  restarting_node: "#eab308",
  fault_inject: "#ef4444",
  observe:      "#a855f7",
};
