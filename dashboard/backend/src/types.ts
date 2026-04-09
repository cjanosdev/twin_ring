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
  id: string;
  label: string;
  bin: string;
  paramKeys: (keyof ExperimentParams)[];
}

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

export interface RunInfo {
  path: string;
  filename: string;
  date: string;
  size_bytes: number;
}

// ── Experiment Registry ────────────────────────────────────────────────────────
// Add new experiments here. The dashboard will auto-discover them.

export const EXPERIMENT_REGISTRY: ExperimentDef[] = [
  {
    id: "baseline",
    label: "Baseline",
    bin: "baseline",
    paramKeys: [
      "key_space",
      "workers_per_shard",
      "poll_interval_secs",
      "request_timeout_ms",
      "baseline_warmup_secs",
      "baseline_warmup_start_workers",
      "baseline_warmup_ramp_step",
      "baseline_warmup_ramp_step_secs",
      "baseline_steady_secs",
    ],
  },
  {
    id: "simple_metastable",
    label: "Metastable",
    bin: "simple_metastable",
    paramKeys: [
      "key_space",
      "workers_per_shard",
      "poll_interval_secs",
      "request_timeout_ms",
      "warmup_secs",
      "warmup_start_workers",
      "warmup_ramp_step",
      "warmup_ramp_step_secs",
      "fault_workers",
      "fault_down_secs",
      "fault_ramp_step_secs",
      "observe_secs",
      "cassandra_mem_threshold_pct",
      "mem_poll_interval_ms",
    ],
  },
];
