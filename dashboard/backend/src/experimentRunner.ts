import { spawn, ChildProcess } from "child_process";
import * as path from "path";
import {
  ExperimentParams,
  ExperimentStatus,
  ExperimentStep,
  EXPERIMENT_REGISTRY,
} from "./types";

// Workspace root is 2 levels up from dashboard/backend/
const WORKSPACE_ROOT = path.resolve(__dirname, "../../..");

// Maps ExperimentParams keys → environment variable names.
// Each experiment only reads the vars relevant to it (Rust binary ignores unknown vars).
const ENV_VAR_MAP: Record<keyof ExperimentParams, string> = {
  // Shared
  key_space:                       "TR_KEY_SPACE",
  workers_per_shard:               "TR_WORKERS_PER_SHARD",
  poll_interval_secs:              "TR_POLL_INTERVAL_SECS",
  request_timeout_ms:              "TR_REQUEST_TIMEOUT_MS",
  // Baseline-specific (prefixed to avoid clashing with metastable warmup vars)
  baseline_warmup_secs:            "TR_BASELINE_WARMUP_SECS",
  baseline_warmup_start_workers:   "TR_BASELINE_WARMUP_START_WORKERS",
  baseline_warmup_ramp_step:       "TR_BASELINE_WARMUP_RAMP_STEP",
  baseline_warmup_ramp_step_secs:  "TR_BASELINE_WARMUP_RAMP_STEP_SECS",
  baseline_steady_secs:            "TR_BASELINE_STEADY_SECS",
  // Metastable-specific
  warmup_secs:                     "TR_WARMUP_SECS",
  warmup_start_workers:            "TR_WARMUP_START_WORKERS",
  warmup_ramp_step:                "TR_WARMUP_RAMP_STEP",
  warmup_ramp_step_secs:           "TR_WARMUP_RAMP_STEP_SECS",
  fault_down_secs:                 "TR_FAULT_DOWN_SECS",
  observe_secs:                    "TR_OBSERVE_SECS",
  fault_workers:                   "TR_FAULT_WORKERS",
  fault_ramp_step_secs:            "TR_FAULT_RAMP_STEP_SECS",
  cassandra_mem_threshold_pct:     "TR_CASSANDRA_MEM_THRESHOLD_PCT",
  mem_poll_interval_ms:            "TR_MEM_POLL_INTERVAL_MS",
};

interface RunnerState {
  process: ChildProcess | null;
  stdoutTail: string[];
  csvPath: string | null;
  startedAt: number | null;
  exitCode: number | null;
  /** Sequence state — null when idle */
  sequence: ExperimentStep[] | null;
  currentStep: number | null;
}

const state: RunnerState = {
  process: null,
  stdoutTail: [],
  csvPath: null,
  startedAt: null,
  exitCode: null,
  sequence: null,
  currentStep: null,
};

function isRunning(): boolean {
  return state.process !== null && state.process.exitCode === null;
}

function buildEnv(params: ExperimentParams): Record<string, string> {
  const env: Record<string, string> = { ...process.env } as Record<string, string>;
  for (const [paramKey, envKey] of Object.entries(ENV_VAR_MAP) as [keyof ExperimentParams, string][]) {
    const val = params[paramKey];
    if (val !== undefined && val !== null) {
      env[envKey] = String(val);
    }
  }
  return env;
}

function appendLog(line: string) {
  state.stdoutTail.push(line);
  if (state.stdoutTail.length > 200) state.stdoutTail.shift();
  // Extract CSV path from lines like: "📊 Metrics → /absolute/path.csv"
  if (line.includes("Metrics") && line.includes("→")) {
    const parts = line.split("→");
    if (parts.length >= 2) {
      state.csvPath = parts[parts.length - 1].trim().replace(/[^\x20-\x7E]/g, "").trim();
    }
  }
}

function spawnStep(step: ExperimentStep): ChildProcess {
  const def = EXPERIMENT_REGISTRY.find((d) => d.id === step.experimentId);
  if (!def) throw new Error(`Unknown experiment: ${step.experimentId}`);

  const env = buildEnv(step.params);

  return spawn("cargo", ["run", "-p", "twin_ring_exp", "--bin", def.bin], {
    cwd: WORKSPACE_ROOT,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function attachHandlers(child: ChildProcess, onDone: (code: number) => void) {
  let stdoutBuf = "";
  let stderrBuf = "";

  child.stdout?.on("data", (chunk: Buffer) => {
    stdoutBuf += chunk.toString();
    const lines = stdoutBuf.split("\n");
    stdoutBuf = lines.pop() ?? "";
    for (const l of lines) appendLog(l);
  });

  child.stderr?.on("data", (chunk: Buffer) => {
    stderrBuf += chunk.toString();
    const lines = stderrBuf.split("\n");
    stderrBuf = lines.pop() ?? "";
    for (const l of lines) appendLog(`[stderr] ${l}`);
  });

  child.on("exit", (code) => {
    if (stdoutBuf) appendLog(stdoutBuf);
    if (stderrBuf) appendLog(`[stderr] ${stderrBuf}`);
    onDone(code ?? -1);
  });
}

/** Run a single experiment step (no sequence context). */
export function startExperiment(
  experimentId: string,
  params: ExperimentParams
): { ok: boolean; error?: string } {
  if (isRunning()) return { ok: false, error: "experiment already running" };

  const def = EXPERIMENT_REGISTRY.find((d) => d.id === experimentId);
  if (!def) return { ok: false, error: `unknown experiment: ${experimentId}` };

  state.stdoutTail = [];
  state.csvPath = null;
  state.startedAt = Date.now();
  state.exitCode = null;
  state.sequence = [{ experimentId, params }];
  state.currentStep = 0;

  appendLog(`[dashboard] Starting ${def.label}`);

  try {
    const child = spawnStep({ experimentId, params });
    state.process = child;
    attachHandlers(child, (code) => {
      state.exitCode = code;
      state.process = null;
      appendLog(`[dashboard] ${def.label} exited (code ${code})`);
    });
    return { ok: true };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

/** Run a sequence of experiment steps, one after another. */
export function startSequence(
  steps: ExperimentStep[]
): { ok: boolean; error?: string } {
  if (isRunning()) return { ok: false, error: "experiment already running" };
  if (steps.length === 0) return { ok: false, error: "steps array is empty" };

  for (const step of steps) {
    if (!EXPERIMENT_REGISTRY.find((d) => d.id === step.experimentId)) {
      return { ok: false, error: `unknown experiment: ${step.experimentId}` };
    }
  }

  state.stdoutTail = [];
  state.csvPath = null;
  state.startedAt = Date.now();
  state.exitCode = null;
  state.sequence = steps;
  state.currentStep = 0;

  runNextStep();
  return { ok: true };
}

function runNextStep() {
  if (state.sequence === null || state.currentStep === null) return;
  if (state.currentStep >= state.sequence.length) {
    appendLog(`[dashboard] Sequence complete`);
    state.exitCode = 0;
    state.currentStep = null;
    return;
  }

  const step = state.sequence[state.currentStep];
  const def = EXPERIMENT_REGISTRY.find((d) => d.id === step.experimentId)!;
  const stepNum = state.currentStep + 1;
  const total = state.sequence.length;

  appendLog(`[dashboard] Step ${stepNum}/${total}: Starting ${def.label}`);

  let child: ChildProcess;
  try {
    child = spawnStep(step);
  } catch (err) {
    appendLog(`[dashboard] Failed to start ${def.label}: ${err}`);
    state.exitCode = -1;
    state.currentStep = null;
    return;
  }

  state.process = child;

  attachHandlers(child, (code) => {
    state.process = null;
    appendLog(`[dashboard] ${def.label} exited (code ${code})`);
    if (code !== 0) {
      appendLog(`[dashboard] Sequence aborted — step failed with code ${code}`);
      state.exitCode = code;
      state.currentStep = null;
      return;
    }
    if (state.sequence && state.currentStep !== null) {
      state.currentStep += 1;
      // Brief pause between steps so processes release ports cleanly
      setTimeout(runNextStep, 1000);
    }
  });
}

export function stopExperiment(): { ok: boolean; error?: string } {
  if (!isRunning()) return { ok: false, error: "no running experiment" };
  // Abort the sequence so the next step doesn't start after this one exits
  state.sequence = null;
  state.currentStep = null;
  state.process!.kill("SIGTERM");
  return { ok: true };
}

export function getStatus(): ExperimentStatus {
  const running = isRunning();
  const seq = state.sequence;
  const step = state.currentStep;
  const currentDef =
    seq && step !== null && step < seq.length
      ? EXPERIMENT_REGISTRY.find((d) => d.id === seq[step].experimentId) ?? null
      : null;

  return {
    running,
    currentStep: step,
    totalSteps: seq ? seq.length : null,
    currentExperiment: currentDef?.label ?? null,
    csv_path: state.csvPath,
    stdout_tail: [...state.stdoutTail],
    started_at: state.startedAt,
    exit_code: state.exitCode,
  };
}

export function getRegistry() {
  return EXPERIMENT_REGISTRY;
}
