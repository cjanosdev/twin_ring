import { spawn, ChildProcess } from "child_process";
import * as os from "os";
import * as path from "path";
import {
  CacheStrategy,
  ExperimentParams,
  ExperimentStatus,
  ExperimentStep,
  EXPERIMENT_REGISTRY,
} from "./types";

// Workspace root is 2 levels up from dashboard/backend/
export const WORKSPACE_ROOT = path.resolve(__dirname, "../../..");

const COMPOSE_FILE = "docker/docker-compose-baseline.yml";
const CACHE_NODE_SERVICES = ["cache_node_1", "cache_node_2", "cache_node_3"];
const CACHE_NODE_URLS = [
  "http://localhost:8001",
  "http://localhost:8002",
  "http://localhost:8003",
];
const NODE_HEALTH_TIMEOUT_MS = 30_000;

/**
 * Docker Desktop on macOS uses a user-scoped socket, not /var/run/docker.sock.
 * The compose file mounts ${DOCKER_SOCK} into the control container, so it must
 * be set the same way run_experiments.sh sets it.
 */
export function dockerSock(): string {
  return os.platform() === "darwin"
    ? path.join(os.homedir(), ".docker/run/docker.sock")
    : "/var/run/docker.sock";
}

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
  regular_rps:                    "TR_REGULAR_RPS",
  overload_rps:                   "TR_OVERLOAD_RPS",
  max_in_flight:                  "TR_MAX_IN_FLIGHT",
  baseline_offered_rps_tolerance: "TR_BASELINE_OFFERED_RPS_TOLERANCE",
  warmup_start_rps:               "TR_WARMUP_START_RPS",
  warmup_ramp_step_rps:           "TR_WARMUP_RAMP_STEP_RPS",
  overload_ramp_step_rps:         "TR_OVERLOAD_RAMP_STEP_RPS",
  // Older standalone experiments
  regular_workers:                "TR_REGULAR_WORKERS",
  regular_work_secs:              "TR_REGULAR_WORK_SECS",
  baseline_window_secs:           "TR_BASELINE_WINDOW_SECS",
  overload_workers:               "TR_OVERLOAD_WORKERS",
  overload_secs:                  "TR_OVERLOAD_SECS",
  outage_secs:                    "TR_OUTAGE_SECS",
  outage_node:                    "TR_OUTAGE_NODE",
  overload_ramp_step:             "TR_OVERLOAD_RAMP_STEP",
  overload_ramp_step_secs:        "TR_OVERLOAD_RAMP_STEP_SECS",
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

export function isRunning(): boolean {
  // An active sequence counts as running even when no child process is live —
  // e.g. while waiting for cache nodes to pass their health check between the
  // docker restart and the experiment spawn.
  if (state.sequence !== null && state.currentStep !== null) return true;
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

/** Run a command to completion, streaming its output into the log. Resolves with the exit code. */
function runToCompletion(
  cmd: string,
  args: string[],
  env: Record<string, string>
): Promise<number> {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, {
      cwd: WORKSPACE_ROOT,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    // Track it so stopExperiment() can interrupt a slow node restart.
    state.process = child;
    child.stdout?.on("data", (c: Buffer) => appendLog(`[docker] ${c.toString().trimEnd()}`));
    child.stderr?.on("data", (c: Buffer) => appendLog(`[docker] ${c.toString().trimEnd()}`));
    child.on("exit", (code) => {
      state.process = null;
      resolve(code ?? -1);
    });
    child.on("error", (err) => {
      appendLog(`[docker] spawn failed: ${err}`);
      state.process = null;
      resolve(-1);
    });
  });
}

/** Poll a node's root endpoint until it responds or the deadline passes. */
async function waitForNode(url: string): Promise<boolean> {
  const deadline = Date.now() + NODE_HEALTH_TIMEOUT_MS;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(2000) });
      if (res.ok) return true;
    } catch {
      // not up yet
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  return false;
}

/**
 * Restart the 3 cache nodes with CACHE_STRATEGY set, then wait for them to be healthy.
 *
 * This is the GUI equivalent of the per-strategy restart loop in run_experiments.sh.
 * Without it the experiment binary would run against whatever strategy the nodes were
 * last started with, silently producing mislabeled results. Cassandra and the control
 * API are left untouched.
 */
async function prepareNodes(strategy: CacheStrategy): Promise<{ ok: boolean; error?: string }> {
  appendLog(`[dashboard] Restarting cache nodes with CACHE_STRATEGY=${strategy}...`);

  const env: Record<string, string> = {
    ...(process.env as Record<string, string>),
    CACHE_STRATEGY: strategy,
    DOCKER_SOCK: dockerSock(),
  };

  const code = await runToCompletion(
    "docker",
    ["compose", "-f", COMPOSE_FILE, "up", "-d", ...CACHE_NODE_SERVICES],
    env
  );
  if (code !== 0) {
    return { ok: false, error: `docker compose up failed with code ${code}` };
  }

  for (const url of CACHE_NODE_URLS) {
    if (!(await waitForNode(url))) {
      return { ok: false, error: `node ${url} did not become healthy within 30s` };
    }
    appendLog(`[dashboard] ${url} ready`);
  }

  appendLog(`[dashboard] Cache nodes running ${strategy}`);
  return { ok: true };
}

function spawnStep(step: ExperimentStep): ChildProcess {
  const def = EXPERIMENT_REGISTRY.find((d) => d.id === step.experimentId);
  if (!def) throw new Error(`Unknown experiment: ${step.experimentId}`);

  const env = buildEnv(step.params);

  // Strategy-based experiments share one binary and take the strategy as an arg.
  // The binary re-verifies it against the running nodes and aborts on a mismatch,
  // so this must be the same value prepareNodes() restarted them with.
  const args = ["run", "-p", "twin_ring_exp", "--bin", def.bin];
  if (def.strategy) args.push("--", "--strategy", def.strategy, "--scenario", def.scenario ?? "overload");

  return spawn("cargo", args, {
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

  // A single-step run is just a one-element sequence — reuse the same path so
  // node preparation happens identically in both cases.
  return startSequence([{ experimentId, params }]);
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

  // Fire-and-forget: node preparation can take ~30s, so the HTTP response must not
  // wait on it. Progress is reported via /api/experiments/status.
  void runNextStep().catch((err) => {
    appendLog(`[dashboard] Sequence failed: ${err}`);
    state.exitCode = -1;
    state.currentStep = null;
    state.sequence = null;
  });
  return { ok: true };
}

async function runNextStep() {
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

  // Bring the cache nodes onto this experiment's strategy before measuring anything.
  if (def.strategy) {
    const prep = await prepareNodes(def.strategy);
    // stopExperiment() clears the sequence; honor that rather than pressing on.
    if (state.sequence === null || state.currentStep === null) {
      appendLog(`[dashboard] Aborted during node preparation`);
      return;
    }
    if (!prep.ok) {
      appendLog(`[dashboard] Node preparation failed: ${prep.error}`);
      state.exitCode = -1;
      state.currentStep = null;
      state.sequence = null;
      return;
    }
  }

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
      setTimeout(() => {
        void runNextStep().catch((e) => {
          appendLog(`[dashboard] Sequence failed: ${e}`);
          state.exitCode = -1;
          state.currentStep = null;
          state.sequence = null;
        });
      }, 1000);
    }
  });
}

export function stopExperiment(): { ok: boolean; error?: string } {
  if (!isRunning()) return { ok: false, error: "no running experiment" };
  // Abort the sequence so the next step doesn't start after this one exits.
  // runNextStep() re-checks these after awaiting node preparation.
  state.sequence = null;
  state.currentStep = null;
  // May be null if we are mid-health-check; clearing the sequence above is
  // what actually stops the run in that case.
  state.process?.kill("SIGTERM");
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
