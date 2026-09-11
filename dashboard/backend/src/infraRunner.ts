import { spawn, execFile } from "child_process";
import { InfraStatus } from "./types";
import { dockerSock, WORKSPACE_ROOT, isRunning as experimentIsRunning } from "./experimentRunner";

const INIT_COMPOSE_FILE = "docker/docker-compose-init.yml";
const BASELINE_COMPOSE_FILE = "docker/docker-compose-baseline.yml";
const CASSANDRA_VOLUME = "docker_cassandra_data";
const CACHE_NODE_URLS = [
  "http://localhost:8001",
  "http://localhost:8002",
  "http://localhost:8003",
];
const CONTROL_API_URL = "http://localhost:9000";

interface InfraState {
  busy: boolean;
  log: string[];
  process: ReturnType<typeof spawn> | null;
}

const state: InfraState = {
  busy: false,
  log: [],
  process: null,
};

function appendLog(line: string) {
  // Docker/compose output carries \r progress updates and other control chars
  // that break JSON serialization of the log array — strip everything but
  // printable ASCII and newlines.
  const clean = line.replace(/[^\x20-\x7E\n]/g, "").trim();
  if (!clean) return;
  for (const part of clean.split("\n")) {
    state.log.push(part);
  }
  while (state.log.length > 200) state.log.shift();
}

function execFileText(cmd: string, args: string[]): Promise<string> {
  return new Promise((resolve) => {
    execFile(cmd, args, { timeout: 5000 }, (err, stdout) => {
      resolve(err ? "" : stdout.trim());
    });
  });
}

async function checkCassandra(): Promise<InfraStatus["cassandra"]> {
  const out = await execFileText("docker", [
    "inspect",
    "--format",
    "{{.State.Health.Status}}",
    "cassandra",
  ]);
  if (out === "healthy") return "healthy";
  if (out === "starting") return "starting";
  if (out === "unhealthy") return "unhealthy";
  return "absent";
}

async function checkVolume(): Promise<boolean> {
  const out = await execFileText("docker", ["volume", "inspect", CASSANDRA_VOLUME]);
  return out.length > 0;
}

async function checkHttp(url: string): Promise<boolean> {
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(2000) });
    return res.ok;
  } catch {
    return false;
  }
}

export async function getInfraStatus(): Promise<InfraStatus> {
  const [cassandra, volumePresent, nodeChecks, controlApi] = await Promise.all([
    checkCassandra(),
    checkVolume(),
    Promise.all(CACHE_NODE_URLS.map(checkHttp)),
    checkHttp(CONTROL_API_URL),
  ]);

  return {
    cassandra,
    volumePresent,
    cacheNodes: { up: nodeChecks.filter(Boolean).length, total: CACHE_NODE_URLS.length },
    controlApi,
    busy: state.busy,
    log: [...state.log],
  };
}

/** Run a docker compose command to completion, streaming output into the infra log. */
function runToCompletion(args: string[]): Promise<number> {
  return new Promise((resolve) => {
    const env: Record<string, string> = {
      ...(process.env as Record<string, string>),
      DOCKER_SOCK: dockerSock(),
    };
    const child = spawn("docker", args, {
      cwd: WORKSPACE_ROOT,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    state.process = child;
    child.stdout?.on("data", (c: Buffer) => appendLog(c.toString().trimEnd()));
    child.stderr?.on("data", (c: Buffer) => appendLog(c.toString().trimEnd()));
    child.on("exit", (code) => {
      state.process = null;
      resolve(code ?? -1);
    });
    child.on("error", (err) => {
      appendLog(`spawn failed: ${err}`);
      state.process = null;
      resolve(-1);
    });
  });
}

function guardBusy(): { ok: boolean; error?: string } {
  if (state.busy) return { ok: false, error: "an infra action is already running" };
  if (experimentIsRunning()) return { ok: false, error: "an experiment is currently running" };
  return { ok: true };
}

async function runAction(label: string, steps: string[][]): Promise<{ ok: boolean; error?: string }> {
  state.busy = true;
  state.log = [];
  appendLog(`[dashboard] ${label}...`);
  try {
    for (const args of steps) {
      appendLog(`[dashboard] $ docker ${args.join(" ")}`);
      const code = await runToCompletion(args);
      if (code !== 0) {
        appendLog(`[dashboard] ${label} failed (exit ${code})`);
        return { ok: false, error: `command failed with code ${code}: docker ${args.join(" ")}` };
      }
    }
    appendLog(`[dashboard] ${label} complete`);
    return { ok: true };
  } finally {
    state.busy = false;
  }
}

/** One-time (idempotent) data init: create the volume + preload, without wiping existing data. */
export function runInit(): { ok: boolean; error?: string } {
  const guard = guardBusy();
  if (!guard.ok) return guard;
  void runAction("Initializing data volume", [
    ["compose", "-f", INIT_COMPOSE_FILE, "up", "--build"],
  ]);
  return { ok: true };
}

/** Destructive re-init: wipe the volume, then recreate + preload from scratch. */
export function runInitClean(): { ok: boolean; error?: string } {
  const guard = guardBusy();
  if (!guard.ok) return guard;
  void runAction("Wiping and re-initializing data volume", [
    ["compose", "-f", INIT_COMPOSE_FILE, "down", "-v"],
    ["compose", "-f", INIT_COMPOSE_FILE, "up", "--build"],
  ]);
  return { ok: true };
}

/** Start the baseline stack (Cassandra, 3 cache nodes, control API). */
export function runUp(): { ok: boolean; error?: string } {
  const guard = guardBusy();
  if (!guard.ok) return guard;
  void runAction("Starting baseline stack", [
    ["compose", "-f", BASELINE_COMPOSE_FILE, "up", "--build", "-d"],
  ]);
  return { ok: true };
}

/** Stop the baseline stack without touching the data volume. */
export function runDown(): { ok: boolean; error?: string } {
  const guard = guardBusy();
  if (!guard.ok) return guard;
  void runAction("Stopping baseline stack", [
    ["compose", "-f", BASELINE_COMPOSE_FILE, "down"],
  ]);
  return { ok: true };
}
