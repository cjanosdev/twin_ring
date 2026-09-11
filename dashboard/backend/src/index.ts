import Fastify from "fastify";
import cors from "@fastify/cors";
import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import {
  startExperiment,
  startSequence,
  stopExperiment,
  getStatus,
  getRegistry,
} from "./experimentRunner";
import { watchCsv } from "./csvWatcher";
import { CsvRow, ExperimentParams, ExperimentStep, NoCacheRow, RunInfo, RunType, SequenceRequest } from "./types";
import { getInfraStatus, runInit, runInitClean, runUp, runDown } from "./infraRunner";

const WORKSPACE_ROOT = path.resolve(__dirname, "../../..");
const RUNS_DIR = path.join(WORKSPACE_ROOT, "experiment_results", "runs");
const RESULTS_DIR = path.join(WORKSPACE_ROOT, "experiment_results");
const CONTROL_API = "http://localhost:9000";

const app = Fastify({ logger: false });

function inferRunType(filename: string): RunType {
  if (filename.startsWith("simple_metastable")) return "metastable";
  if (filename.startsWith("baseline_no_cache")) return "baseline_no_cache";
  if (filename.startsWith("baseline_warmup"))   return "baseline_warmup";
  if (filename.startsWith("baseline_steady"))   return "baseline_steady";
  if (filename.startsWith("lru_"))              return "exp_lru";
  if (filename.startsWith("dual-ring_"))        return "exp_dual";
  if (filename.startsWith("ttl-tiered_"))       return "exp_ttl";
  if (filename.startsWith("leased_"))           return "exp_leased";
  if (filename.startsWith("combined_"))         return "exp_combined";
  return "unknown";
}

// ── Runs browser ──────────────────────────────────────────────────────────────

app.get("/api/runs", async () => {
  const runs: RunInfo[] = [];
  if (!fs.existsSync(RUNS_DIR)) return runs;

  for (const dateDir of fs.readdirSync(RUNS_DIR).sort().reverse()) {
    const datePath = path.join(RUNS_DIR, dateDir);
    if (!fs.statSync(datePath).isDirectory()) continue;
    for (const file of fs.readdirSync(datePath).sort().reverse()) {
      if (!file.endsWith(".csv")) continue;
      const filePath = path.join(datePath, file);
      runs.push({
        path: filePath,
        filename: file,
        date: dateDir,
        size_bytes: fs.statSync(filePath).size,
        run_type: inferRunType(file),
      });
    }
  }
  return runs;
});

app.get<{ Querystring: { path: string } }>("/api/runs/data", async (request, reply) => {
  const { path: filePath } = request.query;
  if (!filePath) {
    return reply.code(400).send({ error: "path required" });
  }
  const resolved = path.resolve(filePath);
  if (!resolved.startsWith(RUNS_DIR)) {
    return reply.code(403).send({ error: "invalid path" });
  }
  if (!fs.existsSync(resolved)) {
    return reply.code(404).send({ error: "file not found" });
  }
  const content = fs.readFileSync(resolved, "utf8");
  const records: CsvRow[] = parse(content, { columns: true, skip_empty_lines: true, cast: true });

  for (const row of records) {
    const portMatch = String(row.node).match(/:(\d+)$/);
    row.node_short = portMatch ? `node${parseInt(portMatch[1]) - 8000}` : row.node;
  }

  return records;
});

// Serve no-cache CSV rows (different schema: step/workers/ops_per_sec/…, no node column)
app.get<{ Querystring: { path: string } }>("/api/runs/no-cache-data", async (request, reply) => {
  const { path: filePath } = request.query;
  if (!filePath) return reply.code(400).send({ error: "path required" });
  const resolved = path.resolve(filePath);
  if (!resolved.startsWith(RUNS_DIR)) return reply.code(403).send({ error: "invalid path" });
  if (!fs.existsSync(resolved)) return reply.code(404).send({ error: "file not found" });
  const content = fs.readFileSync(resolved, "utf8");
  const records: NoCacheRow[] = parse(content, { columns: true, skip_empty_lines: true, cast: true });
  return records;
});

// Summary JSONs for the two baseline experiments
app.get("/api/baselines", async () => {
  const read = (file: string) => {
    const p = path.join(RESULTS_DIR, file);
    if (!fs.existsSync(p)) return null;
    try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return null; }
  };
  return {
    baseline:         read("baseline.json"),
    baseline_no_cache: read("baseline_no_cache.json"),
  };
});

// ── Experiment registry ───────────────────────────────────────────────────────

app.get("/api/experiments/registry", async () => {
  return getRegistry();
});

// ── Experiment control ────────────────────────────────────────────────────────

interface RunBody {
  experimentId: string;
  params?: ExperimentParams;
}

app.post<{ Body: RunBody }>("/api/experiments/run", async (request, reply) => {
  const { experimentId, params = {} } = request.body ?? {};
  if (!experimentId) {
    return reply.code(400).send({ ok: false, error: "experimentId required" });
  }
  const result = startExperiment(experimentId, params);
  return reply.code(result.ok ? 200 : 400).send(result);
});

app.post<{ Body: SequenceRequest }>("/api/experiments/run-sequence", async (request, reply) => {
  const { steps } = request.body ?? {};
  if (!steps || !Array.isArray(steps) || steps.length === 0) {
    return reply.code(400).send({ ok: false, error: "steps array required" });
  }
  const result = startSequence(steps as ExperimentStep[]);
  return reply.code(result.ok ? 200 : 400).send(result);
});

app.post("/api/experiments/stop", async (_, reply) => {
  return reply.send(stopExperiment());
});

app.get("/api/experiments/status", async () => {
  return getStatus();
});

// ── SSE stream ────────────────────────────────────────────────────────────────

app.get("/api/stream", async (request, reply) => {
  const status = getStatus();

  reply.raw.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
  });

  const sendEvent = (event: string, data: unknown) => {
    reply.raw.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };

  if (!status.csv_path) {
    sendEvent("waiting", { message: "no active experiment" });
    reply.raw.end();
    return reply;
  }

  reply.hijack();

  const csvPath = status.csv_path;

  request.socket.on("close", () => {
    reply.raw.end();
  });

  try {
    for await (const row of watchCsv(csvPath)) {
      if (reply.raw.destroyed) break;
      sendEvent("row", row);
    }
  } catch (err) {
    if (!reply.raw.destroyed) {
      sendEvent("error", { message: String(err) });
      reply.raw.end();
    }
  }

  return reply;
});

// ── Infrastructure control ────────────────────────────────────────────────────

app.get("/api/infra/status", async (_, reply) => {
  return reply.send(await getInfraStatus());
});

app.post("/api/infra/init", async (_, reply) => {
  const result = runInit();
  return reply.code(result.ok ? 200 : 400).send(result);
});

app.post("/api/infra/init-clean", async (_, reply) => {
  const result = runInitClean();
  return reply.code(result.ok ? 200 : 400).send(result);
});

app.post("/api/infra/up", async (_, reply) => {
  const result = runUp();
  return reply.code(result.ok ? 200 : 400).send(result);
});

app.post("/api/infra/down", async (_, reply) => {
  const result = runDown();
  return reply.code(result.ok ? 200 : 400).send(result);
});

// ── Cassandra mem proxy ───────────────────────────────────────────────────────

app.get("/api/cassandra/mem", async (_, reply) => {
  try {
    const res = await fetch(`${CONTROL_API}/cassandra-mem`, { signal: AbortSignal.timeout(3000) });
    if (!res.ok) return reply.code(502).send({ error: `upstream ${res.status}` });
    return reply.send(await res.json());
  } catch (err) {
    return reply.code(502).send({ error: String(err) });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────────

async function main() {
  await app.register(cors, {
    origin: ["http://localhost:5173", "http://127.0.0.1:5173"],
  });

  const PORT = 8080;
  await app.listen({ port: PORT, host: "0.0.0.0" });
  console.log(`TwinRing dashboard backend running on http://localhost:${PORT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
