import { useEffect, useRef, useState, useCallback } from "react";
import {
  BaselineNoCacheSummary,
  BaselineSummary,
  CassandraMem,
  CsvRow,
  ExperimentDef,
  ExperimentParams,
  ExperimentStatus,
  ExperimentStep,
  InfraStatus,
  NoCacheRow,
  RunInfo,
} from "./types";

const BASE = "/api";

export async function apiGetRegistry(): Promise<ExperimentDef[]> {
  const res = await fetch(`${BASE}/experiments/registry`);
  return res.json();
}

export async function apiRunExperiment(
  experimentId: string,
  params: ExperimentParams
): Promise<void> {
  const res = await fetch(`${BASE}/experiments/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ experimentId, params }),
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error ?? "Failed to start");
}

export async function apiRunSequence(steps: ExperimentStep[]): Promise<void> {
  const res = await fetch(`${BASE}/experiments/run-sequence`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ steps }),
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error ?? "Failed to start sequence");
}

export async function apiStopExperiment(): Promise<void> {
  await fetch(`${BASE}/experiments/stop`, { method: "POST" });
}

export async function apiGetStatus(): Promise<ExperimentStatus> {
  const res = await fetch(`${BASE}/experiments/status`);
  return res.json();
}

export async function apiListRuns(): Promise<RunInfo[]> {
  const res = await fetch(`${BASE}/runs`);
  return res.json();
}

export async function apiGetRunData(filePath: string): Promise<CsvRow[]> {
  const res = await fetch(`${BASE}/runs/data?path=${encodeURIComponent(filePath)}`);
  return res.json();
}

export async function apiGetNoCacheData(filePath: string): Promise<NoCacheRow[]> {
  const res = await fetch(`${BASE}/runs/no-cache-data?path=${encodeURIComponent(filePath)}`);
  return res.json();
}

export async function apiGetBaselines(): Promise<{
  baseline: BaselineSummary | null;
  baseline_no_cache: BaselineNoCacheSummary | null;
}> {
  const res = await fetch(`${BASE}/baselines`);
  return res.json();
}

export async function apiGetCassandraMem(): Promise<CassandraMem | null> {
  try {
    const res = await fetch(`${BASE}/cassandra/mem`, { signal: AbortSignal.timeout(2000) });
    if (!res.ok) return null;
    return res.json();
  } catch {
    return null;
  }
}

/**
 * Opens an EventSource to /api/stream and appends rows to state.
 * Reconnects automatically if the stream closes while experiment is running.
 */
export function useSSEStream(enabled: boolean): {
  rows: CsvRow[];
  clearRows: () => void;
} {
  const [rows, setRows] = useState<CsvRow[]>([]);
  const esRef = useRef<EventSource | null>(null);

  const clearRows = useCallback(() => setRows([]), []);

  useEffect(() => {
    if (!enabled) {
      esRef.current?.close();
      esRef.current = null;
      return;
    }

    let active = true;

    const connect = () => {
      if (!active) return;
      const es = new EventSource("/api/stream");
      esRef.current = es;

      es.addEventListener("row", (e: MessageEvent) => {
        try {
          const row: CsvRow = JSON.parse(e.data);
          setRows((prev) => [...prev, row]);
        } catch {
          // ignore parse errors
        }
      });

      es.addEventListener("waiting", () => {
        es.close();
        if (active) setTimeout(connect, 2000);
      });

      es.addEventListener("error", () => {
        es.close();
        if (active) setTimeout(connect, 2000);
      });
    };

    connect();

    return () => {
      active = false;
      esRef.current?.close();
      esRef.current = null;
    };
  }, [enabled]);

  return { rows, clearRows };
}

// ── Infrastructure control ────────────────────────────────────────────────────

export async function apiGetInfraStatus(): Promise<InfraStatus> {
  const res = await fetch(`${BASE}/infra/status`);
  return res.json();
}

async function postInfraAction(path: string): Promise<void> {
  const res = await fetch(`${BASE}/infra/${path}`, { method: "POST" });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error ?? `Failed: ${path}`);
}

export const apiInfraInit = () => postInfraAction("init");
export const apiInfraInitClean = () => postInfraAction("init-clean");
export const apiInfraUp = () => postInfraAction("up");
export const apiInfraDown = () => postInfraAction("down");

/**
 * Polls /api/infra/status every `intervalMs` milliseconds.
 */
export function useInfraPoller(intervalMs = 3000): InfraStatus | null {
  const [status, setStatus] = useState<InfraStatus | null>(null);

  useEffect(() => {
    let mounted = true;
    const poll = async () => {
      try {
        const s = await apiGetInfraStatus();
        if (mounted) setStatus(s);
      } catch {
        // backend not up yet
      }
    };
    poll();
    const id = setInterval(poll, intervalMs);
    return () => {
      mounted = false;
      clearInterval(id);
    };
  }, [intervalMs]);

  return status;
}

/**
 * Polls /api/experiments/status every `intervalMs` milliseconds.
 */
export function useStatusPoller(intervalMs = 2000): ExperimentStatus | null {
  const [status, setStatus] = useState<ExperimentStatus | null>(null);

  useEffect(() => {
    let mounted = true;
    const poll = async () => {
      try {
        const s = await apiGetStatus();
        if (mounted) setStatus(s);
      } catch {
        // backend not up yet
      }
    };
    poll();
    const id = setInterval(poll, intervalMs);
    return () => {
      mounted = false;
      clearInterval(id);
    };
  }, [intervalMs]);

  return status;
}
