import { useEffect, useRef, useState, useCallback } from "react";
import {
  CsvRow,
  ExperimentDef,
  ExperimentParams,
  ExperimentStatus,
  ExperimentStep,
  RunInfo,
  CassandraMem,
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
