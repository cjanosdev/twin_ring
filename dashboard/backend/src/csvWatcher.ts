import * as fs from "fs";
import { CsvRow } from "./types";

const CSV_HEADERS = [
  "timestamp_ms", "phase", "node", "hits", "misses", "db_hits",
  "db_not_found", "db_errors", "hit_rate", "throughput_rps",
  "db_call_rate", "cache_p50_us", "cache_p99_us", "db_p50_us", "db_p99_us",
] as const;

function parseRow(line: string, headers: readonly string[]): CsvRow | null {
  const values = line.split(",");
  if (values.length !== headers.length) return null;

  const obj: Record<string, string | number> = {};
  for (let i = 0; i < headers.length; i++) {
    const key = headers[i];
    const raw = values[i].trim();
    // Numeric fields
    const numericFields = new Set([
      "timestamp_ms", "hits", "misses", "db_hits", "db_not_found", "db_errors",
      "hit_rate", "throughput_rps", "db_call_rate",
      "cache_p50_us", "cache_p99_us", "db_p50_us", "db_p99_us",
    ]);
    obj[key] = numericFields.has(key) ? parseFloat(raw) : raw;
  }

  const row = obj as unknown as CsvRow;
  // Derive node_short from port in URL (e.g. "http://localhost:8001" → "node1")
  const portMatch = row.node.match(/:(\d+)$/);
  if (portMatch) {
    const port = parseInt(portMatch[1]);
    row.node_short = `node${port - 8000}`;
  } else {
    row.node_short = row.node;
  }
  return row;
}

/**
 * Async generator that tails a CSV file and yields new rows as they appear.
 * Designed for use in SSE routes — call next() to get each row.
 */
export async function* watchCsv(csvPath: string): AsyncGenerator<CsvRow> {
  // Wait for the file to appear (up to 30s — cargo takes time to compile)
  const deadline = Date.now() + 30_000;
  while (!fs.existsSync(csvPath)) {
    if (Date.now() > deadline) {
      throw new Error(`CSV file never appeared: ${csvPath}`);
    }
    await sleep(500);
  }

  let offset = 0;
  let headerSkipped = false;

  while (true) {
    let stat: fs.Stats;
    try {
      stat = fs.statSync(csvPath);
    } catch {
      await sleep(500);
      continue;
    }

    if (stat.size > offset) {
      const fd = fs.openSync(csvPath, "r");
      const toRead = stat.size - offset;
      const buf = Buffer.alloc(toRead);
      const bytesRead = fs.readSync(fd, buf, 0, toRead, offset);
      fs.closeSync(fd);
      offset += bytesRead;

      const text = buf.slice(0, bytesRead).toString("utf8");
      const lines = text.split("\n");

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        if (!headerSkipped) {
          // Skip the CSV header row
          headerSkipped = true;
          continue;
        }

        const row = parseRow(trimmed, CSV_HEADERS);
        if (row) yield row;
      }
    }

    await sleep(500);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
