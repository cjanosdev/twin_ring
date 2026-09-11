import { parse } from "csv-parse/sync";
import * as fs from "fs";

/**
 * Parse a CSV file into an array of row objects.
 * Numeric columns are cast automatically; string columns kept as-is.
 * Returns { rows, strategy } where strategy is inferred from the filename.
 */
export function loadCsv(csvPath) {
  const content = fs.readFileSync(csvPath, "utf8");
  const rows = parse(content, {
    columns: true,
    skip_empty_lines: true,
    cast: true,
  });
  const filename = csvPath.split("/").pop();
  const strategy = inferStrategy(filename);
  return { rows, strategy, filename };
}

export function inferStrategy(filename) {
  if (filename.startsWith("lru_"))        return "lru";
  if (filename.startsWith("dual-ring_"))  return "dual-ring";
  if (filename.startsWith("ttl-tiered_")) return "ttl-tiered";
  if (filename.startsWith("leased_"))     return "leased";
  if (filename.startsWith("combined_"))   return "combined";
  // Legacy / control
  if (filename.startsWith("simple_metastable")) return "lru";
  return "unknown";
}

export const STRATEGY_COLORS = {
  "lru":       "#94a3b8",
  "dual-ring": "#60a5fa",
  "ttl-tiered":"#4ade80",
  "leased":    "#fb923c",
  "combined":  "#a78bfa",
  "unknown":   "#e2e8f0",
};
