import * as fs from "fs";
import * as path from "path";

/**
 * Load the sidecar _summary.json for a CSV file.
 * Returns null if the sidecar does not exist.
 *
 * CSV path:     .../lru_metastable_143022.csv
 * Sidecar path: .../lru_metastable_143022_summary.json
 */
export function loadSummary(csvPath) {
  const dir  = path.dirname(csvPath);
  const base = path.basename(csvPath, ".csv");
  const sidecarPath = path.join(dir, `${base}_summary.json`);
  if (!fs.existsSync(sidecarPath)) return null;
  try {
    return JSON.parse(fs.readFileSync(sidecarPath, "utf8"));
  } catch {
    return null;
  }
}
