import { useEffect, useState } from "react";
import { RunInfo, RunType } from "../types";
import { apiListRuns } from "../api";

export interface SelectedSources {
  metastable: string[];
  baseline_steady: string[];
  baseline_no_cache: string[];
}

interface Props {
  selected: SelectedSources;
  onChange: (s: SelectedSources) => void;
}

const TYPE_CONFIG: Array<{
  key: keyof SelectedSources;
  runType: RunType;
  label: string;
  color: string;
}> = [
  { key: "metastable",       runType: "metastable",       label: "Metastable",       color: "#ef4444" },
  { key: "baseline_steady",  runType: "baseline_steady",  label: "Baseline Steady",  color: "#4ade80" },
  { key: "baseline_no_cache",runType: "baseline_no_cache",label: "No-Cache Baseline", color: "#fb923c" },
];

function shortLabel(run: RunInfo): string {
  const name = run.filename.replace(/\.[^.]+$/, "");
  return name.length > 30 ? name.slice(0, 29) + "…" : name;
}

function formatBytes(b: number): string {
  if (b < 1024) return `${b} B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
  return `${(b / 1024 / 1024).toFixed(1)} MB`;
}

export function CompareSourcePicker({ selected, onChange }: Props) {
  const [runs, setRuns] = useState<RunInfo[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    apiListRuns().then((r) => {
      if (!active) return;
      setRuns(r);
      setLoading(false);

      // Pre-select most recent of each visible type
      const next: SelectedSources = {
        metastable: [],
        baseline_steady: [],
        baseline_no_cache: [],
      };

      for (const tc of TYPE_CONFIG) {
        const matching = r
          .filter((run) => run.run_type === tc.runType)
          .sort((a, b) => b.date.localeCompare(a.date));
        if (matching.length > 0) {
          next[tc.key] = [matching[0].path];
        }
      }

      onChange(next);
    }).catch(() => {
      if (active) setLoading(false);
    });

    return () => { active = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function toggle(key: keyof SelectedSources, path: string) {
    const current = selected[key];
    const next = current.includes(path)
      ? current.filter((p) => p !== path)
      : [...current, path];
    onChange({ ...selected, [key]: next });
  }

  if (loading) {
    return (
      <div style={containerStyle}>
        <div style={headerStyle}>Compare Sources</div>
        <div style={{ color: "#64748b", fontSize: 12, padding: "12px 0" }}>Loading runs…</div>
      </div>
    );
  }

  if (runs.length === 0) {
    return (
      <div style={containerStyle}>
        <div style={headerStyle}>Compare Sources</div>
        <div style={{ color: "#64748b", fontSize: 12, padding: "12px 0" }}>No runs found.</div>
      </div>
    );
  }

  return (
    <div style={containerStyle}>
      <div style={headerStyle}>Compare Sources</div>

      {TYPE_CONFIG.map((tc) => {
        const matching = runs
          .filter((r) => r.run_type === tc.runType)
          .sort((a, b) => b.date.localeCompare(a.date));

        if (matching.length === 0) return null;

        // Group by date
        const byDate = new Map<string, RunInfo[]>();
        for (const run of matching) {
          if (!byDate.has(run.date)) byDate.set(run.date, []);
          byDate.get(run.date)!.push(run);
        }

        const dates = [...byDate.keys()].sort((a, b) => b.localeCompare(a));

        return (
          <div key={tc.key} style={sectionStyle}>
            {/* Type header */}
            <div style={{ ...sectionHeaderStyle, borderLeftColor: tc.color }}>
              <span style={{ color: tc.color, fontWeight: 700, fontSize: 11, letterSpacing: 0.5 }}>
                {tc.label.toUpperCase()}
              </span>
              <span style={{ color: "#475569", fontSize: 10, fontWeight: 400 }}>
                {matching.length} run{matching.length !== 1 ? "s" : ""}
              </span>
            </div>

            {dates.map((date) => (
              <div key={date} style={{ marginBottom: 6 }}>
                {/* Date sub-header */}
                <div style={dateHeaderStyle}>{date}</div>

                {byDate.get(date)!.map((run) => {
                  const checked = selected[tc.key].includes(run.path);
                  return (
                    <label key={run.path} style={rowStyle}>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggle(tc.key, run.path)}
                        style={checkboxStyle}
                      />
                      <span
                        style={{
                          ...labelTextStyle,
                          color: checked ? "#e2e8f0" : "#64748b",
                        }}
                        title={run.filename}
                      >
                        {shortLabel(run)}
                      </span>
                      <span style={sizeStyle}>{formatBytes(run.size_bytes)}</span>
                    </label>
                  );
                })}
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}

// ---- Styles ----

const containerStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  height: "100%",
  overflowY: "auto",
  padding: "12px 10px",
  background: "#0f172a",
  borderRight: "1px solid #1e293b",
  minWidth: 0,
};

const headerStyle: React.CSSProperties = {
  fontSize: 12,
  fontWeight: 700,
  color: "#94a3b8",
  letterSpacing: 0.6,
  marginBottom: 12,
  textTransform: "uppercase",
};

const sectionStyle: React.CSSProperties = {
  marginBottom: 16,
};

const sectionHeaderStyle: React.CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  borderLeft: "3px solid",
  paddingLeft: 8,
  marginBottom: 8,
};

const dateHeaderStyle: React.CSSProperties = {
  fontSize: 9,
  color: "#475569",
  letterSpacing: 0.5,
  textTransform: "uppercase",
  marginBottom: 4,
  marginLeft: 2,
  paddingTop: 2,
  borderTop: "1px solid #1e293b",
};

const rowStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 6,
  padding: "4px 4px",
  borderRadius: 4,
  cursor: "pointer",
  userSelect: "none",
};

const checkboxStyle: React.CSSProperties = {
  accentColor: "#60a5fa",
  flexShrink: 0,
  cursor: "pointer",
};

const labelTextStyle: React.CSSProperties = {
  fontSize: 11,
  flex: 1,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  fontFamily: "monospace",
};

const sizeStyle: React.CSSProperties = {
  fontSize: 9,
  color: "#334155",
  flexShrink: 0,
  whiteSpace: "nowrap",
};
