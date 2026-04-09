import { useEffect, useRef } from "react";
import { ExperimentStatus } from "../types";

interface Props {
  status: ExperimentStatus | null;
}

export function LogPanel({ status }: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [status?.stdout_tail]);

  const lines = status?.stdout_tail ?? [];

  return (
    <div style={containerStyle}>
      <div style={titleStyle}>EXPERIMENT LOG</div>
      <div ref={scrollRef} style={scrollStyle}>
        {lines.length === 0 ? (
          <span style={{ color: "#475569", fontSize: 11 }}>
            No output yet. Run an experiment to see logs here.
          </span>
        ) : (
          lines.map((line, i) => (
            <div key={i} style={lineStyle(line)}>
              {line || " "}
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function lineStyle(line: string): React.CSSProperties {
  let color = "#94a3b8";
  if (line.includes("METASTABLE FAILURE")) color = "#ef4444";
  else if (line.includes("recovered") || line.includes("✅")) color = "#4ade80";
  else if (line.includes("💥") || line.includes("fault")) color = "#fb923c";
  else if (line.includes("[stderr]")) color = "#f87171";
  else if (line.includes("Metrics →") || line.includes("📊")) color = "#60a5fa";
  return {
    color,
    fontSize: 10,
    fontFamily: "monospace",
    whiteSpace: "pre-wrap",
    wordBreak: "break-all",
    lineHeight: 1.5,
  };
}

const containerStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  height: "100%",
  padding: "10px",
};

const titleStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  color: "#64748b",
  letterSpacing: 1,
  marginBottom: 8,
  flexShrink: 0,
};

const scrollStyle: React.CSSProperties = {
  flex: 1,
  overflowY: "auto",
  background: "#0f172a",
  borderRadius: 6,
  padding: "8px 10px",
  border: "1px solid #1e293b",
};
