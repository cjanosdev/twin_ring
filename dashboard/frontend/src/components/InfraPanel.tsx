import { useState } from "react";
import { useInfraPoller, apiInfraInit, apiInfraInitClean, apiInfraUp, apiInfraDown } from "../api";

export function InfraPanel() {
  const status = useInfraPoller(3000);
  const [error, setError] = useState<string | null>(null);
  const [acting, setActing] = useState(false);

  const busy = acting || (status?.busy ?? false);

  const run = async (fn: () => Promise<void>) => {
    setError(null);
    setActing(true);
    try {
      await fn();
    } catch (e) {
      setError(String(e));
    } finally {
      setActing(false);
    }
  };

  const handleInitClean = () => {
    if (!window.confirm("This wipes all preloaded data and re-initializes from scratch. Continue?")) {
      return;
    }
    void run(apiInfraInitClean);
  };

  const stackUp = (status?.cacheNodes.up ?? 0) === (status?.cacheNodes.total ?? 3) && status?.cassandra === "healthy";
  const stackDown = status?.cassandra === "absent" && (status?.cacheNodes.up ?? 0) === 0;

  return (
    <div style={containerStyle}>
      <div style={titleStyle}>INFRASTRUCTURE</div>

      <div style={{ marginBottom: 8 }}>
        <StatusRow label="Cassandra" ok={status?.cassandra === "healthy"} text={status?.cassandra ?? "…"} />
        <StatusRow
          label="Data volume"
          ok={status?.volumePresent ?? false}
          text={status?.volumePresent ? "preloaded" : "not initialized"}
        />
        <StatusRow
          label="Cache nodes"
          ok={(status?.cacheNodes.up ?? 0) === (status?.cacheNodes.total ?? 3)}
          text={status ? `${status.cacheNodes.up}/${status.cacheNodes.total} up` : "…"}
        />
        <StatusRow label="Control API" ok={status?.controlApi ?? false} text={status?.controlApi ? "up" : "down"} />
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        <button
          onClick={() => void run(apiInfraInit)}
          disabled={busy || (status?.volumePresent ?? false)}
          style={btnStyle("#334155", busy || (status?.volumePresent ?? false))}
          title="Creates the data volume and preloads 100k keys. No-op if already initialized."
        >
          Initialize data
        </button>
        <button
          onClick={handleInitClean}
          disabled={busy}
          style={btnStyle("#7c2d12", busy)}
          title="Wipes the volume and re-initializes from scratch."
        >
          Re-initialize (wipe data)
        </button>
        <button
          onClick={() => void run(apiInfraUp)}
          disabled={busy || stackUp}
          style={btnStyle("#16a34a", busy || stackUp)}
        >
          Start stack
        </button>
        <button
          onClick={() => void run(apiInfraDown)}
          disabled={busy || stackDown}
          style={btnStyle("#dc2626", busy || stackDown)}
        >
          Stop stack
        </button>
      </div>

      {error && (
        <div style={{ color: "#ef4444", fontSize: 11, marginTop: 8, wordBreak: "break-word" }}>
          {error}
        </div>
      )}

      {status?.busy && (
        <div style={{ fontSize: 10, color: "#60a5fa", marginTop: 8 }}>Working…</div>
      )}

      {status && status.log.length > 0 && (
        <div style={logBoxStyle}>
          {status.log.slice(-40).map((line, i) => (
            <div key={i} style={{ whiteSpace: "pre-wrap", wordBreak: "break-word" }}>{line}</div>
          ))}
        </div>
      )}
    </div>
  );
}

function StatusRow({ label, ok, text }: { label: string; ok: boolean; text: string }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
      <div style={{ width: 6, height: 6, borderRadius: "50%", background: ok ? "#4ade80" : "#64748b", flexShrink: 0 }} />
      <span style={{ fontSize: 10, color: "#94a3b8", minWidth: 70 }}>{label}</span>
      <span style={{ fontSize: 10, color: ok ? "#4ade80" : "#94a3b8" }}>{text}</span>
    </div>
  );
}

const containerStyle: React.CSSProperties = {
  padding: "12px 10px",
  borderBottom: "1px solid #1e293b",
};

const titleStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 700,
  color: "#64748b",
  letterSpacing: 1,
  marginBottom: 10,
};

const btnStyle = (bg: string, disabled: boolean): React.CSSProperties => ({
  background: disabled ? "#1e293b" : bg,
  color: disabled ? "#64748b" : "#fff",
  border: "none",
  borderRadius: 6,
  padding: "6px 10px",
  fontSize: 11,
  fontWeight: 600,
  cursor: disabled ? "not-allowed" : "pointer",
  width: "100%",
});

const logBoxStyle: React.CSSProperties = {
  marginTop: 8,
  maxHeight: 120,
  overflowY: "auto",
  background: "#0f172a",
  border: "1px solid #1e293b",
  borderRadius: 4,
  padding: 6,
  fontSize: 9,
  color: "#64748b",
  fontFamily: "monospace",
};
