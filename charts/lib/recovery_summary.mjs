// Display the runner's recorded decision; do not derive a second verdict from CSV.
export function recoverySummary(summary) {
  const seconds = summary?.time_to_recovery_secs;
  const time = Number.isFinite(seconds) && seconds >= 0 ? seconds : null;
  const labels = {
    resisted_degradation: "No degradation observed",
    recovered: "Recovered",
    recovered_then_relapsed: "Recovered, then relapsed",
    did_not_recover_within_observation: "Not recovered within observation",
    inconclusive: "Inconclusive",
  };
  const status = summary?.outcome
    ? labels[summary.outcome] ?? "Unknown outcome"
    : summary ? "Legacy recovery criteria" : "No summary";
  // Prevention has no recovery event. Never turn a null or old -1 sentinel
  // into an invented duration, a zero-second recovery, or a claim of 'never'.
  return {
    seconds: summary?.outcome === "resisted_degradation" ? null : time,
    status,
    relapses: summary?.relapse_times_ms?.length ?? 0,
  };
}
