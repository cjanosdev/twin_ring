use chrono::Local;
use std::path::PathBuf;

fn workspace_runs_dir(date: &str) -> anyhow::Result<PathBuf> {
    // CARGO_MANIFEST_DIR = crates/twin_ring_core
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .and_then(|p| p.parent()) // workspace root
        .expect("failed to find workspace root")
        .to_path_buf();

    let dir = workspace_root
        .join("experiment_results")
        .join("runs")
        .join(date);

    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Returns a path like:
/// experiment_results/runs/2026-02-02/<test_name>_HHMMSS.csv
pub fn results_path(test_name: &str) -> anyhow::Result<PathBuf> {
    let now = Local::now();
    let date = now.format("%Y-%m-%d").to_string();
    let time = now.format("%H%M%S").to_string();
    let dir = workspace_runs_dir(&date)?;
    Ok(dir.join(format!("{test_name}_{time}.csv")))
}

/// Returns a path like:
/// experiment_results/runs/2026-02-02/<strategy>_<kind>_HHMMSS.csv
///
/// Example: `results_path_prefix("lru", "metastable")` →
///   `.../runs/2026-05-18/lru_metastable_143022.csv`
pub fn results_path_prefix(strategy: &str, kind: &str) -> anyhow::Result<PathBuf> {
    let now = Local::now();
    let date = now.format("%Y-%m-%d").to_string();
    let time = now.format("%H%M%S").to_string();
    let dir = workspace_runs_dir(&date)?;
    Ok(dir.join(format!("{strategy}_{kind}_{time}.csv")))
}
