use chrono::Utc;
use std::path::PathBuf;

/// Returns a workspace-root-relative path like:
/// twin_ring/experiment_results/runs/2026-02-02/<test_name>_HHMMSS.csv
pub fn results_path(test_name: &str) -> anyhow::Result<PathBuf> {
    let date = Utc::now().format("%Y-%m-%d").to_string();
    let time = Utc::now().format("%H%M%S").to_string();

    // CARGO_MANIFEST_DIR = crates/twin_ring_core
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .and_then(|p| p.parent()) // workspace root
        .expect("failed to find workspace root")
        .to_path_buf();

    let dir = workspace_root
        .join("experiment_results")
        .join("runs")
        .join(&date);

    std::fs::create_dir_all(&dir)?;

    Ok(dir.join(format!("{test_name}_{time}.csv")))
}
