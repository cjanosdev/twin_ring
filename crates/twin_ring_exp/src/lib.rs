/// Shared experiment infrastructure.
///
/// All experiment binaries in this crate can import from here:
///
///   use twin_ring_exp::metrics::{MetricsWriter, NodeWindow, StatsPoller, run_phase};
///
pub mod metrics;
