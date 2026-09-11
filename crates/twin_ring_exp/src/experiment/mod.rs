//! One strategy and one disruption per run, sharing a regular baseline and recovery assessment.
//! LRU is the control; scenarios are database overload and a cache-node outage.
//!
//! Start with [`protocol::run_experiment`] to follow the sequence.

pub mod cluster;
mod outage;
pub mod output;
pub mod protocol;
pub mod recovery;
pub mod scenario;
pub mod settings;
pub mod stats;
pub mod topology;
pub mod workload;

pub use protocol::{run_experiment, run_experiment_with_scenario};
