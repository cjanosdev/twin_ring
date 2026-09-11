//! Metastable failure experiment — runs against any cache strategy.
//!
//! Replaces the former per-strategy wrapper binaries (`exp_lru`, `exp_dual`, …),
//! which were identical except for a hardcoded string. The protocol is the same
//! for every strategy by design: the only thing that differs between runs is
//! which cache backend the *nodes* are running. Keeping one binary means the
//! comparison can never drift because someone edited one wrapper and not another.
//!
//! # Usage
//!
//!   # Nodes must already be running the matching strategy:
//!   CACHE_STRATEGY=dual-ring docker compose -f docker/docker-compose-baseline.yml \
//!     up -d cache_node_1 cache_node_2 cache_node_3
//!
//!   cargo run -p twin_ring_exp --bin exp -- --strategy dual-ring
//!
//! The run aborts before measuring if the nodes report a different strategy —
//! see `experiment::cluster::verify_node_strategies`.

use anyhow::Result;
use clap::{Parser, ValueEnum};
use twin_ring_exp::experiment::{run_experiment_with_scenario, scenario::Scenario};

/// Cache strategies the node can be started with.
///
/// Mirrors the `match` on `CACHE_STRATEGY` in `twin_ring_node/src/main.rs`.
/// The string forms below are what the node reports from `GET /strategy`, so
/// they must stay in sync with that match.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum Strategy {
    /// Control: TTL + LRU eviction
    Lru,
    /// Proactive hot-key replication to backup nodes
    #[value(name = "dual-ring")]
    DualRing,
    /// Hot / warm / cold tiers with different TTLs
    #[value(name = "ttl-tiered")]
    TtlTiered,
    /// Per-key fill leases (one DB fetch per key, rest wait)
    Leased,
    /// Dual-ring + ttl-tiered + leased together
    Combined,
}

impl Strategy {
    fn as_str(self) -> &'static str {
        match self {
            Strategy::Lru => "lru",
            Strategy::DualRing => "dual-ring",
            Strategy::TtlTiered => "ttl-tiered",
            Strategy::Leased => "leased",
            Strategy::Combined => "combined",
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    about = "Metastable failure experiment: warmup → regular work → overload or node outage → recovery observation",
    long_about = None,
)]
struct Args {
    /// Cache strategy to measure. The running nodes must already be started with
    /// this strategy — the experiment verifies it and aborts on a mismatch.
    #[arg(long)]
    strategy: Strategy,
    /// Disruption to test. Defaults to TR_SCENARIO, or overload when unset.
    #[arg(long, value_enum)]
    scenario: Option<Scenario>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    run_experiment_with_scenario(args.strategy.as_str(), args.scenario).await
}
