//! Where the cluster lives and how keys route to it.
//!
//! The shared dual-ring placement function chooses an L1 primary and a distinct
//! L2 backup for every key. The generator draws from the whole keyspace; the key
//! determines both locations.

/// Base URLs of the three cache nodes, indexed by shared zero-based server index.
pub const NODES: &[&str] = &[
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
];

/// The Docker control API calls this container `1`, which is the server at
/// zero-based position 0 in `NODES`. Keeping both names together prevents the
/// experiment from silently killing a different server than it reconfigures.
pub const FAULTED_SERVER_INDEX: usize = 0;
pub const FAULTED_CONTAINER_ID: &str = "1";

/// Control API for killing/restarting node containers and reading Cassandra memory.
pub const CONTROL_API: &str = "http://localhost:9000";
