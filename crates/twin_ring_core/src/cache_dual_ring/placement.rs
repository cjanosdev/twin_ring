//! Deterministic consistent-hash placement for the level-1 and level-2 rings.
//!
//! Every cache node and workload client builds this value from the same ordered
//! set of member IDs. The resulting placement is therefore a small protocol,
//! rather than a process-local implementation detail.

/// Index of one cache server in the client-visible cluster list.
pub type ServerIndex = usize;

const VIRTUAL_NODES_PER_SERVER: u32 = 128;

/// The two locations a client needs for one key.
///
/// `primary` is the normal level-1 cache location. `backup` is the level-2
/// replica location that a client tries only when it cannot reach `primary`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyPlacement {
    pub primary: ServerIndex,
    pub backup: ServerIndex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RingPoint {
    hash: u64,
    owner: ServerIndex,
}

/// One immutable consistent-hash ring.
///
/// A server appears at many virtual points, which gives a small cluster a more
/// even key distribution than placing each server at just one point.
#[derive(Clone, Debug)]
struct HashRing {
    points: Vec<RingPoint>,
}

impl HashRing {
    fn new(ring_seed: u8, members: &[ServerIndex]) -> Self {
        let mut points = Vec::with_capacity(members.len() * VIRTUAL_NODES_PER_SERVER as usize);
        for &member in members {
            for virtual_node in 0..VIRTUAL_NODES_PER_SERVER {
                points.push(RingPoint {
                    hash: point_hash(ring_seed, member, virtual_node),
                    owner: member,
                });
            }
        }
        points.sort_unstable_by_key(|point| (point.hash, point.owner));
        Self { points }
    }

    fn owner_for_hash(&self, hash: u64) -> ServerIndex {
        let index = self.points.partition_point(|point| point.hash < hash);
        self.points[index % self.points.len()].owner
    }

    /// Find the first distinct server encountered clockwise from `hash`.
    fn first_owner_other_than(&self, hash: u64, excluded: ServerIndex) -> ServerIndex {
        let start = self.points.partition_point(|point| point.hash < hash) % self.points.len();
        for offset in 0..self.points.len() {
            let owner = self.points[(start + offset) % self.points.len()].owner;
            if owner != excluded {
                return owner;
            }
        }
        unreachable!("a dual ring always has at least two distinct members")
    }
}

/// Two independent consistent-hash rings over one member set.
///
/// The L1 ring locates the normal owner. The L2 ring independently chooses a
/// replica destination, then skips the L1 owner if it happens to select it.
/// Keeping `DualRing` immutable means a request sees one coherent membership
/// snapshot. To reconfigure the experiment, construct and distribute a new
/// ring from the new member list.
#[derive(Clone, Debug)]
pub struct DualRing {
    members: Vec<ServerIndex>,
    l1: HashRing,
    l2: HashRing,
}

impl DualRing {
    /// Build both rings from distinct, zero-based server IDs.
    ///
    /// At least two servers are required because a replica cannot share the
    /// primary's machine. Sorting makes `[2, 0, 1]` describe the same cluster
    /// as `[0, 1, 2]`.
    pub fn new<I>(members: I) -> Option<Self>
    where
        I: IntoIterator<Item = ServerIndex>,
    {
        let mut members: Vec<_> = members.into_iter().collect();
        members.sort_unstable();
        members.dedup();
        if members.len() < 2 {
            return None;
        }

        Some(Self {
            l1: HashRing::new(0, &members),
            l2: HashRing::new(1, &members),
            members,
        })
    }

    /// Return the member IDs used to construct this ring, in sorted order.
    pub fn members(&self) -> &[ServerIndex] {
        &self.members
    }

    /// Calculate the L1 primary and distinct L2 backup for one key.
    pub fn placement_for(&self, key: &str) -> KeyPlacement {
        let primary = self.l1.owner_for_hash(key_hash(0, key));
        let backup = self.l2.first_owner_other_than(key_hash(1, key), primary);
        KeyPlacement { primary, backup }
    }
}

/// Stable FNV-1a hash of a key for one ring.
fn key_hash(ring_seed: u8, key: &str) -> u64 {
    stable_hash(&[b"key", &[ring_seed], key.as_bytes()])
}

/// Stable FNV-1a hash of a virtual point for one ring.
fn point_hash(ring_seed: u8, member: ServerIndex, virtual_node: u32) -> u64 {
    stable_hash(&[
        b"point",
        &[ring_seed],
        &(member as u64).to_le_bytes(),
        &virtual_node.to_le_bytes(),
    ])
}

/// A tiny stable hash for experimental placement.
///
/// `DefaultHasher` is fine for maps inside one process, but its algorithm is
/// not a protocol that cache nodes and clients can safely share. Separators make
/// the byte sequences unambiguous (for example, `("ab", "c")` versus
/// `("a", "bc")`).
fn stable_hash(parts: &[&[u8]]) -> u64 {
    const OFFSET_BASIS: u64 = 14_695_981_039_346_656_037;
    const PRIME: u64 = 1_099_511_628_211;

    let mut hash = OFFSET_BASIS;
    for part in parts {
        for byte in *part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
