# Leased cache artifact

Source: `crates/twin_ring_core/src/cache_leased/mod.rs`.

## `LeaseLookup`

```rust
pub enum LeaseLookup {
    Hit(String),
    LeaseGranted(Lease),
    LeaseHeld,
}
```

**What:** the result of one lookup. `Hit` owns the cached value. `LeaseGranted` gives this request a capability to fill a miss. `LeaseHeld` says another request already owns that capability.

**Why:** the node must distinguish “read Cassandra” from “do not create a stampede.” `Option<String>` cannot express that difference.

## `Lease`

```rust
pub struct Lease {
    key: String,
    token: u64,
}
```

**What:** the private key and unique generation number returned only to the lease holder.

**Why:** a key alone is unsafe. After a lease expires, a second request may acquire a replacement; the old request must not be allowed to fill or release the new request’s lease.

## `ActiveLease`

```rust
struct ActiveLease {
    token: u64,
    expires_at: Instant,
}
```

**What:** the registry’s record for the currently valid token and its deadline.

**Why:** it is separate from `Lease` because the registry needs expiry state while the holder needs only its identity token.

## `LeaseRegistry`

```rust
struct LeaseRegistry {
    leases: HashMap<String, ActiveLease>,
    next_token: u64,
    lease_duration: Duration,
}
```

**What:** local per-key coordination state. `leases` maps one key to at most one active fill. `next_token` generates a new identity. `lease_duration` prevents a crashed filler from blocking recovery forever.

**Why:** consistent hashing means requests for one key reach one cache node, so this local map is sufficient; there is no distributed lock protocol.

## `LeaseRegistry::new`

```rust
fn new(lease_duration: Duration) -> Self {
    Self {
        active: HashMap::new(),
        lease_duration,
        next_id: 0,
    }
}
```

**What:** constructs an empty lease table and starts the ID sequence at zero.

**Why:** `lease_duration` is configuration, while `next_id` makes every later lease distinguishable.

## `LeaseRegistry::acquire`

```rust
fn acquire(&mut self, key: &str, now: Instant) -> LeaseAcquisition {
    if let Some(existing) = self.active.get(key) {
        if existing.expires_at > now {
            return LeaseAcquisition::Held;
        }
    }

    self.next_id = self.next_id.checked_add(1).expect("lease ID overflow");
    let lease = Lease { key: key.to_owned(), id: self.next_id };
    self.active.insert(
        lease.key.clone(),
        ActiveLease { id: lease.id, expires_at: now + self.lease_duration },
    );
    LeaseAcquisition::Granted(lease)
}
```

**What:** looks for a current lease. A live lease returns `Held`; a missing or expired lease is replaced with a new token and deadline.

**Why:** callers never wait while a mutex is locked. The first miss can query Cassandra; simultaneous misses receive an immediate “someone else is filling” result.

## `LeaseRegistry::release`

```rust
fn release(&mut self, lease: &Lease) -> bool {
    let Some(active) = self.active.get(&lease.key) else { return false; };
    if active.id != lease.id { return false; }
    self.active.remove(&lease.key);
    true
}
```

**What:** removes a lease only when its key and token match the active record.

**Why:** an old request must not release a newer request’s replacement lease.

## `LeaseRegistry::is_current`

```rust
fn is_current(&self, lease: &Lease, now: Instant) -> bool {
    self.active
        .get(&lease.key)
        .is_some_and(|active| active.id == lease.id && active.expires_at > now)
}
```

**What:** checks the key, token, and expiry without mutating the registry.

**Why:** `complete_fill` uses this before writing the database result into cache, preventing a late result from an expired lease from overwriting a newer fill.

## Exact code, before commentary

Read [cache_leased/mod.rs](../crates/twin_ring_core/src/cache_leased/mod.rs) and [leased tests](../crates/twin_ring_core/src/cache_leased/leased_tests.rs) beside this artifact. Read in this order: `LeaseLookup`, `Lease`, `ActiveLease`, `LeaseRegistry`, `LeasedInner`, `LeasedState`, then `LeasedCache`.

The leased strategy prevents a same-key cache stampede. Its lookup result is `Hit`, `LeaseGranted`, or `LeaseHeld`.

The cache stores normal entries plus a per-key lease registry. A lease has a unique token and expiry. Only the current token may call `complete_fill` or `abandon_fill`; an old requester cannot overwrite a newer retry after lease expiry. Different keys have independent leases.

The registry is local because consistent hashing gives one cache server responsibility for a key. A mutex protects the entry and lease maps; the Cassandra request happens after the lock is released.

`LeaseRegistry::acquire` first removes an expired lease, then either returns a new token or reports that the live token is held. `is_current` prevents an old request from completing after expiry. `release` removes only a matching token. `LeasedState::lookup_or_acquire` checks the cache before the registry; a cached value always wins. `complete_fill` verifies the token, inserts into LRU+TTL storage, then clears the lease. `abandon_fill` clears only its own current lease.
