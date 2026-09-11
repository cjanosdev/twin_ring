# How to test TwinRing

Run commands from the repository root.

## Fast, focused tests

Run one strategy's pure in-memory tests while rebuilding it:

```sh
cargo test -p twin_ring_core cache_lru --offline --locked
cargo test -p twin_ring_core cache_ttl_tiered --offline --locked
cargo test -p twin_ring_core cache_leased --offline --locked
cargo test -p twin_ring_core cache_dual_ring --offline --locked
cargo test -p twin_ring_core cache_combined --offline --locked
```

These do not start Docker or Cassandra. They are the first check for cache rules such as expiry, eviction, lease ownership, replica placement, and snapshot ordering.

## Whole Rust workspace

```sh
cargo test --workspace --offline --locked
```

This compiles the cache library, cache node, experiment program, control API, and their tests together. Use it before treating a change as ready.

## Formatting and diff checks

```sh
cargo fmt --check
git diff --check
```

The repository has some older formatting drift. If `cargo fmt --check` reports unrelated files, format only the files you changed with `rustfmt --edition 2021 path/to/file.rs`.

## Docker configuration

```sh
docker compose -f docker/docker-compose-baseline.yml config --quiet
```

This validates the Compose file without starting containers.

## Runtime smoke test

Rebuild and recreate the node and control images after code or Compose changes:

```sh
docker compose -f docker/docker-compose-baseline.yml up -d --build
```

Then confirm each node reports the intended strategy:

```sh
curl http://localhost:8001/strategy
curl http://localhost:8002/strategy
curl http://localhost:8003/strategy
```

For dual-ring or combined, also inspect replication after warm traffic:

```sh
curl http://localhost:8001/replicate-stats
curl http://localhost:8002/replicate-stats
curl http://localhost:8003/replicate-stats
```

The full experiment is the final test. It intentionally kills and restarts a cache container, so do not run it against anything important.
