//! Talking to the cluster: preflight checks, fault injection, and introspection.
//!
//! Node kill/start goes through the control API container, which drives Docker.
//! These calls use a longer-timeout client than the load generator — a container
//! start takes seconds, far beyond the sub-second request timeout.

use anyhow::Result;
use reqwest::Client;
use twin_ring_core::{DualRing, ReplicateEntry, ServerIndex};

use super::topology::CONTROL_API;

/// Stop a node's container. Used to inject the fault.
pub async fn node_kill(node_id: &str, client: &Client) -> Result<()> {
    let url = format!("{CONTROL_API}/kill/{node_id}");
    match client.post(&url).send().await {
        Ok(resp) if resp.status().is_success() => {
            println!("  ✓ Killed node {node_id}");
            Ok(())
        }
        Ok(resp) => anyhow::bail!(
            "kill for node {node_id} returned {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        ),
        Err(error) => Err(error.into()),
    }
}

/// Restart a node's container, retrying a few times.
///
/// The node comes back with an empty cache, which is the point: a cold node
/// rejoining a saturated cluster is what sustains the metastable loop.
pub async fn node_start(node_id: &str, client: &Client) -> Result<()> {
    let url = format!("{CONTROL_API}/start/{node_id}");
    let mut last_error = None;
    for attempt in 1..=3 {
        match client.post(&url).send().await {
            Ok(response) if response.status().is_success() => {
                println!("  ✓ Started node {node_id}");
                return Ok(());
            }
            Ok(response) => {
                last_error = Some(format!(
                    "start returned {}: {}",
                    response.status(),
                    response.text().await.unwrap_or_default()
                ));
            }
            Err(error) => last_error = Some(error.to_string()),
        }
        if attempt < 3 {
            println!("  ✗ Start attempt {attempt}/3 failed; retrying...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }
    anyhow::bail!(
        "could not start node {node_id} after 3 attempts: {}",
        last_error.unwrap_or_else(|| "unknown error".to_string())
    )
}

/// Install one membership view on every server that remains in that view.
///
/// We deliberately update the node processes first. Only once all acknowledge
/// the new ring should the caller switch request routing to it.
pub async fn replace_ring_members(
    nodes: &[String],
    members: &[usize],
    client: &Client,
) -> Result<()> {
    for &member in members {
        let url = nodes
            .get(member)
            .ok_or_else(|| anyhow::anyhow!("ring member {member} has no URL in topology::NODES"))?;
        let response = client
            .post(format!("{url}/ring-members"))
            .json(&serde_json::json!({ "members": members }))
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("could not reconfigure {url}: {error}"))?;
        if !response.status().is_success() {
            anyhow::bail!(
                "{url} rejected membership {:?}: {}",
                members,
                response.text().await.unwrap_or_default()
            );
        }
    }
    println!(
        "  ✓ Installed ring membership {:?} on healthy nodes",
        members
    );
    Ok(())
}

/// Copy replicas of `failed_member` into the new primary selected for each key.
/// Each source snapshot is read from its healthy L2 backup before request
/// routing changes, making the transition deterministic rather than a race.
pub async fn hand_off_failed_replicas(
    nodes: &[String],
    failed_member: ServerIndex,
    healthy_members: &[ServerIndex],
    replacement_ring: &DualRing,
    client: &Client,
) -> Result<usize> {
    let mut moved = 0;
    for &source in healthy_members {
        let source_url = nodes.get(source).ok_or_else(|| {
            anyhow::anyhow!("healthy member {source} has no URL in topology::NODES")
        })?;
        let response = client
            .get(format!("{source_url}/replicas/{failed_member}"))
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("could not read replicas from {source_url}: {error}"))?
            .error_for_status()?;
        let replicas: Vec<ReplicateEntry> = response.json().await?;

        for replica in replicas {
            let destination = replacement_ring.placement_for(&replica.key).primary;
            let destination_url = nodes.get(destination).ok_or_else(|| {
                anyhow::anyhow!("new primary {destination} has no URL in topology::NODES")
            })?;
            client
                .post(format!("{destination_url}/put/{}", replica.key))
                .body(replica.value)
                .send()
                .await
                .map_err(|error| {
                    anyhow::anyhow!("could not prefill {destination_url} during handoff: {error}")
                })?
                .error_for_status()?;
            moved += 1;
        }
    }
    Ok(moved)
}

/// Wait for a restarted cache node to answer HTTP before reconfiguring it.
pub async fn wait_for_node(url: &str, client: &Client) -> Result<()> {
    for _ in 0..30 {
        if client.get(format!("{url}/strategy")).send().await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    anyhow::bail!("restarted node {url} did not become ready within 30 seconds")
}

/// Verify every node is actually running the strategy we were asked to measure.
///
/// The strategy string passed to `run_experiment` only *labels* output — it names
/// the CSV and fills the summary JSON. The cache backend is chosen independently,
/// by each node reading `CACHE_STRATEGY` at startup. Nothing else cross-checks the
/// two, so without this preflight, running `--strategy dual-ring` against nodes
/// started as `lru` silently produces LRU results filed under "dual-ring".
///
/// Aborts the run rather than measuring something we cannot label honestly.
pub async fn verify_node_strategies(
    nodes: &[String],
    expected: &str,
    client: &Client,
) -> Result<()> {
    println!("🔎 Preflight — verifying all nodes are running '{expected}'...");

    for url in nodes {
        let resp = client
            .get(format!("{url}/strategy"))
            .send()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "node {url} is unreachable ({e}).\n   Is the cluster up? \
                 Try: docker compose -f docker/docker-compose-baseline.yml up -d"
                )
            })?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!(
                "node {url} has no /strategy endpoint — it is running an older build.\n   \
                 Rebuild the node image: docker compose -f docker/docker-compose-baseline.yml up -d --build"
            );
        }

        let body: serde_json::Value = resp.json().await?;
        let actual = body["strategy"].as_str().unwrap_or("<missing>");

        if actual != expected {
            anyhow::bail!(
                "strategy mismatch — refusing to run.\n   \
                 requested: {expected}\n   \
                 node {url} is running: {actual}\n\n   \
                 Restart the cache nodes with the matching strategy:\n   \
                 CACHE_STRATEGY={expected} docker compose -f docker/docker-compose-baseline.yml \
                 up -d cache_node_1 cache_node_2 cache_node_3"
            );
        }
        println!("   ✓ {url} → {actual}");
    }

    Ok(())
}

/// Cassandra's memory use as a percentage of its container limit.
/// The fault phase ends early once this crosses the configured threshold.
pub async fn get_cassandra_mem_pct(client: &Client) -> Result<f64> {
    let resp = client
        .get(format!("{CONTROL_API}/cassandra-mem"))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    Ok(resp["mem_pct"].as_f64().unwrap_or(0.0))
}

/// Print each node's hot-store contents, for strategies that replicate.
///
/// Only dual-ring and combined serve `/replicate-stats`; other strategies 404 and
/// are skipped silently.
///
/// NOTE: this currently prints nothing even for dual-ring, because the node's
/// `/replicate-stats` handler has an actix `web::Data` type mismatch and returns
/// 500. Fixing that handler is what makes this diagnostic useful.
pub async fn print_replicate_stats(nodes: &[String], client: &Client) {
    let mut any = false;
    for url in nodes {
        let Ok(resp) = client.get(format!("{url}/replicate-stats")).send().await else {
            continue;
        };
        if !resp.status().is_success() {
            continue;
        }
        let Ok(v) = resp.json::<serde_json::Value>().await else {
            continue;
        };
        if !any {
            println!("   🔁 Replication stats:");
            any = true;
        }
        let total = v["hot_store_total"].as_u64().unwrap_or(0);
        let by_node = &v["hot_store_by_node"];
        let top5: Vec<String> = v["top5_main_hot_keys"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|e| {
                let k = e.get(0)?.as_str()?;
                let c = e.get(1)?.as_u64()?;
                Some(format!("{k}({c})"))
            })
            .collect();
        println!(
            "      {url}: hot_store={total}  by_node={by_node}  top5=[{}]",
            top5.join(", ")
        );
    }
}
