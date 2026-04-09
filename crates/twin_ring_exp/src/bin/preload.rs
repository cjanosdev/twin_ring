use scylla::{Session, SessionBuilder};
use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;

const CONCURRENCY: usize = 64;

#[tokio::main]
async fn main() -> Result<()> {
    let total: usize = std::env::var("PRELOAD_KEYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);

    let session: Arc<Session> = Arc::new(
        SessionBuilder::new()
            .known_node("cassandra:9042")
            .build()
            .await?,
    );

    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS kvstore WITH replication = \
             {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[],
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS kvstore.kv (key text PRIMARY KEY, value text)",
            &[],
        )
        .await?;

    let prepared = Arc::new(
        session
            .prepare("INSERT INTO kvstore.kv (key, value) VALUES (?, ?)")
            .await?,
    );

    let mut in_flight = FuturesUnordered::new();

    for i in 0..total {
        let session = Arc::clone(&session);
        let prepared = Arc::clone(&prepared);
        in_flight.push(async move {
            let key = format!("key{}", i);
            let val = format!("val{}", i);
            session.execute(&prepared, (key, val)).await
        });

        // Drain one completed future whenever we hit the concurrency cap
        if in_flight.len() >= CONCURRENCY {
            in_flight.next().await.unwrap()?;
        }
    }

    // Drain remaining
    while let Some(result) = in_flight.next().await {
        result?;
    }

    println!("✅ Preloaded {} keys into Cassandra", total);
    Ok(())
}
