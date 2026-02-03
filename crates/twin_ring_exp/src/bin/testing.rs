use anyhow::Result;
use chrono::Utc;
use futures::future::join_all;
use hdrhistogram::Histogram;
use rand::{prelude::IndexedRandom, rngs::StdRng, Rng, SeedableRng};
use reqwest::Client;
use std::{sync::Arc};
use tokio::sync::Barrier;
use tokio::time::Instant;
use twin_ring_core::experiment_path::results_path;


#[derive(Debug)]
struct ClientResult {
    client_id: usize,
    node: String,
    key: String,
    client_latency_ms: u64,
    finished_at: chrono::DateTime<chrono::Utc>, // 👈 NEW
    ok: bool,
    status: Option<u16>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
     let client = Client::builder()
        .pool_max_idle_per_host(100)
        .build()?;

    let nodes: Vec<String> = vec![
        "http://localhost:8001".to_string(),
        "http://localhost:8002".to_string(),
        "http://localhost:8003".to_string(),
    ];

    let n_clients = 100usize;
    let barrier = Arc::new(Barrier::new(n_clients));

    let tasks = (0..n_clients).map(|client_id| {
        let barrier = barrier.clone();
        let client = client.clone();
        let nodes = nodes.clone();

        tokio::spawn(async move {
            // Pick a random node for this "client"
            // Deterministic per-client RNG (nice for repeatable tests)
            let mut rng = StdRng::seed_from_u64(client_id as u64);

            // Start together
            barrier.wait().await;

            let node: String = nodes.choose(&mut rng).unwrap().clone();
            let key = format!("key{}", rng.random_range(0..100_000));

            let get_url = format!("{}/get/{}", node, key);
            println!("GET {}", get_url);

            // Measure client-side latency (start -> full response)
            let start = Instant::now();
            let resp = client.get(&get_url).send().await;

            match resp {
                Ok(r) => {
                    let status = r.status().as_u16();

                    // IMPORTANT: read the body before marking finished
                    let _body = r.bytes().await;

                    let elapsed = start.elapsed();
                    let finished_at = chrono::Utc::now(); // 👈 RIGHT HERE

                    Ok::<_, anyhow::Error>(ClientResult {
                        client_id,
                        node,
                        key,
                        client_latency_ms: elapsed.as_millis() as u64,
                        finished_at,
                        ok: true,
                        status: Some(status),
                    })
                }
                Err(_) => {
                    let elapsed = start.elapsed();
                    let finished_at = chrono::Utc::now(); // 👈 ALSO HERE (failure still finished)

                    Ok::<_, anyhow::Error>(ClientResult {
                        client_id,
                        node,
                        key,
                        client_latency_ms: elapsed.as_millis() as u64,
                        finished_at,
                        ok: false,
                        status: None,
                    }) // end Ok(ClientResult)
                } // end Err(e
            } // end match resp
        }) // end tokio::spawn
    }); // end main


 // Collect results
    let joined = join_all(tasks).await;
    let mut results = Vec::with_capacity(n_clients);

    for j in joined {
        match j {
            Ok(Ok(r)) => results.push(r),
            Ok(Err(e)) => eprintln!("request error: {e:#}"),
            Err(e) => eprintln!("task join error: {e}"),
        }
    }

    // Basic metrics summary
    let mut hist = Histogram::<u64>::new(3)?; // 3 sig figs
    let mut by_node = std::collections::BTreeMap::<String, usize>::new();

     for r in &results {
        if r.ok {
            hist.record(r.client_latency_ms)?;
        }
        *by_node.entry(r.node.clone()).or_default() += 1;
    }

    println!("Requests completed: {}", results.len());
    println!("Node distribution:");
    for (node, count) in by_node {
        println!("  {node}: {count}");
    }

    println!("Client latency (ms):");
    println!("  p50: {}", hist.value_at_quantile(0.50));
    println!("  p90: {}", hist.value_at_quantile(0.90));
    println!("  p99: {}", hist.value_at_quantile(0.99));
    println!("  max: {}", hist.max());

    // ✅ Write CSV for Plotly grouping by node
   let out_path = results_path("loadtest")?; // or "read_100c_3n" etc.
println!("Writing results to {}", out_path.display());

let run_id = Utc::now().format("%Y%m%d_%H%M%S").to_string();

let mut wtr = csv::Writer::from_path(&out_path)?;
wtr.write_record([
    "run_id",
    "client_id",
    "node",
    "key",
    "latency_ms",
    "finished_at",
    "ok",
    "status",
])?;

for r in &results {
    wtr.write_record(&[
        run_id.clone(),
        r.client_id.to_string(),
        r.node.clone(),
        r.key.clone(),
        r.client_latency_ms.to_string(),
        r.finished_at.to_rfc3339(),
        r.ok.to_string(),
        r.status.map(|s| s.to_string()).unwrap_or_default(),
    ])?;
}

wtr.flush()?;
println!("Wrote {}", out_path.display());
Ok(())
}