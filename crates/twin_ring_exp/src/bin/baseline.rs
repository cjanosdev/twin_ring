use rand::prelude::*; // brings in SliceRandom, Rng, etc.
use reqwest::Client;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new();
    let peers = vec![
        "http://localhost:8001",
        "http://localhost:8002",
        "http://localhost:8003",
    ];

    let num_requests = 1000;
    let mut rng = rand::rng(); // new way in rand 0.9

    println!("Starting baseline simulation with {num_requests} requests...");

    for i in 0..num_requests {
        let peer = peers.choose(&mut rng).unwrap();
        let key = format!("key{}", rng.random_range(0..100));
        let value = format!("val{}", i);

        let start = Instant::now();

        // Simple put
       let put_url = format!("{}/put/{}", peer, key);
       println!("PUT {}", put_url);
       let _ = client.post(&put_url).body(value.clone()).send().await?;
       //let _ = client.post(&put_url).send().await?;

        // Simple get
        let get_url = format!("{}/get/{}", peer, key);
        println!("GET {}", get_url);
        let resp = client.get(&get_url).send().await?;
        let elapsed = start.elapsed();

        println!("Req {} -> {} (took {:?})", i, resp.status(), elapsed);
    }

    Ok(())
}