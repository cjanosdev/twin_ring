//! Stop/restart lifecycle. An ambiguous stop still requires restoration.
use super::{cluster, topology::NODES};
use anyhow::Result;
use reqwest::Client;
use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) trait NodeControl {
    async fn stop(&self) -> Result<()>;
    async fn start(&self) -> Result<()>;
    async fn ready(&self) -> Result<()>;
}

pub(super) struct HttpNodeControl {
    pub node: usize, // one-based control API ID
    pub strategy: String,
    pub client: Client,
}

impl NodeControl for HttpNodeControl {
    async fn stop(&self) -> Result<()> {
        cluster::node_kill(&self.node.to_string(), &self.client).await
    }
    async fn start(&self) -> Result<()> {
        match cluster::node_start(&self.node.to_string(), &self.client).await {
            Ok(()) => Ok(()),
            // A previous start may have succeeded despite a lost response.
            // Accept an already-running node only after verifying its strategy.
            Err(error) => {
                if self.ready().await.is_ok() {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }
    async fn ready(&self) -> Result<()> {
        let probe = Client::builder().timeout(Duration::from_secs(2)).build()?;
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(response) = probe
                    .get(format!("{}/strategy", NODES[self.node - 1]))
                    .send()
                    .await
                {
                    if response.status().is_success() {
                        if let Ok(body) = response.json::<serde_json::Value>().await {
                            if body["strategy"].as_str() == Some(self.strategy.as_str()) {
                                return;
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "node {} did not report strategy '{}' within 30s of restart",
                self.node,
                self.strategy
            )
        })
    }
}

#[derive(Debug, Default, Serialize)]
pub struct OutageEvents {
    pub stop_requested_ms: Option<u128>,
    pub stop_acknowledged_ms: Option<u128>,
    pub restart_requested_ms: Option<u128>,
    pub restart_acknowledged_ms: Option<u128>,
    pub ready_ms: Option<u128>,
}

pub(super) struct NodeOutage<C> {
    control: C,
    needs_restore: bool,
    pub events: OutageEvents,
}

impl<C: NodeControl> NodeOutage<C> {
    pub fn new(control: C) -> Self {
        Self {
            control,
            needs_restore: false,
            events: OutageEvents::default(),
        }
    }
    pub async fn stop(&mut self) -> Result<()> {
        // Arm BEFORE awaiting: Docker may stop the node even if the response is
        // lost, or cancellation drops this future before acknowledgement.
        self.needs_restore = true;
        self.events.stop_requested_ms = Some(unix_ms());
        self.control.stop().await?;
        self.events.stop_acknowledged_ms = Some(unix_ms());
        Ok(())
    }
    pub async fn start(&mut self) -> Result<()> {
        self.events.restart_requested_ms = Some(unix_ms());
        self.control.start().await?;
        self.events.restart_acknowledged_ms = Some(unix_ms());
        Ok(())
    }
    pub async fn confirm_ready(&mut self) -> Result<()> {
        self.control.ready().await?;
        self.events.ready_ms = Some(unix_ms());
        self.needs_restore = false;
        Ok(())
    }
    pub async fn finish(&mut self, result: Result<()>) -> Result<()> {
        let restoration = if self.needs_restore {
            println!("Restoring the cache node after the interrupted outage...");
            async {
                self.start().await?;
                self.confirm_ready().await
            }
            .await
        } else {
            Ok(())
        };
        match (result, restoration) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error.context("cache node restoration failed")),
            (Err(error), Err(cleanup)) => {
                anyhow::bail!("{error:#}; cache node restoration also failed: {cleanup:#}")
            }
        }
    }
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[cfg(test)]
#[path = "outage_tests.rs"]
mod outage_tests;
