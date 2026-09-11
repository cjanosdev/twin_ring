use super::*;
use futures::FutureExt;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct FakeControl {
    calls: Arc<Mutex<Vec<&'static str>>>,
    stop_error: bool,
    stop_pending: bool,
    start_error: bool,
    ready_error: bool,
}
impl NodeControl for FakeControl {
    async fn stop(&self) -> Result<()> {
        self.calls.lock().unwrap().push("stop");
        if self.stop_pending {
            std::future::pending::<()>().await;
        }
        anyhow::ensure!(!self.stop_error, "stop response lost");
        Ok(())
    }
    async fn start(&self) -> Result<()> {
        self.calls.lock().unwrap().push("start");
        anyhow::ensure!(!self.start_error, "start failed");
        Ok(())
    }
    async fn ready(&self) -> Result<()> {
        self.calls.lock().unwrap().push("ready");
        anyhow::ensure!(!self.ready_error, "wrong strategy or not ready");
        Ok(())
    }
}

#[tokio::test]
async fn overload_and_preflight_failures_do_not_touch_node_controls() {
    let control = FakeControl::default();
    let mut outage = NodeOutage::new(control.clone());
    outage.finish(Ok(())).await.unwrap();
    assert!(
        outage
            .finish(Err(anyhow::anyhow!("preflight failed")))
            .await
            .is_err()
    );
    assert!(control.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn successful_outage_stops_once_starts_once_and_confirms_readiness() {
    let control = FakeControl::default();
    let mut outage = NodeOutage::new(control.clone());
    outage.stop().await.unwrap();
    outage.start().await.unwrap();
    assert!(outage.needs_restore); // start acknowledgement is not readiness
    assert!(outage.events.restart_acknowledged_ms.is_some());
    assert!(outage.events.ready_ms.is_none());
    outage.confirm_ready().await.unwrap();
    outage.finish(Ok(())).await.unwrap();
    assert_eq!(*control.calls.lock().unwrap(), ["stop", "start", "ready"]);
}

#[tokio::test]
async fn a_lost_stop_response_still_triggers_restoration() {
    let control = FakeControl {
        stop_error: true,
        ..FakeControl::default()
    };
    let mut outage = NodeOutage::new(control.clone());
    let result = outage.stop().await;
    let error = outage.finish(result).await.unwrap_err();
    assert!(error.to_string().contains("stop response lost"));
    assert_eq!(*control.calls.lock().unwrap(), ["stop", "start", "ready"]);
}

#[tokio::test]
async fn cancellation_during_stop_does_not_lose_the_restoration_obligation() {
    let control = FakeControl {
        stop_pending: true,
        ..FakeControl::default()
    };
    let mut outage = NodeOutage::new(control.clone());
    assert!(outage.stop().now_or_never().is_none()); // poll then cancel while awaiting acknowledgement
    assert!(
        outage
            .finish(Err(anyhow::anyhow!("cancelled")))
            .await
            .is_err()
    );
    assert_eq!(*control.calls.lock().unwrap(), ["stop", "start", "ready"]);
}

#[tokio::test]
async fn a_measurement_error_restores_the_stopped_node() {
    let control = FakeControl::default();
    let mut outage = NodeOutage::new(control.clone());
    outage.stop().await.unwrap();
    let result = outage
        .finish(Err(anyhow::anyhow!("metrics stream closed")))
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("metrics stream closed")
    );
    assert_eq!(*control.calls.lock().unwrap(), ["stop", "start", "ready"]);
}

#[tokio::test]
async fn restoration_failures_preserve_both_errors_and_remain_armed() {
    let control = FakeControl {
        start_error: true,
        ..FakeControl::default()
    };
    let mut outage = NodeOutage::new(control);
    outage.stop().await.unwrap();
    let message = outage
        .finish(Err(anyhow::anyhow!("cancelled")))
        .await
        .unwrap_err()
        .to_string();
    assert!(message.contains("cancelled"));
    assert!(message.contains("start failed"));
    assert!(outage.needs_restore);
}

#[tokio::test]
async fn readiness_failure_cannot_disarm_restoration() {
    let control = FakeControl {
        ready_error: true,
        ..FakeControl::default()
    };
    let mut outage = NodeOutage::new(control);
    outage.stop().await.unwrap();
    outage.start().await.unwrap();
    assert!(outage.confirm_ready().await.is_err());
    assert!(outage.needs_restore);
    assert!(outage.events.ready_ms.is_none());
}
