mod manager;

pub mod counter;
pub mod event_metric;

pub async fn init() {
    manager::METRIC_MANAGER.start().await;
}
