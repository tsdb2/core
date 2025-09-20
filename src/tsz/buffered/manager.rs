use crate::tsz::{FieldMap, config::MetricConfig, distribution::Distribution, exporter::EXPORTER};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::Mutex;

/// Implemented by all buffered metrics.
///
/// The `MetricManager` below uses this dyn-compatible trait to manage all buffered metrics and
/// flush them periodically.
pub trait Metric: std::fmt::Debug + Send + Sync {
    fn id(&self) -> u64;
    fn name(&self) -> &'static str;
    fn config(&self) -> &MetricConfig;
    fn flush(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

// Manages the buffered metrics.
#[derive(Debug)]
pub struct MetricManager {
    metrics: Mutex<BTreeMap<String, BTreeMap<u64, Arc<dyn Metric>>>>,
}

impl MetricManager {
    pub const FLUSH_PERIOD: Duration = Duration::from_secs(60);

    /// Starts the background task that periodically flushes the buffered metrics.
    pub async fn start(&'static self) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Self::FLUSH_PERIOD);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let metrics = self.metrics.lock().await;
                for (_, metrics) in &*metrics {
                    for (_, metric) in metrics {
                        metric.flush().await;
                    }
                }
            }
        });
    }

    /// Registers a buffered metric instance. Invoked automatically by `Metric` implementations when
    /// they are constructed.
    ///
    /// NOTE: since the buffered metrics may be instantiated as thread-local, this method accepts
    /// multiple registrations for the same metric name, i.e. it will keep track of multiple
    /// `Arc<dyn Metric>` objects even if they have the same name.
    pub async fn register_metric(&self, metric: Arc<dyn Metric>) {
        let metric_name = metric.name();
        EXPORTER.define_metric_redundant(metric_name, *metric.config());
        let mut metrics = self.metrics.lock().await;
        if let Some(metrics) = metrics.get_mut(metric_name) {
            let previous = metrics.insert(metric.id(), metric);
            assert!(previous.is_none());
        } else {
            metrics.insert(metric_name.into(), BTreeMap::from([(metric.id(), metric)]));
        }
    }

    /// Unregisters a buffered metric instance. Invoked automatically by `Metric` implementations
    /// upon drop.
    ///
    /// NOTE: since the buffered metrics may be instantiated as thread-local, unregistering a
    /// `Metric` object doesn't necessarily mean the metric no longer exists in this manager or in
    /// the underlying tsz exporter. Other `Metric`s (from other threads) may refer to the same
    /// metric name.
    pub async fn unregister_metric(&self, metric: Arc<dyn Metric>) {
        let metric_name = metric.name();
        let metric_id = metric.id();
        let mut metrics = self.metrics.lock().await;
        let metrics = metrics.get_mut(metric_name).unwrap();
        metrics.remove(&metric_id);
    }

    /// Retrieves an integer value in a buffered metric, atomically flushing all buffers beforehand.
    /// The returned value will be accurate even if it was updated by other threads.
    pub async fn get_int(
        &self,
        entity_labels: &FieldMap,
        metric_name: &'static str,
        metric_fields: &FieldMap,
    ) -> Option<i64> {
        let metrics = self.metrics.lock().await;
        if let Some(metrics) = metrics.get(metric_name) {
            for (_, metric) in metrics {
                metric.flush().await;
            }
            EXPORTER
                .get_int(entity_labels, metric_name, metric_fields)
                .await
        } else {
            None
        }
    }

    /// Retrieves a distribution value in a buffered metric, atomically flushing all buffers
    /// beforehand. The returned value will be accurate even if it was updated by other threads.
    pub async fn get_distribution(
        &self,
        entity_labels: &FieldMap,
        metric_name: &'static str,
        metric_fields: &FieldMap,
    ) -> Option<Distribution> {
        let metrics = self.metrics.lock().await;
        if let Some(metrics) = metrics.get(metric_name) {
            for (_, metric) in metrics {
                metric.flush().await;
            }
            EXPORTER
                .get_distribution(entity_labels, metric_name, metric_fields)
                .await
        } else {
            None
        }
    }
}

static METRIC_MANAGER_INSTANCE: LazyLock<Pin<Box<MetricManager>>> = LazyLock::new(|| {
    Box::pin(MetricManager {
        metrics: Mutex::default(),
    })
});

pub static METRIC_MANAGER: LazyLock<Pin<&MetricManager>> =
    LazyLock::new(|| METRIC_MANAGER_INSTANCE.as_ref());
