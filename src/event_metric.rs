use crate::bucketer::Bucketer;
use crate::distribution::Distribution;
use crate::exporter::{EXPORTER, MetricConfig};
use crate::fields::FieldMap;
use std::sync::OnceLock;

#[derive(Debug)]
struct EventMetricImpl {}

impl EventMetricImpl {
    fn new(name: &'static str, config: MetricConfig) -> Self {
        EXPORTER.define_metric_redundant(name, config);
        Self {}
    }

    async fn get(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
        metric_fields: &FieldMap,
    ) -> Option<Distribution> {
        EXPORTER
            .get_distribution(entity_labels, metric_name, metric_fields)
            .await
    }

    async fn record(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
        sample: f64,
        times: usize,
        metric_fields: &FieldMap,
    ) {
        EXPORTER
            .add_many_to_distribution(entity_labels, metric_name, sample, times, metric_fields)
            .await
    }

    async fn delete(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
        metric_fields: &FieldMap,
    ) -> bool {
        EXPORTER
            .delete_value(entity_labels, metric_name, metric_fields)
            .await
            .is_some()
    }

    async fn delete_entity(&self, entity_labels: &FieldMap, metric_name: &str) -> bool {
        EXPORTER
            .delete_metric_from_entity(entity_labels, metric_name)
            .await
    }
}

#[derive(Debug)]
pub struct EventMetric {
    name: &'static str,
    config: MetricConfig,
    inner: OnceLock<EventMetricImpl>,
}

impl EventMetric {
    pub fn new(name: &'static str, mut config: MetricConfig) -> Self {
        config.cumulative = true;
        Self {
            name,
            config,
            inner: OnceLock::default(),
        }
    }

    fn inner(&self) -> &EventMetricImpl {
        self.inner
            .get_or_init(|| EventMetricImpl::new(self.name, self.config))
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn config(&self) -> &MetricConfig {
        &self.config
    }

    pub async fn get(
        &self,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) -> Option<Distribution> {
        self.inner()
            .get(entity_labels, self.name, metric_fields)
            .await
    }

    pub async fn get_or_empty(
        &self,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) -> Distribution {
        let bucketer = match self.config.bucketer {
            Some(bucketer) => bucketer,
            None => Bucketer::default().into(),
        };
        self.inner()
            .get(entity_labels, self.name, metric_fields)
            .await
            .or(Some(Distribution::new(bucketer)))
            .unwrap()
    }

    pub async fn record_many(
        &self,
        sample: f64,
        times: usize,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) {
        self.inner()
            .record(entity_labels, self.name, sample, times, metric_fields)
            .await
    }

    pub async fn record(&self, sample: f64, entity_labels: &FieldMap, metric_fields: &FieldMap) {
        self.inner()
            .record(entity_labels, self.name, sample, 1, metric_fields)
            .await
    }

    pub async fn delete(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> bool {
        self.inner()
            .delete(entity_labels, self.name, metric_fields)
            .await
    }

    pub async fn delete_entity(&self, entity_labels: &FieldMap) -> bool {
        self.inner().delete_entity(entity_labels, self.name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fields::FieldValue;
    use std::sync::{LazyLock, atomic::AtomicI64, atomic::Ordering};

    fn test_entity_labels() -> FieldMap {
        static IOTA: LazyLock<AtomicI64> = LazyLock::new(|| AtomicI64::from(42));
        FieldMap::from([
            ("sator", FieldValue::Str("arepo".into())),
            (
                "lorem",
                FieldValue::Int(IOTA.fetch_add(1, Ordering::Relaxed)),
            ),
        ])
    }

    fn test_metric_fields() -> FieldMap {
        static IOTA: LazyLock<AtomicI64> = LazyLock::new(|| AtomicI64::from(42));
        FieldMap::from([
            ("tenet", FieldValue::Bool(true)),
            (
                "opera",
                FieldValue::Int(IOTA.fetch_add(1, Ordering::Relaxed)),
            ),
        ])
    }

    #[tokio::test]
    async fn test_new() {
        let config = MetricConfig::default().set_cumulative(true);
        let metric = EventMetric::new("/foo/bar/distribution", config);
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        assert_eq!(metric.name(), "/foo/bar/distribution");
        assert_eq!(*metric.config(), config);
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields).await,
            Distribution::default()
        );
    }

    #[tokio::test]
    async fn test_config_overrides() {
        let config = MetricConfig::default();
        let metric = EventMetric::new("/foo/bar/distribution", config);
        assert_eq!(*metric.config(), config.set_cumulative(true));
    }

    #[tokio::test]
    async fn test_custom_config() {
        let config = MetricConfig::default()
            .set_skip_stable_cells(true)
            .set_delta_mode(true);
        let metric = EventMetric::new("/foo/bar/distribution", config);
        assert_eq!(
            *metric.config(),
            config
                .set_cumulative(true)
                .set_skip_stable_cells(true)
                .set_delta_mode(true)
        );
    }

    #[tokio::test]
    async fn test_record_sample() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.record(42.0, &entity_labels, &metric_fields).await;
        let mut d = Distribution::default();
        d.record(42.0);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(metric.get_or_empty(&entity_labels, &metric_fields).await, d);
    }

    #[tokio::test]
    async fn test_record_sample_twice() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric
            .record_many(42.0, 2, &entity_labels, &metric_fields)
            .await;
        let mut d = Distribution::default();
        d.record_many(42.0, 2);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(metric.get_or_empty(&entity_labels, &metric_fields).await, d);
    }

    #[tokio::test]
    async fn test_record_with_custom_bucketer() {
        let bucketer = Bucketer::custom(1.0, 2.0, 0.5, 20);
        let metric = EventMetric::new(
            "/foo/bar/distribution/custom",
            MetricConfig::default().set_bucketer(bucketer),
        );
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.record(42.0, &entity_labels, &metric_fields).await;
        let mut d = Distribution::new(bucketer.into());
        d.record(42.0);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(metric.get_or_empty(&entity_labels, &metric_fields).await, d);
    }

    #[tokio::test]
    async fn test_record_two_samples() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.record(12.0, &entity_labels, &metric_fields).await;
        metric.record(34.0, &entity_labels, &metric_fields).await;
        let mut d = Distribution::default();
        d.record(12.0);
        d.record(34.0);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(metric.get_or_empty(&entity_labels, &metric_fields).await, d);
    }

    #[tokio::test]
    async fn test_delete_missing() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.delete(&entity_labels, &metric_fields).await;
        assert!(metric.get(&entity_labels, &metric_fields).await.is_none());
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields).await,
            Distribution::default()
        );
    }

    #[tokio::test]
    async fn test_delete() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.record(42.0, &entity_labels, &metric_fields).await;
        metric.delete(&entity_labels, &metric_fields).await;
        assert!(metric.get(&entity_labels, &metric_fields).await.is_none());
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields).await,
            Distribution::default()
        );
    }

    #[tokio::test]
    async fn test_record_after_deletion() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.record(12.0, &entity_labels, &metric_fields).await;
        metric.delete(&entity_labels, &metric_fields).await;
        metric.record(34.0, &entity_labels, &metric_fields).await;
        let mut d = Distribution::default();
        d.record(34.0);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(metric.get_or_empty(&entity_labels, &metric_fields).await, d);
    }

    #[tokio::test]
    async fn test_delete_missing_entity() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields1 = test_metric_fields();
        let metric_fields2 = test_metric_fields();
        metric.delete_entity(&entity_labels).await;
        assert!(metric.get(&entity_labels, &metric_fields1).await.is_none());
        assert!(metric.get(&entity_labels, &metric_fields2).await.is_none());
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields1).await,
            Distribution::default()
        );
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields2).await,
            Distribution::default()
        );
    }

    #[tokio::test]
    async fn test_delete_entity() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields1 = test_metric_fields();
        let metric_fields2 = test_metric_fields();
        metric.record(12.0, &entity_labels, &metric_fields1).await;
        metric.record(34.0, &entity_labels, &metric_fields2).await;
        metric.delete_entity(&entity_labels).await;
        assert!(metric.get(&entity_labels, &metric_fields1).await.is_none());
        assert!(metric.get(&entity_labels, &metric_fields2).await.is_none());
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields1).await,
            Distribution::default()
        );
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields2).await,
            Distribution::default()
        );
    }

    #[tokio::test]
    async fn test_delete_another_entity() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels1 = test_entity_labels();
        let entity_labels2 = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.record(12.0, &entity_labels1, &metric_fields).await;
        metric.record(34.0, &entity_labels2, &metric_fields).await;
        metric.delete_entity(&entity_labels1).await;
        let mut d = Distribution::default();
        d.record(34.0);
        assert!(metric.get(&entity_labels1, &metric_fields).await.is_none());
        assert_eq!(
            metric.get(&entity_labels2, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(
            metric.get_or_empty(&entity_labels1, &metric_fields).await,
            Distribution::default()
        );
        assert_eq!(
            metric.get_or_empty(&entity_labels2, &metric_fields).await,
            d
        );
    }

    #[tokio::test]
    async fn test_record_after_entity_deletion() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields1 = test_metric_fields();
        let metric_fields2 = test_metric_fields();
        metric.record(12.0, &entity_labels, &metric_fields1).await;
        metric.record(34.0, &entity_labels, &metric_fields2).await;
        metric.delete_entity(&entity_labels).await;
        metric.record(56.0, &entity_labels, &metric_fields1).await;
        let mut d = Distribution::default();
        d.record(56.0);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields1).await,
            Some(d.clone())
        );
        assert!(metric.get(&entity_labels, &metric_fields2).await.is_none());
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields1).await,
            d
        );
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields2).await,
            Distribution::default()
        );
    }
}
