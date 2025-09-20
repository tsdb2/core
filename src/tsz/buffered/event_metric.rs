use crate::tsz::{
    FieldMap, bucketer::BucketerRef, buffered::manager::METRIC_MANAGER, buffered::manager::Metric,
    config::MetricConfig, distribution::Distribution, exporter::EXPORTER,
};
use crate::utils::lazy::Lazy;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, atomic::AtomicU64, atomic::Ordering};
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct EventMetricImpl {
    id: u64,
    name: &'static str,
    config: MetricConfig,
    register_task_handle: Mutex<Option<JoinHandle<()>>>,
    data: Mutex<BTreeMap<(FieldMap, FieldMap), Distribution>>,
}

impl EventMetricImpl {
    fn new(name: &'static str, config: MetricConfig) -> Arc<Self> {
        static IOTA: AtomicU64 = AtomicU64::new(0);
        let metric = Arc::new(Self {
            id: IOTA.fetch_add(1, Ordering::Relaxed),
            name,
            config,
            register_task_handle: Mutex::new(None),
            data: Mutex::default(),
        });
        metric.register();
        metric
    }

    fn register(self: &Arc<Self>) {
        let metric = self.clone();
        let mut register_task_handle = self.register_task_handle.lock().unwrap();
        *register_task_handle = Some(tokio::spawn(async move {
            METRIC_MANAGER.register_metric(metric).await;
        }));
    }

    async fn await_registration(&self) {
        let mut register_task_handle = self.register_task_handle.lock().unwrap();
        if let Some(handle) = &mut *register_task_handle {
            handle.await.unwrap();
            *register_task_handle = None;
        }
    }

    async fn get(
        &self,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) -> Option<Distribution> {
        self.await_registration().await;
        METRIC_MANAGER
            .get_distribution(entity_labels, self.name, metric_fields)
            .await
    }

    fn record(&self, sample: f64, times: usize, entity_labels: FieldMap, metric_fields: FieldMap) {
        let bucketer = self.config.bucketer.unwrap();
        let bucket = bucketer.get_bucket_for(sample);
        let key = (entity_labels, metric_fields);
        let mut data = self.data.lock().unwrap();
        if let Some(distribution) = data.get_mut(&key) {
            distribution.record_to_bucket(sample, bucket, times);
        } else {
            let mut distribution = Distribution::new(bucketer);
            distribution.record_to_bucket(sample, bucket, times);
            data.insert(key, distribution);
        }
    }

    fn fetch(&self) -> BTreeMap<(FieldMap, FieldMap), Distribution> {
        let new_data = BTreeMap::default();
        let mut data = self.data.lock().unwrap();
        std::mem::replace(&mut *data, new_data)
    }

    async fn flush_impl(&self) {
        let data = self.fetch();
        let mut data_by_entity = BTreeMap::<FieldMap, BTreeMap<FieldMap, Distribution>>::default();
        for ((entity_labels, metric_fields), delta) in data {
            if let Some(entity_data) = data_by_entity.get_mut(&entity_labels) {
                if let Some(distribution) = entity_data.get_mut(&metric_fields) {
                    distribution.add(&delta).unwrap();
                } else {
                    entity_data.insert(metric_fields, delta);
                }
            } else {
                data_by_entity.insert(entity_labels, BTreeMap::from([(metric_fields, delta)]));
            }
        }
        for (entity_labels, deltas) in data_by_entity {
            EXPORTER
                .add_distribution_deltas(&entity_labels, self.name, deltas)
                .await;
        }
    }
}

impl Metric for EventMetricImpl {
    fn id(&self) -> u64 {
        self.id
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn config(&self) -> &crate::tsz::config::MetricConfig {
        &self.config
    }

    fn flush(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.flush_impl())
    }
}

#[derive(Debug)]
pub struct EventMetric {
    name: &'static str,
    config: MetricConfig,
    inner: Lazy<Arc<EventMetricImpl>>,
}

impl EventMetric {
    pub fn new(name: &'static str, mut config: MetricConfig) -> Self {
        config.cumulative = true;
        config.user_timestamps = true;
        if config.bucketer.is_none() {
            config.bucketer = Some(BucketerRef::default());
        }
        Self {
            name,
            config,
            inner: Lazy::new(move || EventMetricImpl::new(name, config)),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn config(&self) -> &MetricConfig {
        &self.config
    }

    pub fn bucketer(&self) -> BucketerRef {
        self.config.bucketer.unwrap()
    }

    pub async fn get(
        &self,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) -> Option<Distribution> {
        self.inner.get(entity_labels, metric_fields).await
    }

    pub async fn get_or_empty(
        &self,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) -> Distribution {
        self.inner
            .get(entity_labels, metric_fields)
            .await
            .or(Some(Distribution::new(self.bucketer())))
            .unwrap()
    }

    pub fn record_many(
        &self,
        sample: f64,
        times: usize,
        entity_labels: FieldMap,
        metric_fields: FieldMap,
    ) {
        self.inner
            .record(sample, times, entity_labels, metric_fields);
    }

    pub fn record(&self, sample: f64, entity_labels: FieldMap, metric_fields: FieldMap) {
        self.inner.record(sample, 1, entity_labels, metric_fields);
    }

    // TODO
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tsz::{
        bucketer::Bucketer, testing::test_entity_labels, testing::test_metric_fields,
    };

    #[tokio::test]
    async fn test_new() {
        let config = MetricConfig::default()
            .set_cumulative(true)
            .set_user_timestamps(true)
            .set_bucketer(Bucketer::default());
        let metric = EventMetric::new("/foo/bar/distribution", config);
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        assert_eq!(metric.name(), "/foo/bar/distribution");
        assert_eq!(*metric.config(), config);
        assert_eq!(metric.get(&entity_labels, &metric_fields).await, None);
        assert!(
            metric
                .get_or_empty(&entity_labels, &metric_fields)
                .await
                .is_empty()
        );
        assert!(
            EXPORTER
                .get_distribution(&entity_labels, "/foo/bar/distribution", &metric_fields)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_config_overrides() {
        let config = MetricConfig::default();
        let metric = EventMetric::new("/foo/bar/distribution", config);
        assert_eq!(
            *metric.config(),
            config
                .set_cumulative(true)
                .set_user_timestamps(true)
                .set_bucketer(Bucketer::default())
        );
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
                .set_user_timestamps(true)
                .set_bucketer(Bucketer::default())
        );
    }

    #[tokio::test]
    async fn test_record_sample() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.record(42.0, entity_labels.clone(), metric_fields.clone());
        let mut d = Distribution::default();
        d.record(42.0);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields).await,
            d.clone()
        );
        assert_eq!(
            EXPORTER
                .get_distribution(&entity_labels, "/foo/bar/distribution", &metric_fields)
                .await,
            Some(d)
        );
    }

    #[tokio::test]
    async fn test_record_sample_twice() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.record_many(42.0, 2, entity_labels.clone(), metric_fields.clone());
        let mut d = Distribution::default();
        d.record_many(42.0, 2);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields).await,
            d.clone()
        );
        assert_eq!(
            EXPORTER
                .get_distribution(&entity_labels, "/foo/bar/distribution", &metric_fields)
                .await,
            Some(d)
        );
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
        metric.record(42.0, entity_labels.clone(), metric_fields.clone());
        let mut d = Distribution::new(bucketer.into());
        d.record(42.0);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(
            metric.get_or_empty(&entity_labels, &metric_fields).await,
            d.clone()
        );
        assert_eq!(
            EXPORTER
                .get_distribution(
                    &entity_labels,
                    "/foo/bar/distribution/custom",
                    &metric_fields
                )
                .await,
            Some(d)
        );
    }

    #[tokio::test]
    async fn test_record_two_samples() {
        let metric = EventMetric::new("/foo/bar/distribution", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        metric.record(12.0, entity_labels.clone(), metric_fields.clone());
        metric.record(34.0, entity_labels.clone(), metric_fields.clone());
        let mut d = Distribution::default();
        d.record(12.0);
        d.record(34.0);
        assert_eq!(
            metric.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(metric.get_or_empty(&entity_labels, &metric_fields).await, d);
        assert_eq!(
            EXPORTER
                .get_distribution(&entity_labels, "/foo/bar/distribution", &metric_fields)
                .await,
            Some(d)
        );
    }

    // TODO
}
