use crate::tsz::{
    FieldMap, buffered::manager::METRIC_MANAGER, buffered::manager::Metric, config::MetricConfig,
    exporter::EXPORTER,
};
use crate::utils::lazy::Lazy;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, atomic::AtomicU64, atomic::Ordering};
use tokio::task::JoinHandle;

#[derive(Debug)]
struct CounterImpl {
    id: u64,
    name: &'static str,
    config: MetricConfig,
    register_task_handle: Mutex<Option<JoinHandle<()>>>,
    data: Mutex<BTreeMap<(FieldMap, FieldMap), i64>>,
}

impl CounterImpl {
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

    async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<i64> {
        self.await_registration().await;
        METRIC_MANAGER
            .get_int(entity_labels, self.name, metric_fields)
            .await
    }

    fn increment_by(&self, delta: i64, entity_labels: FieldMap, metric_fields: FieldMap) {
        let key = (entity_labels, metric_fields);
        let mut data = self.data.lock().unwrap();
        if let Some(value) = data.get_mut(&key) {
            *value += delta;
        } else {
            data.insert(key, delta);
        }
    }

    fn fetch(&self) -> BTreeMap<(FieldMap, FieldMap), i64> {
        let new_data = BTreeMap::default();
        let mut data = self.data.lock().unwrap();
        std::mem::replace(&mut *data, new_data)
    }

    async fn flush_impl(&self) {
        let data = self.fetch();
        let mut data_by_entity = BTreeMap::<FieldMap, BTreeMap<FieldMap, i64>>::default();
        for ((entity_labels, metric_fields), delta) in data {
            if let Some(entity_data) = data_by_entity.get_mut(&entity_labels) {
                if let Some(value) = entity_data.get_mut(&metric_fields) {
                    *value += delta;
                } else {
                    entity_data.insert(metric_fields, delta);
                }
            } else {
                data_by_entity.insert(entity_labels, BTreeMap::from([(metric_fields, delta)]));
            }
        }
        for (entity_labels, deltas) in data_by_entity {
            EXPORTER
                .add_int_deltas(&entity_labels, self.name, deltas)
                .await;
        }
    }
}

impl Metric for CounterImpl {
    fn id(&self) -> u64 {
        self.id
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn config(&self) -> &MetricConfig {
        &self.config
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.flush_impl())
    }
}

#[derive(Debug)]
pub struct Counter {
    name: &'static str,
    config: MetricConfig,
    inner: Lazy<Arc<CounterImpl>>,
}

impl Counter {
    pub fn new(name: &'static str, mut config: MetricConfig) -> Self {
        config.cumulative = true;
        config.user_timestamps = true;
        config.bucketer = None;
        Self {
            name,
            config,
            inner: Lazy::new(move || CounterImpl::new(name, config)),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn config(&self) -> &MetricConfig {
        &self.config
    }

    pub async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<i64> {
        self.inner.get(entity_labels, metric_fields).await
    }

    pub async fn get_or_zero(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> i64 {
        self.inner
            .get(entity_labels, metric_fields)
            .await
            .or(Some(0))
            .unwrap()
    }

    pub fn increment_by(&self, delta: i64, entity_labels: FieldMap, metric_fields: FieldMap) {
        self.inner.increment_by(delta, entity_labels, metric_fields);
    }

    pub fn increment(&self, entity_labels: FieldMap, metric_fields: FieldMap) {
        self.inner.increment_by(1, entity_labels, metric_fields);
    }

    // TODO
}

impl Drop for Counter {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            METRIC_MANAGER.unregister_metric(inner).await;
        });
    }
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
            .set_user_timestamps(true);
        let counter = Counter::new("/foo/bar/counter", config);
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        assert_eq!(counter.name(), "/foo/bar/counter");
        assert_eq!(*counter.config(), config);
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, None);
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 0);
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_config_overrides() {
        let config = MetricConfig::default().set_bucketer(Bucketer::fixed_width(1.0, 20));
        let counter = Counter::new("/foo/bar/counter", config);
        assert_eq!(
            *counter.config(),
            config
                .set_cumulative(true)
                .set_user_timestamps(true)
                .clear_bucketer()
        );
    }

    #[tokio::test]
    async fn test_custom_config() {
        let config = MetricConfig::default()
            .set_skip_stable_cells(true)
            .set_delta_mode(true);
        let counter = Counter::new("/foo/bar/counter", config);
        assert_eq!(
            *counter.config(),
            config
                .set_cumulative(true)
                .set_skip_stable_cells(true)
                .set_delta_mode(true)
                .set_user_timestamps(true)
        );
    }

    #[tokio::test]
    async fn test_increment_by_zero() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.increment_by(0, entity_labels.clone(), metric_fields.clone());
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(0));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 0);
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await,
            Some(0)
        );
    }

    #[tokio::test]
    async fn test_increment_by_one() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.increment_by(1, entity_labels.clone(), metric_fields.clone());
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(1));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 1);
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_increment_by_two() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.increment_by(2, entity_labels.clone(), metric_fields.clone());
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(2));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 2);
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await,
            Some(2)
        );
    }

    #[tokio::test]
    async fn test_increment_by_delta_twice() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.increment_by(3, entity_labels.clone(), metric_fields.clone());
        counter.increment_by(2, entity_labels.clone(), metric_fields.clone());
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(5));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 5);
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await,
            Some(5)
        );
    }

    #[tokio::test]
    async fn test_increment() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.increment(entity_labels.clone(), metric_fields.clone());
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(1));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 1);
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_increment_twice() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.increment(entity_labels.clone(), metric_fields.clone());
        counter.increment(entity_labels.clone(), metric_fields.clone());
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(2));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 2);
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await,
            Some(2)
        );
    }
}
