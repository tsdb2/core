use crate::tsz::{FieldMap, config::MetricConfig, exporter::EXPORTER};
use crate::utils::lazy::Lazy;

#[derive(Debug)]
struct CounterImpl {
    name: &'static str,
}

impl CounterImpl {
    fn new(name: &'static str, config: MetricConfig) -> Self {
        EXPORTER.define_metric_redundant(name, config);
        Self { name }
    }

    async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<i64> {
        EXPORTER
            .get_int(entity_labels, self.name, metric_fields)
            .await
    }

    async fn increment_by(&self, entity_labels: &FieldMap, delta: i64, metric_fields: &FieldMap) {
        EXPORTER
            .add_to_int(entity_labels, self.name, delta, metric_fields)
            .await;
    }

    async fn delete(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> bool {
        EXPORTER
            .delete_value(entity_labels, self.name, metric_fields)
            .await
            .is_some()
    }

    async fn delete_entity(&self, entity_labels: &FieldMap) -> bool {
        EXPORTER
            .delete_metric_from_entity(entity_labels, self.name)
            .await
    }
}

#[derive(Debug)]
pub struct Counter {
    name: &'static str,
    config: MetricConfig,
    inner: Lazy<CounterImpl>,
}

impl Counter {
    pub fn new(name: &'static str, mut config: MetricConfig) -> Self {
        config.cumulative = true;
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

    pub async fn increment_by(
        &self,
        delta: i64,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) {
        self.inner
            .increment_by(entity_labels, delta, metric_fields)
            .await;
    }

    pub async fn increment(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) {
        self.inner
            .increment_by(entity_labels, 1, metric_fields)
            .await;
    }

    pub async fn delete(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> bool {
        self.inner.delete(entity_labels, metric_fields).await
    }

    pub async fn delete_entity(&self, entity_labels: &FieldMap) -> bool {
        self.inner.delete_entity(entity_labels).await
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
        let config = MetricConfig::default().set_cumulative(true);
        let counter = Counter::new("/foo/bar/counter", config);
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        assert_eq!(counter.name(), "/foo/bar/counter");
        assert_eq!(*counter.config(), config);
        assert!(counter.get(&entity_labels, &metric_fields).await.is_none());
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
            config.set_cumulative(true).clear_bucketer()
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
        );
    }

    #[tokio::test]
    async fn test_increment_by_zero() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter
            .increment_by(0, &entity_labels, &metric_fields)
            .await;
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
        counter
            .increment_by(1, &entity_labels, &metric_fields)
            .await;
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
        counter
            .increment_by(2, &entity_labels, &metric_fields)
            .await;
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
        counter
            .increment_by(3, &entity_labels, &metric_fields)
            .await;
        counter
            .increment_by(2, &entity_labels, &metric_fields)
            .await;
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
        counter.increment(&entity_labels, &metric_fields).await;
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
        counter.increment(&entity_labels, &metric_fields).await;
        counter.increment(&entity_labels, &metric_fields).await;
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
    async fn test_delete_missing() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.delete(&entity_labels, &metric_fields).await;
        assert!(counter.get(&entity_labels, &metric_fields).await.is_none());
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 0);
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_delete() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter
            .increment_by(2, &entity_labels, &metric_fields)
            .await;
        counter.delete(&entity_labels, &metric_fields).await;
        assert!(counter.get(&entity_labels, &metric_fields).await.is_none());
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 0);
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_increment_after_deletion() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter
            .increment_by(2, &entity_labels, &metric_fields)
            .await;
        counter.delete(&entity_labels, &metric_fields).await;
        counter
            .increment_by(3, &entity_labels, &metric_fields)
            .await;
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(3));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 3);
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields)
                .await,
            Some(3)
        );
    }

    #[tokio::test]
    async fn test_delete_missing_entity() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields1 = test_metric_fields();
        let metric_fields2 = test_metric_fields();
        counter.delete_entity(&entity_labels).await;
        assert!(counter.get(&entity_labels, &metric_fields1).await.is_none());
        assert!(counter.get(&entity_labels, &metric_fields2).await.is_none());
        assert_eq!(
            counter.get_or_zero(&entity_labels, &metric_fields1).await,
            0
        );
        assert_eq!(
            counter.get_or_zero(&entity_labels, &metric_fields2).await,
            0
        );
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields1)
                .await
                .is_none()
        );
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields2)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_delete_entity() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields1 = test_metric_fields();
        let metric_fields2 = test_metric_fields();
        counter.increment(&entity_labels, &metric_fields1).await;
        counter.increment(&entity_labels, &metric_fields2).await;
        counter.delete_entity(&entity_labels).await;
        assert!(counter.get(&entity_labels, &metric_fields1).await.is_none());
        assert!(counter.get(&entity_labels, &metric_fields2).await.is_none());
        assert_eq!(
            counter.get_or_zero(&entity_labels, &metric_fields1).await,
            0
        );
        assert_eq!(
            counter.get_or_zero(&entity_labels, &metric_fields2).await,
            0
        );
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields1)
                .await
                .is_none()
        );
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields2)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_delete_another_entity() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels1 = test_entity_labels();
        let entity_labels2 = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter
            .increment_by(4, &entity_labels1, &metric_fields)
            .await;
        counter
            .increment_by(2, &entity_labels2, &metric_fields)
            .await;
        counter.delete_entity(&entity_labels1).await;
        assert!(counter.get(&entity_labels1, &metric_fields).await.is_none());
        assert_eq!(counter.get(&entity_labels2, &metric_fields).await, Some(2));
        assert_eq!(
            counter.get_or_zero(&entity_labels1, &metric_fields).await,
            0
        );
        assert_eq!(
            counter.get_or_zero(&entity_labels2, &metric_fields).await,
            2
        );
        assert!(
            EXPORTER
                .get_int(&entity_labels1, "/foo/bar/counter", &metric_fields)
                .await
                .is_none()
        );
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels2, "/foo/bar/counter", &metric_fields)
                .await,
            Some(2)
        );
    }

    #[tokio::test]
    async fn test_increment_after_entity_deletion() {
        let counter = Counter::new("/foo/bar/counter", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields1 = test_metric_fields();
        let metric_fields2 = test_metric_fields();
        counter
            .increment_by(1, &entity_labels, &metric_fields1)
            .await;
        counter
            .increment_by(2, &entity_labels, &metric_fields2)
            .await;
        counter.delete_entity(&entity_labels).await;
        counter
            .increment_by(3, &entity_labels, &metric_fields1)
            .await;
        assert_eq!(counter.get(&entity_labels, &metric_fields1).await, Some(3));
        assert!(counter.get(&entity_labels, &metric_fields2).await.is_none());
        assert_eq!(
            counter.get_or_zero(&entity_labels, &metric_fields1).await,
            3
        );
        assert_eq!(
            counter.get_or_zero(&entity_labels, &metric_fields2).await,
            0
        );
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields1)
                .await,
            Some(3)
        );
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/counter", &metric_fields2)
                .await
                .is_none()
        );
    }
}
