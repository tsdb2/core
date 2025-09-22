use crate::tsz::{FieldMap, config::MetricConfig, distribution::Distribution, exporter::EXPORTER};
use crate::utils::lazy::Lazy;
use std::fmt::Debug;
use std::marker::PhantomData;

pub trait Value: Debug + Send + Sync {}

impl Value for bool {}
impl Value for i64 {}
impl Value for f64 {}
impl Value for String {}
impl Value for Distribution {}

#[derive(Debug)]
struct GaugeImpl<V: Value> {
    name: &'static str,
    _value: PhantomData<V>,
}

impl<V: Value> GaugeImpl<V> {
    fn new(name: &'static str, config: MetricConfig) -> Self {
        EXPORTER.define_metric_redundant(name, config);
        Self {
            name,
            _value: PhantomData::default(),
        }
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

impl GaugeImpl<bool> {
    async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<bool> {
        EXPORTER
            .get_bool(entity_labels, self.name, metric_fields)
            .await
    }

    async fn set(&self, entity_labels: &FieldMap, value: bool, metric_fields: &FieldMap) {
        EXPORTER
            .set_bool(entity_labels, self.name, value, metric_fields)
            .await;
    }
}

impl GaugeImpl<i64> {
    async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<i64> {
        EXPORTER
            .get_int(entity_labels, self.name, metric_fields)
            .await
    }

    async fn set(&self, entity_labels: &FieldMap, value: i64, metric_fields: &FieldMap) {
        EXPORTER
            .set_int(entity_labels, self.name, value, metric_fields)
            .await;
    }
}

impl GaugeImpl<f64> {
    async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<f64> {
        EXPORTER
            .get_float(entity_labels, self.name, metric_fields)
            .await
    }

    async fn set(&self, entity_labels: &FieldMap, value: f64, metric_fields: &FieldMap) {
        EXPORTER
            .set_float(entity_labels, self.name, value, metric_fields)
            .await;
    }
}

impl GaugeImpl<String> {
    async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<String> {
        EXPORTER
            .get_string(entity_labels, self.name, metric_fields)
            .await
    }

    async fn set(&self, entity_labels: &FieldMap, value: String, metric_fields: &FieldMap) {
        EXPORTER
            .set_string(entity_labels, self.name, value, metric_fields)
            .await;
    }
}

impl GaugeImpl<Distribution> {
    async fn get(
        &self,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) -> Option<Distribution> {
        EXPORTER
            .get_distribution(entity_labels, self.name, metric_fields)
            .await
    }

    async fn set(&self, entity_labels: &FieldMap, value: Distribution, metric_fields: &FieldMap) {
        EXPORTER
            .set_distribution(entity_labels, self.name, value, metric_fields)
            .await;
    }
}

#[derive(Debug)]
pub struct Gauge<V: Value> {
    name: &'static str,
    config: MetricConfig,
    inner: Lazy<GaugeImpl<V>>,
}

impl<V: Value> Gauge<V> {
    pub fn new(name: &'static str, config: MetricConfig) -> Self {
        Self {
            name,
            config,
            inner: Lazy::new(move || GaugeImpl::<V>::new(name, config)),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn config(&self) -> &MetricConfig {
        &self.config
    }

    pub async fn delete(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> bool {
        self.inner.delete(entity_labels, metric_fields).await
    }

    pub async fn delete_entity(&self, entity_labels: &FieldMap) -> bool {
        self.inner.delete_entity(entity_labels).await
    }
}

impl Gauge<bool> {
    pub async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<bool> {
        self.inner.get(entity_labels, metric_fields).await
    }

    pub async fn set(&self, value: bool, entity_labels: &FieldMap, metric_fields: &FieldMap) {
        self.inner.set(entity_labels, value, metric_fields).await;
    }
}

impl Gauge<i64> {
    pub async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<i64> {
        self.inner.get(entity_labels, metric_fields).await
    }

    pub async fn set(&self, value: i64, entity_labels: &FieldMap, metric_fields: &FieldMap) {
        self.inner.set(entity_labels, value, metric_fields).await;
    }
}

impl Gauge<f64> {
    pub async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<f64> {
        self.inner.get(entity_labels, metric_fields).await
    }

    pub async fn set(&self, value: f64, entity_labels: &FieldMap, metric_fields: &FieldMap) {
        self.inner.set(entity_labels, value, metric_fields).await;
    }
}

impl Gauge<String> {
    pub async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<String> {
        self.inner.get(entity_labels, metric_fields).await
    }

    pub async fn set(&self, value: String, entity_labels: &FieldMap, metric_fields: &FieldMap) {
        self.inner.set(entity_labels, value, metric_fields).await;
    }
}

impl Gauge<Distribution> {
    pub async fn get(
        &self,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) -> Option<Distribution> {
        self.inner.get(entity_labels, metric_fields).await
    }

    pub async fn set(
        &self,
        value: Distribution,
        entity_labels: &FieldMap,
        metric_fields: &FieldMap,
    ) {
        self.inner.set(entity_labels, value, metric_fields).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tsz::{testing::test_entity_labels, testing::test_metric_fields};

    #[tokio::test]
    async fn test_new() {
        let config = MetricConfig::default();
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", config);
        assert_eq!(gauge.name(), "/foo/bar/gauge");
        assert_eq!(*gauge.config(), config);
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        assert_eq!(gauge.get(&entity_labels, &metric_fields).await, None);
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_custom_config() {
        let config = MetricConfig::default()
            .set_delta_mode(true)
            .set_user_timestamps(true);
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", config);
        assert_eq!(
            *gauge.config(),
            config.set_delta_mode(true).set_user_timestamps(true)
        );
    }

    #[tokio::test]
    async fn test_set_bool() {
        let gauge = Gauge::<bool>::new("/foo/bar/gauge/bool", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge.set(true, &entity_labels, &metric_fields).await;
        assert_eq!(gauge.get(&entity_labels, &metric_fields).await, Some(true));
        assert_eq!(
            EXPORTER
                .get_bool(&entity_labels, "/foo/bar/gauge/bool", &metric_fields)
                .await,
            Some(true)
        );
    }

    #[tokio::test]
    async fn test_set_int() {
        let gauge = Gauge::<i64>::new("/foo/bar/gauge/int", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge.set(42, &entity_labels, &metric_fields).await;
        assert_eq!(gauge.get(&entity_labels, &metric_fields).await, Some(42));
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge/int", &metric_fields)
                .await,
            Some(42)
        );
    }

    #[tokio::test]
    async fn test_set_float() {
        let gauge = Gauge::<f64>::new("/foo/bar/gauge/float", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge.set(3.14, &entity_labels, &metric_fields).await;
        assert_eq!(gauge.get(&entity_labels, &metric_fields).await, Some(3.14));
        assert_eq!(
            EXPORTER
                .get_float(&entity_labels, "/foo/bar/gauge/float", &metric_fields)
                .await,
            Some(3.14)
        );
    }

    #[tokio::test]
    async fn test_set_string() {
        let gauge = Gauge::<String>::new("/foo/bar/gauge/string", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge
            .set("lorem".into(), &entity_labels, &metric_fields)
            .await;
        assert_eq!(
            gauge.get(&entity_labels, &metric_fields).await,
            Some("lorem".into())
        );
        assert_eq!(
            EXPORTER
                .get_string(&entity_labels, "/foo/bar/gauge/string", &metric_fields)
                .await,
            Some("lorem".into())
        );
    }

    #[tokio::test]
    async fn test_set_distribution() {
        let gauge =
            Gauge::<Distribution>::new("/foo/bar/gauge/distribution", MetricConfig::default());
        let mut d = Distribution::default();
        d.record(12.0);
        d.record(34.0);
        d.record(56.0);
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge.set(d.clone(), &entity_labels, &metric_fields).await;
        assert_eq!(
            gauge.get(&entity_labels, &metric_fields).await,
            Some(d.clone())
        );
        assert_eq!(
            EXPORTER
                .get_distribution(
                    &entity_labels,
                    "/foo/bar/gauge/distribution",
                    &metric_fields
                )
                .await,
            Some(d)
        );
    }

    #[tokio::test]
    async fn test_set_twice() {
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge.set(42, &entity_labels, &metric_fields).await;
        gauge.set(123, &entity_labels, &metric_fields).await;
        assert_eq!(gauge.get(&entity_labels, &metric_fields).await, Some(123));
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields)
                .await,
            Some(123)
        );
    }

    #[tokio::test]
    async fn test_delete_missing() {
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge.delete(&entity_labels, &metric_fields).await;
        assert!(gauge.get(&entity_labels, &metric_fields).await.is_none());
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_delete() {
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge.set(42, &entity_labels, &metric_fields).await;
        gauge.delete(&entity_labels, &metric_fields).await;
        assert!(gauge.get(&entity_labels, &metric_fields).await.is_none());
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_set_after_deletion() {
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge.set(42, &entity_labels, &metric_fields).await;
        gauge.delete(&entity_labels, &metric_fields).await;
        gauge.set(123, &entity_labels, &metric_fields).await;
        assert_eq!(gauge.get(&entity_labels, &metric_fields).await, Some(123));
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields)
                .await,
            Some(123)
        );
    }

    #[tokio::test]
    async fn test_delete_missing_entity() {
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields1 = test_metric_fields();
        let metric_fields2 = test_metric_fields();
        gauge.delete_entity(&entity_labels).await;
        assert!(gauge.get(&entity_labels, &metric_fields1).await.is_none());
        assert!(gauge.get(&entity_labels, &metric_fields2).await.is_none());
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields1)
                .await
                .is_none()
        );
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields2)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_delete_entity() {
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields1 = test_metric_fields();
        let metric_fields2 = test_metric_fields();
        gauge.set(12, &entity_labels, &metric_fields1).await;
        gauge.set(34, &entity_labels, &metric_fields2).await;
        gauge.delete_entity(&entity_labels).await;
        assert!(gauge.get(&entity_labels, &metric_fields1).await.is_none());
        assert!(gauge.get(&entity_labels, &metric_fields2).await.is_none());
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields1)
                .await
                .is_none()
        );
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields2)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_delete_another_entity() {
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", MetricConfig::default());
        let entity_labels1 = test_entity_labels();
        let entity_labels2 = test_entity_labels();
        let metric_fields = test_metric_fields();
        gauge.set(12, &entity_labels1, &metric_fields).await;
        gauge.set(34, &entity_labels2, &metric_fields).await;
        gauge.delete_entity(&entity_labels1).await;
        assert!(gauge.get(&entity_labels1, &metric_fields).await.is_none());
        assert_eq!(gauge.get(&entity_labels2, &metric_fields).await, Some(34));
        assert!(
            EXPORTER
                .get_int(&entity_labels1, "/foo/bar/gauge", &metric_fields)
                .await
                .is_none()
        );
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels2, "/foo/bar/gauge", &metric_fields)
                .await,
            Some(34)
        );
    }

    #[tokio::test]
    async fn test_set_after_entity_deletion() {
        let gauge = Gauge::<i64>::new("/foo/bar/gauge", MetricConfig::default());
        let entity_labels = test_entity_labels();
        let metric_fields1 = test_metric_fields();
        let metric_fields2 = test_metric_fields();
        gauge.set(12, &entity_labels, &metric_fields1).await;
        gauge.set(34, &entity_labels, &metric_fields2).await;
        gauge.delete_entity(&entity_labels).await;
        gauge.set(56, &entity_labels, &metric_fields1).await;
        assert_eq!(gauge.get(&entity_labels, &metric_fields1).await, Some(56));
        assert!(gauge.get(&entity_labels, &metric_fields2).await.is_none());
        assert_eq!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields1)
                .await,
            Some(56)
        );
        assert!(
            EXPORTER
                .get_int(&entity_labels, "/foo/bar/gauge", &metric_fields2)
                .await
                .is_none()
        );
    }
}
