use crate::exporter::EXPORTER;
use crate::fields::FieldMap;

#[derive(Debug, Default)]
pub struct Counter {
    name: &'static str,
}

impl Counter {
    pub const fn new(name: &'static str) -> Self {
        Self { name }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub async fn get(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> Option<i64> {
        EXPORTER
            .get_int(entity_labels, self.name, metric_fields)
            .await
    }

    pub async fn get_or_zero(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> i64 {
        EXPORTER
            .get_int(entity_labels, self.name, metric_fields)
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
        EXPORTER
            .add_to_int(entity_labels, self.name, delta, metric_fields)
            .await;
    }

    pub async fn increment(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) {
        EXPORTER
            .add_to_int(entity_labels, self.name, 1, metric_fields)
            .await;
    }

    pub async fn delete(&self, entity_labels: &FieldMap, metric_fields: &FieldMap) -> bool {
        EXPORTER
            .delete_value(entity_labels, self.name, metric_fields)
            .await
            .is_some()
    }

    pub async fn delete_entity(&self, entity_labels: &FieldMap) -> bool {
        EXPORTER
            .delete_metric_from_entity(entity_labels, self.name)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fields::FieldValue;
    use std::{sync::LazyLock, sync::atomic::AtomicI64, sync::atomic::Ordering};

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
    async fn test_name() {
        let counter = Counter::new("/foo/bar");
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        assert_eq!(counter.name(), "/foo/bar");
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 0);
    }

    #[tokio::test]
    async fn test_increment_by_zero() {
        let counter = Counter::new("/foo/bar");
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter
            .increment_by(0, &entity_labels, &metric_fields)
            .await;
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(0));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 0);
    }

    #[tokio::test]
    async fn test_increment_by_one() {
        let counter = Counter::new("/foo/bar");
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter
            .increment_by(1, &entity_labels, &metric_fields)
            .await;
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(1));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 1);
    }

    #[tokio::test]
    async fn test_increment_by_two() {
        let counter = Counter::new("/foo/bar");
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter
            .increment_by(2, &entity_labels, &metric_fields)
            .await;
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(2));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 2);
    }

    #[tokio::test]
    async fn test_increment_by_delta_twice() {
        let counter = Counter::new("/foo/bar");
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
    }

    #[tokio::test]
    async fn test_increment() {
        let counter = Counter::new("/foo/bar");
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.increment(&entity_labels, &metric_fields).await;
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(1));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 1);
    }

    #[tokio::test]
    async fn test_increment_twice() {
        let counter = Counter::new("/foo/bar");
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.increment(&entity_labels, &metric_fields).await;
        counter.increment(&entity_labels, &metric_fields).await;
        assert_eq!(counter.get(&entity_labels, &metric_fields).await, Some(2));
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 2);
    }

    #[tokio::test]
    async fn test_delete_missing() {
        let counter = Counter::new("/foo/bar");
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter.delete(&entity_labels, &metric_fields).await;
        assert!(counter.get(&entity_labels, &metric_fields).await.is_none());
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 0);
    }

    #[tokio::test]
    async fn test_delete() {
        let counter = Counter::new("/foo/bar");
        let entity_labels = test_entity_labels();
        let metric_fields = test_metric_fields();
        counter
            .increment_by(2, &entity_labels, &metric_fields)
            .await;
        counter.delete(&entity_labels, &metric_fields).await;
        assert!(counter.get(&entity_labels, &metric_fields).await.is_none());
        assert_eq!(counter.get_or_zero(&entity_labels, &metric_fields).await, 0);
    }

    #[tokio::test]
    async fn test_increment_after_deletion() {
        let counter = Counter::new("/foo/bar");
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
    }

    #[tokio::test]
    async fn test_delete_missing_entity() {
        let counter = Counter::new("/foo/bar");
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
    }

    #[tokio::test]
    async fn test_delete_entity() {
        let counter = Counter::new("/foo/bar");
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
    }

    #[tokio::test]
    async fn test_delete_another_entity() {
        let counter = Counter::new("/foo/bar");
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
    }

    #[tokio::test]
    async fn test_increment_after_entity_deletion() {
        let counter = Counter::new("/foo/bar");
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
    }
}
