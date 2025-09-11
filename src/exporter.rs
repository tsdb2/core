use crate::clock::Clock;
use crate::clock::RealClock;
use crate::f64::F64;
use crate::fields::FieldMap;
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, atomic::AtomicUsize, atomic::Ordering};
use std::time::SystemTime;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Bool(bool),
    Int(i64),
    Float(F64),
    Str(String),
}

#[derive(Debug, Clone)]
struct Cell {
    value: Value,
    start_timestamp: SystemTime,
    update_timestamp: SystemTime,
}

#[derive(Debug, Clone)]
struct Metric {
    name: String,
    cells: BTreeMap<FieldMap, Cell>,
}

impl Metric {
    fn new(name: String) -> Self {
        Self {
            name,
            cells: BTreeMap::default(),
        }
    }

    fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    fn get_value(&self, metric_fields: &FieldMap) -> Option<Value> {
        if let Some(cell) = self.cells.get(metric_fields) {
            Some(cell.value.clone())
        } else {
            None
        }
    }

    fn get_bool(&self, metric_fields: &FieldMap) -> Option<bool> {
        if let Some(cell) = self.cells.get(metric_fields) {
            match cell.value {
                Value::Bool(value) => Some(value),
                _ => panic!(),
            }
        } else {
            None
        }
    }

    fn get_int(&self, metric_fields: &FieldMap) -> Option<i64> {
        if let Some(cell) = self.cells.get(metric_fields) {
            match cell.value {
                Value::Int(value) => Some(value),
                _ => panic!(),
            }
        } else {
            None
        }
    }

    fn get_float(&self, metric_fields: &FieldMap) -> Option<f64> {
        if let Some(cell) = self.cells.get(metric_fields) {
            match cell.value {
                Value::Float(value) => Some(value.value),
                _ => panic!(),
            }
        } else {
            None
        }
    }

    fn get_string(&self, metric_fields: &FieldMap) -> Option<String> {
        if let Some(cell) = self.cells.get(metric_fields) {
            match &cell.value {
                Value::Str(value) => Some(value.clone()),
                _ => panic!(),
            }
        } else {
            None
        }
    }

    fn set_value(&mut self, value: Value, metric_fields: &FieldMap, now: SystemTime) {
        if let Some(cell) = self.cells.get_mut(metric_fields) {
            cell.value = value;
            cell.update_timestamp = now;
        } else {
            self.cells.insert(
                metric_fields.clone(),
                Cell {
                    value,
                    start_timestamp: now,
                    update_timestamp: now,
                },
            );
        };
    }

    fn add_to_int(&mut self, delta: i64, metric_fields: &FieldMap, now: SystemTime) {
        if let Some(cell) = self.cells.get_mut(metric_fields) {
            match &mut cell.value {
                Value::Int(value) => *value += delta,
                _ => panic!(),
            };
            cell.update_timestamp = now;
        } else {
            self.cells.insert(
                metric_fields.clone(),
                Cell {
                    value: Value::Int(delta),
                    start_timestamp: now,
                    update_timestamp: now,
                },
            );
        };
    }

    fn delete_value(&mut self, metric_fields: &FieldMap) -> Option<Value> {
        self.cells.remove(metric_fields).map(|cell| cell.value)
    }
}

impl PartialEq for Metric {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Metric {}

impl PartialOrd for Metric {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Ord for Metric {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl Borrow<str> for Metric {
    fn borrow(&self) -> &str {
        self.name.as_str()
    }
}

trait EntityManager: Debug + Send + Sync {
    fn remove_entity<'a>(
        &'a self,
        entity_labels: &'a FieldMap,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>>;
}

#[derive(Debug)]
struct Entity<'a> {
    parent: &'a dyn EntityManager,
    labels: FieldMap,
    pin_count: AtomicUsize,
    metrics: Mutex<BTreeSet<Metric>>,
}

impl<'a> Entity<'a> {
    fn new(parent: &'a dyn EntityManager, labels: FieldMap) -> Self {
        Self {
            parent,
            labels,
            pin_count: AtomicUsize::default(),
            metrics: Mutex::default(),
        }
    }

    fn is_pinned(&self) -> bool {
        self.pin_count.load(Ordering::Acquire) > 0
    }

    fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::Relaxed);
    }

    fn unpin(&self) -> bool {
        self.pin_count.fetch_sub(1, Ordering::AcqRel) == 1
    }

    async fn get_value(&self, metric_name: &str, metric_fields: &FieldMap) -> Option<Value> {
        let metrics = self.metrics.lock().await;
        if let Some(metric) = metrics.get(metric_name) {
            metric.get_value(metric_fields)
        } else {
            None
        }
    }

    async fn get_bool(&self, metric_name: &str, metric_fields: &FieldMap) -> Option<bool> {
        let metrics = self.metrics.lock().await;
        if let Some(metric) = metrics.get(metric_name) {
            metric.get_bool(metric_fields)
        } else {
            None
        }
    }

    async fn get_int(&self, metric_name: &str, metric_fields: &FieldMap) -> Option<i64> {
        let metrics = self.metrics.lock().await;
        if let Some(metric) = metrics.get(metric_name) {
            metric.get_int(metric_fields)
        } else {
            None
        }
    }

    async fn get_float(&self, metric_name: &str, metric_fields: &FieldMap) -> Option<f64> {
        let metrics = self.metrics.lock().await;
        if let Some(metric) = metrics.get(metric_name) {
            metric.get_float(metric_fields)
        } else {
            None
        }
    }

    async fn get_string(&self, metric_name: &str, metric_fields: &FieldMap) -> Option<String> {
        let metrics = self.metrics.lock().await;
        if let Some(metric) = metrics.get(metric_name) {
            metric.get_string(metric_fields)
        } else {
            None
        }
    }

    async fn set_value(
        &self,
        metric_name: &str,
        value: Value,
        metric_fields: &FieldMap,
        now: SystemTime,
    ) {
        let mut metrics = self.metrics.lock().await;
        let mut metric = if let Some(metric) = metrics.take(metric_name) {
            metric
        } else {
            Metric::new(metric_name.into())
        };
        metric.set_value(value, metric_fields, now);
        metrics.insert(metric);
    }

    async fn add_to_int(
        &self,
        metric_name: &str,
        delta: i64,
        metric_fields: &FieldMap,
        now: SystemTime,
    ) {
        let mut metrics = self.metrics.lock().await;
        let mut metric = if let Some(metric) = metrics.take(metric_name) {
            metric
        } else {
            Metric::new(metric_name.into())
        };
        metric.add_to_int(delta, metric_fields, now);
        metrics.insert(metric);
    }

    async fn delete_value(&self, metric_name: &str, metric_fields: &FieldMap) -> Option<Value> {
        let mut metrics = self.metrics.lock().await;
        let result = if let Some(mut metric) = metrics.take(metric_name) {
            let result = metric.delete_value(metric_fields);
            if !metric.is_empty() {
                metrics.insert(metric);
            }
            result
        } else {
            None
        };
        if metrics.is_empty() && !self.is_pinned() {
            self.parent.remove_entity(&self.labels).await;
        }
        result
    }

    async fn delete_metric(&self, metric_name: &str) -> bool {
        let mut metrics = self.metrics.lock().await;
        let result = metrics.remove(metric_name);
        if metrics.is_empty() && !self.is_pinned() {
            self.parent.remove_entity(&self.labels).await;
        }
        result
    }

    async fn clear(&self) {
        let mut metrics = self.metrics.lock().await;
        metrics.clear();
        if !self.is_pinned() {
            self.parent.remove_entity(&self.labels).await;
        }
    }
}

impl<'a> PartialEq for Entity<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.labels == other.labels
    }
}

impl<'a> Eq for Entity<'a> {}

impl<'a> PartialOrd for Entity<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.labels.partial_cmp(&other.labels)
    }
}

impl<'a> Ord for Entity<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.labels.cmp(&other.labels)
    }
}

impl<'a> Borrow<FieldMap> for Arc<Entity<'a>> {
    fn borrow(&self) -> &FieldMap {
        &self.labels
    }
}

struct EntityPin<'a> {
    entity: Arc<Entity<'a>>,
}

impl<'a> EntityPin<'a> {
    fn new(entity: Arc<Entity<'a>>) -> Self {
        Self { entity }
    }
}

impl<'a> Deref for EntityPin<'a> {
    type Target = Entity<'a>;

    fn deref(&self) -> &Self::Target {
        self.entity.as_ref()
    }
}

impl<'a> Drop for EntityPin<'a> {
    fn drop(&mut self) {
        self.entity.unpin();
    }
}

#[derive(Debug)]
pub struct Exporter<'a> {
    clock: Arc<dyn Clock>,
    entities: Mutex<BTreeSet<Arc<Entity<'a>>>>,
}

impl<'a> Exporter<'a> {
    async fn get_ephemeral_entity(&self, labels: &FieldMap) -> Option<Arc<Entity<'a>>> {
        let entities = self.entities.lock().await;
        entities.get(labels).cloned()
    }

    async fn get_pinned_entity(self: Pin<&'a Self>, labels: &FieldMap) -> EntityPin<'a> {
        let mut entities = self.entities.lock().await;
        if let Some(entity) = entities.get(labels) {
            EntityPin::new(entity.clone())
        } else {
            let entity = Arc::new(Entity::new(self.get_ref(), labels.clone()));
            entities.insert(entity.clone());
            EntityPin::new(entity)
        }
    }

    pub async fn get_value(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
        metric_fields: &FieldMap,
    ) -> Option<Value> {
        if let Some(entity) = self.get_ephemeral_entity(entity_labels).await {
            entity.get_value(metric_name, metric_fields).await
        } else {
            None
        }
    }

    pub async fn get_bool(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
        metric_fields: &FieldMap,
    ) -> Option<bool> {
        if let Some(entity) = self.get_ephemeral_entity(entity_labels).await {
            entity.get_bool(metric_name, metric_fields).await
        } else {
            None
        }
    }

    pub async fn get_int(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
        metric_fields: &FieldMap,
    ) -> Option<i64> {
        if let Some(entity) = self.get_ephemeral_entity(entity_labels).await {
            entity.get_int(metric_name, metric_fields).await
        } else {
            None
        }
    }

    pub async fn get_float(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
        metric_fields: &FieldMap,
    ) -> Option<f64> {
        if let Some(entity) = self.get_ephemeral_entity(entity_labels).await {
            entity.get_float(metric_name, metric_fields).await
        } else {
            None
        }
    }

    pub async fn get_string(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
        metric_fields: &FieldMap,
    ) -> Option<String> {
        if let Some(entity) = self.get_ephemeral_entity(entity_labels).await {
            entity.get_string(metric_name, metric_fields).await
        } else {
            None
        }
    }

    pub async fn set_value(
        self: Pin<&'a Self>,
        entity_labels: &FieldMap,
        metric_name: &str,
        value: Value,
        metric_fields: &FieldMap,
    ) {
        let now = self.clock.now();
        self.get_pinned_entity(entity_labels)
            .await
            .set_value(metric_name, value, metric_fields, now)
            .await;
    }

    pub async fn set_bool(
        self: Pin<&'a Self>,
        entity_labels: &FieldMap,
        metric_name: &str,
        value: bool,
        metric_fields: &FieldMap,
    ) {
        let now = self.clock.now();
        self.get_pinned_entity(entity_labels)
            .await
            .set_value(metric_name, Value::Bool(value), metric_fields, now)
            .await;
    }

    pub async fn set_int(
        self: Pin<&'a Self>,
        entity_labels: &FieldMap,
        metric_name: &str,
        value: i64,
        metric_fields: &FieldMap,
    ) {
        let now = self.clock.now();
        self.get_pinned_entity(entity_labels)
            .await
            .set_value(metric_name, Value::Int(value), metric_fields, now)
            .await;
    }

    pub async fn set_float(
        self: Pin<&'a Self>,
        entity_labels: &FieldMap,
        metric_name: &str,
        value: f64,
        metric_fields: &FieldMap,
    ) {
        let now = self.clock.now();
        self.get_pinned_entity(entity_labels)
            .await
            .set_value(metric_name, Value::Float(value.into()), metric_fields, now)
            .await;
    }

    pub async fn set_string(
        self: Pin<&'a Self>,
        entity_labels: &FieldMap,
        metric_name: &str,
        value: String,
        metric_fields: &FieldMap,
    ) {
        let now = self.clock.now();
        self.get_pinned_entity(entity_labels)
            .await
            .set_value(metric_name, Value::Str(value), metric_fields, now)
            .await;
    }

    pub async fn add_to_int(
        self: Pin<&'a Self>,
        entity_labels: &FieldMap,
        metric_name: &str,
        delta: i64,
        metric_fields: &FieldMap,
    ) {
        let now = self.clock.now();
        self.get_pinned_entity(entity_labels)
            .await
            .add_to_int(metric_name, delta, metric_fields, now)
            .await;
    }

    pub async fn delete_value(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
        metric_fields: &FieldMap,
    ) -> Option<Value> {
        if let Some(entity) = self.get_ephemeral_entity(entity_labels).await {
            entity.delete_value(metric_name, metric_fields).await
        } else {
            None
        }
    }

    pub async fn delete_metric_from_entity(
        &self,
        entity_labels: &FieldMap,
        metric_name: &str,
    ) -> bool {
        if let Some(entity) = self.get_ephemeral_entity(entity_labels).await {
            entity.delete_metric(metric_name).await
        } else {
            false
        }
    }

    pub async fn delete_metric(&self, metric_name: &str) {
        let entities = self.entities.lock().await;
        for entity in entities.iter() {
            entity.delete_metric(metric_name).await;
        }
    }

    pub async fn delete_entity(&self, entity_labels: &FieldMap) -> bool {
        if let Some(entity) = self.get_ephemeral_entity(entity_labels).await {
            entity.clear().await;
            true
        } else {
            false
        }
    }

    #[cfg(test)]
    pub async fn clear(&self) {
        let mut entities = self.entities.lock().await;
        entities.clear();
    }
}

impl<'a> EntityManager for Exporter<'a> {
    fn remove_entity<'b>(
        &'b self,
        entity_labels: &'b FieldMap,
    ) -> Pin<Box<dyn Future<Output = ()> + 'b>> {
        Box::pin(async move {
            let mut entities = self.entities.lock().await;
            if let Some(entity) = entities.get(entity_labels) {
                if !entity.is_pinned() {
                    entities.remove(entity_labels);
                }
            }
        })
    }
}

impl<'a> Default for Exporter<'a> {
    fn default() -> Self {
        Self {
            clock: Arc::new(RealClock::default()),
            entities: Mutex::default(),
        }
    }
}

static EXPORTER_INSTANCE: LazyLock<Pin<Box<Exporter>>> =
    LazyLock::new(|| Box::pin(Exporter::default()));

pub static EXPORTER: LazyLock<Pin<&Exporter>> = LazyLock::new(|| EXPORTER_INSTANCE.as_ref());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::test::MockClock;
    use crate::fields::FieldValue;

    #[test]
    fn test_empty_metric() {
        let metric = Metric::new("/foo/bar".into());
        assert!(metric.is_empty());
        assert!(metric.get_value(&FieldMap::from([])).is_none());
        let test_fields = FieldMap::from([("lorem", FieldValue::Str("ipsum".into()))]);
        assert!(metric.get_value(&test_fields).is_none());
        assert!(metric.get_bool(&test_fields).is_none());
        assert!(metric.get_int(&test_fields).is_none());
        assert!(metric.get_float(&test_fields).is_none());
        assert!(metric.get_string(&test_fields).is_none());
    }

    #[test]
    fn test_set_bool_metric_value_no_fields() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        metric.set_value(Value::Bool(true), &FieldMap::from([]), clock.now());
        assert!(!metric.is_empty());
        assert_eq!(
            metric.get_value(&FieldMap::from([])),
            Some(Value::Bool(true))
        );
        assert_eq!(metric.get_bool(&FieldMap::from([])), Some(true));
    }

    #[test]
    fn test_set_int_metric_value_no_fields() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        metric.set_value(Value::Int(42), &FieldMap::from([]), clock.now());
        assert!(!metric.is_empty());
        assert_eq!(metric.get_value(&FieldMap::from([])), Some(Value::Int(42)));
        assert_eq!(metric.get_int(&FieldMap::from([])), Some(42));
    }

    #[test]
    fn test_set_float_metric_value_no_fields() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        metric.set_value(Value::Float(3.14.into()), &FieldMap::from([]), clock.now());
        assert!(!metric.is_empty());
        assert_eq!(
            metric.get_value(&FieldMap::from([])),
            Some(Value::Float(3.14.into()))
        );
        assert_eq!(metric.get_float(&FieldMap::from([])), Some(3.14));
    }

    #[test]
    fn test_set_string_metric_value_no_fields() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        metric.set_value(Value::Str("lorem".into()), &FieldMap::from([]), clock.now());
        assert!(!metric.is_empty());
        assert_eq!(
            metric.get_value(&FieldMap::from([])),
            Some(Value::Str("lorem".into()))
        );
        assert_eq!(metric.get_string(&FieldMap::from([])), Some("lorem".into()));
    }

    #[test]
    fn test_set_bool_metric_value() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        metric.set_value(Value::Bool(true), &metric_fields, clock.now());
        assert!(!metric.is_empty());
        assert_eq!(metric.get_value(&metric_fields), Some(Value::Bool(true)));
        assert_eq!(metric.get_bool(&metric_fields), Some(true));
    }

    #[test]
    fn test_set_int_metric_value() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        metric.set_value(Value::Int(42), &metric_fields, clock.now());
        assert!(!metric.is_empty());
        assert_eq!(metric.get_value(&metric_fields), Some(Value::Int(42)));
        assert_eq!(metric.get_int(&metric_fields), Some(42));
    }

    #[test]
    fn test_set_float_metric_value() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        metric.set_value(Value::Float(2.71.into()), &metric_fields, clock.now());
        assert!(!metric.is_empty());
        assert_eq!(
            metric.get_value(&metric_fields),
            Some(Value::Float(2.71.into()))
        );
        assert_eq!(metric.get_float(&metric_fields), Some(2.71));
    }

    #[test]
    fn test_set_string_metric_value() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        metric.set_value(Value::Str("lorem".into()), &metric_fields, clock.now());
        assert!(!metric.is_empty());
        assert_eq!(
            metric.get_value(&metric_fields),
            Some(Value::Str("lorem".into()))
        );
        assert_eq!(metric.get_string(&metric_fields), Some("lorem".into()));
    }

    #[test]
    fn test_set_two_metric_values() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields1 = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(123)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        let metric_fields2 = FieldMap::from([
            ("lorem", FieldValue::Bool(false)),
            ("ipsum", FieldValue::Int(456)),
            ("dolor", FieldValue::Str("consectetur".into())),
        ]);
        metric.set_value(Value::Int(43), &metric_fields1, clock.now());
        metric.set_value(Value::Int(44), &metric_fields2, clock.now());
        assert!(!metric.is_empty());
        assert_eq!(metric.get_value(&metric_fields1), Some(Value::Int(43)));
        assert_eq!(metric.get_value(&metric_fields2), Some(Value::Int(44)));
        assert_eq!(metric.get_int(&metric_fields1), Some(43));
        assert_eq!(metric.get_int(&metric_fields2), Some(44));
    }

    #[test]
    fn test_update_metric_value() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields1 = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(123)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        let metric_fields2 = FieldMap::from([
            ("lorem", FieldValue::Bool(false)),
            ("ipsum", FieldValue::Int(456)),
            ("dolor", FieldValue::Str("consectetur".into())),
        ]);
        metric.set_value(Value::Int(43), &metric_fields1, clock.now());
        metric.set_value(Value::Int(44), &metric_fields2, clock.now());
        metric.set_value(Value::Int(45), &metric_fields1, clock.now());
        assert!(!metric.is_empty());
        assert_eq!(metric.get_value(&metric_fields1), Some(Value::Int(45)));
        assert_eq!(metric.get_value(&metric_fields2), Some(Value::Int(44)));
        assert_eq!(metric.get_int(&metric_fields1), Some(45));
        assert_eq!(metric.get_int(&metric_fields2), Some(44));
    }

    #[test]
    fn test_add_to_metric_int_no_fields() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        metric.add_to_int(42, &FieldMap::from([]), clock.now());
        assert!(!metric.is_empty());
        assert_eq!(metric.get_value(&FieldMap::from([])), Some(Value::Int(42)));
        assert_eq!(metric.get_int(&FieldMap::from([])), Some(42));
    }

    #[test]
    fn test_add_to_metric_int() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        metric.add_to_int(42, &metric_fields, clock.now());
        assert!(!metric.is_empty());
        assert_eq!(metric.get_value(&metric_fields), Some(Value::Int(42)));
        assert_eq!(metric.get_int(&metric_fields), Some(42));
    }

    #[test]
    fn test_add_to_two_metric_ints() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields1 = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(123)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        let metric_fields2 = FieldMap::from([
            ("lorem", FieldValue::Bool(false)),
            ("ipsum", FieldValue::Int(456)),
            ("dolor", FieldValue::Str("consectetur".into())),
        ]);
        metric.add_to_int(43, &metric_fields1, clock.now());
        metric.add_to_int(44, &metric_fields2, clock.now());
        assert!(!metric.is_empty());
        assert_eq!(metric.get_value(&metric_fields1), Some(Value::Int(43)));
        assert_eq!(metric.get_value(&metric_fields2), Some(Value::Int(44)));
        assert_eq!(metric.get_int(&metric_fields1), Some(43));
        assert_eq!(metric.get_int(&metric_fields2), Some(44));
    }

    #[test]
    fn test_delete_missing_metric_value_no_fields() {
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([]);
        metric.delete_value(&metric_fields);
        assert!(metric.is_empty());
        assert!(metric.get_value(&metric_fields).is_none());
        assert!(metric.get_bool(&metric_fields).is_none());
        assert!(metric.get_int(&metric_fields).is_none());
        assert!(metric.get_float(&metric_fields).is_none());
        assert!(metric.get_string(&metric_fields).is_none());
    }

    #[test]
    fn test_delete_missing_metric_value() {
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(123)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        metric.delete_value(&metric_fields);
        assert!(metric.is_empty());
        assert!(metric.get_value(&metric_fields).is_none());
        assert!(metric.get_bool(&metric_fields).is_none());
        assert!(metric.get_int(&metric_fields).is_none());
        assert!(metric.get_float(&metric_fields).is_none());
        assert!(metric.get_string(&metric_fields).is_none());
    }

    #[test]
    fn test_delete_metric_value_no_fields() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([]);
        metric.set_value(Value::Int(42), &metric_fields, clock.now());
        metric.delete_value(&metric_fields);
        assert!(metric.is_empty());
        assert!(metric.get_value(&metric_fields).is_none());
        assert!(metric.get_bool(&metric_fields).is_none());
        assert!(metric.get_int(&metric_fields).is_none());
        assert!(metric.get_float(&metric_fields).is_none());
        assert!(metric.get_string(&metric_fields).is_none());
    }

    #[test]
    fn test_delete_metric_value() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(123)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        metric.set_value(Value::Int(42), &metric_fields, clock.now());
        metric.delete_value(&metric_fields);
        assert!(metric.is_empty());
        assert!(metric.get_value(&metric_fields).is_none());
        assert!(metric.get_bool(&metric_fields).is_none());
        assert!(metric.get_int(&metric_fields).is_none());
        assert!(metric.get_float(&metric_fields).is_none());
        assert!(metric.get_string(&metric_fields).is_none());
    }

    #[test]
    fn test_delete_one_metric_value() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields1 = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(123)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        let metric_fields2 = FieldMap::from([
            ("lorem", FieldValue::Bool(false)),
            ("ipsum", FieldValue::Int(456)),
            ("dolor", FieldValue::Str("consectetur".into())),
        ]);
        metric.set_value(Value::Int(43), &metric_fields1, clock.now());
        metric.set_value(Value::Int(44), &metric_fields2, clock.now());
        metric.delete_value(&metric_fields1);
        assert!(!metric.is_empty());
        assert!(metric.get_value(&metric_fields1).is_none());
        assert!(metric.get_bool(&metric_fields1).is_none());
        assert!(metric.get_int(&metric_fields1).is_none());
        assert!(metric.get_float(&metric_fields1).is_none());
        assert!(metric.get_string(&metric_fields1).is_none());
        assert_eq!(metric.get_value(&metric_fields2), Some(Value::Int(44)));
        assert_eq!(metric.get_int(&metric_fields2), Some(44));
    }

    #[test]
    fn test_set_metric_value_again() {
        let clock = MockClock::default();
        let mut metric = Metric::new("/foo/bar".into());
        let metric_fields = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        metric.set_value(Value::Int(42), &metric_fields, clock.now());
        metric.delete_value(&metric_fields);
        metric.set_value(Value::Int(43), &metric_fields, clock.now());
        assert!(!metric.is_empty());
        assert_eq!(metric.get_value(&metric_fields), Some(Value::Int(43)));
        assert_eq!(metric.get_int(&metric_fields), Some(43));
    }

    // TODO
}
