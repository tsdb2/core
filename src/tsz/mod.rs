use std::ops::Index;

mod exporter;

pub mod bucketer;
pub mod buffered;
pub mod config;
pub mod counter;
pub mod distribution;
pub mod event_metric;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum FieldValue {
    Bool(bool),
    Int(i64),
    Str(String),
}

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FieldMap {
    data: Vec<(String, FieldValue)>,
}

impl FieldMap {
    pub fn from<const N: usize>(entries: [(&str, FieldValue); N]) -> Self {
        let mut data = vec![];
        for (key, value) in entries {
            data.push((key.into(), value));
        }
        data.sort_unstable_by(
            |(lhs, _): &(String, FieldValue), (rhs, _): &(String, FieldValue)| lhs.cmp(rhs),
        );
        let mut i = 1;
        while i < data.len() {
            let (key1, _) = &data[i - 1];
            let (key2, _) = &data[i];
            if key1 == key2 {
                data.remove(i);
            } else {
                i += 1;
            }
        }
        Self { data }
    }
}

impl Index<&str> for FieldMap {
    type Output = FieldValue;

    fn index(&self, index: &str) -> &Self::Output {
        let mut i = 0;
        let mut j = self.data.len();
        while i < j {
            let k = i + ((j - i) >> 1);
            let (key, value) = &self.data[k];
            if index < key.as_str() {
                j = k;
            } else if index > key.as_str() {
                i = k + 1;
            } else {
                return value;
            }
        }
        panic!()
    }
}

pub async fn init() {
    crate::tsz::buffered::init().await;
}

#[cfg(test)]
pub mod testing {
    use crate::tsz::{FieldMap, FieldValue};
    use std::sync::{LazyLock, atomic::AtomicI64, atomic::Ordering};

    pub fn test_entity_labels() -> FieldMap {
        static IOTA: LazyLock<AtomicI64> = LazyLock::new(|| AtomicI64::from(42));
        FieldMap::from([
            ("sator", FieldValue::Str("arepo".into())),
            (
                "lorem",
                FieldValue::Int(IOTA.fetch_add(1, Ordering::Relaxed)),
            ),
        ])
    }

    pub fn test_metric_fields() -> FieldMap {
        static IOTA: LazyLock<AtomicI64> = LazyLock::new(|| AtomicI64::from(42));
        FieldMap::from([
            ("tenet", FieldValue::Bool(true)),
            (
                "opera",
                FieldValue::Int(IOTA.fetch_add(1, Ordering::Relaxed)),
            ),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entries() {
        let map = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        assert_eq!(map["lorem"], FieldValue::Bool(true));
        assert_eq!(map["ipsum"], FieldValue::Int(42));
        assert_eq!(map["dolor"], FieldValue::Str("amet".into()));
    }

    #[test]
    fn test_order() {
        let map1 = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        let map2 = FieldMap::from([
            ("ipsum", FieldValue::Int(42)),
            ("lorem", FieldValue::Bool(true)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        let map3 = FieldMap::from([
            ("dolor", FieldValue::Str("amet".into())),
            ("ipsum", FieldValue::Int(42)),
            ("lorem", FieldValue::Bool(true)),
        ]);
        assert_eq!(map1, map2);
        assert_eq!(map1, map3);
        assert_eq!(map2, map3);
    }

    #[test]
    fn test_not_equal() {
        let map1 = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        let map2 = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("dolor", FieldValue::Int(42)),
            ("ipsum", FieldValue::Str("amet".into())),
        ]);
        let map3 = FieldMap::from([
            ("ipsum", FieldValue::Bool(true)),
            ("dolor", FieldValue::Int(42)),
            ("lorem", FieldValue::Str("amet".into())),
        ]);
        assert_ne!(map1, map2);
        assert_ne!(map1, map3);
        assert_ne!(map2, map3);
    }

    #[test]
    fn test_duplicates() {
        let map = FieldMap::from([
            ("lorem", FieldValue::Bool(true)),
            ("ipsum", FieldValue::Int(42)),
            ("lorem", FieldValue::Int(123)),
            ("dolor", FieldValue::Str("amet".into())),
        ]);
        assert!(map["lorem"] == FieldValue::Bool(true) || map["lorem"] == FieldValue::Int(123));
        assert_eq!(map["ipsum"], FieldValue::Int(42));
        assert_eq!(map["dolor"], FieldValue::Str("amet".into()));
    }
}
