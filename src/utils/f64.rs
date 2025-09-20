/// A fully comparable 64-bit floating point type for internal use in tsz.
#[derive(Debug, Default, Copy, Clone, PartialEq, PartialOrd)]
pub struct F64 {
    pub value: f64,
}

impl Eq for F64 {}

impl Ord for F64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value
            .partial_cmp(&other.value)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl From<f64> for F64 {
    fn from(value: f64) -> Self {
        assert!(value.is_finite());
        Self { value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        assert_eq!(F64::default().value, 0f64);
        assert_eq!(F64::default(), 0f64.into());
    }

    #[test]
    fn test_from() {
        assert_eq!(F64::from(42f64).value, 42f64);
        assert_eq!(F64::from(42f64), 42f64.into());
    }

    #[test]
    fn test_eq() {
        assert_eq!(F64::from(123.0), F64::from(123.0));
        assert_ne!(F64::from(123.0), F64::from(456.0));
    }

    #[test]
    fn test_cmp() {
        assert!(F64::from(123.0) < F64::from(456.0));
        assert!(F64::from(123.0) < F64::from(789.0));
        assert!(F64::from(456.0) < F64::from(789.0));
    }
}
