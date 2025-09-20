use crate::proto;
use crate::utils::f64::F64;
use anyhow::{Result, anyhow};
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};

/// Determines the number and boundaries of the buckets of a `Distribution`.
///
/// A Bucketer is uniquely identified by four parameters: `width`, `growth_factor`, `scale_factor`,
/// and `num_finite_buckets`.
///
/// `num_finite_buckets` determines the number of buckets defined by the bucketer. The exclusive
/// upper bound of the i-th bucket, with `i` being a zero-based index, is calculated as:
///
///   width * (i + 1) + scale_factor * pow(growth_factor, i)
///
/// for any `growth_factor` != 0. If `growth_factor` is zero the upper bound is just
/// `width * (i + 1)`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bucketer {
    params: (F64, F64, F64, usize),
}

impl Bucketer {
    pub const MAX_NUM_FINITE_BUCKETS: usize = 5000;

    fn get(
        width: f64,
        growth_factor: f64,
        scale_factor: f64,
        num_finite_buckets: usize,
    ) -> &'static Self {
        assert!(num_finite_buckets <= Self::MAX_NUM_FINITE_BUCKETS);
        static BUCKETERS: LazyLock<Mutex<BTreeSet<Pin<Box<Bucketer>>>>> =
            LazyLock::new(|| Mutex::default());
        let params = (
            width.into(),
            growth_factor.into(),
            scale_factor.into(),
            num_finite_buckets,
        );
        let mut bucketers = BUCKETERS.lock().unwrap();
        if !bucketers.contains(&params) {
            bucketers.insert(Box::pin(Self { params }));
        }
        let bucketer = bucketers.get(&params).unwrap();
        let bucketer: &Self = bucketer.as_ref().get_ref();
        unsafe {
            // Transmuting extends the lifetime of the `bucketer` reference to `'static`. This is
            // safe here because bucketers are pinned and never removed from the bucketer set, and
            // the bucketer set is never dropped.
            std::mem::transmute(bucketer)
        }
    }

    pub fn fixed_width(width: f64, num_finite_buckets: usize) -> &'static Self {
        Self::get(width, 0.0, 1.0, num_finite_buckets)
    }

    pub fn scaled_powers_of(base: f64, scale_factor: f64, max: f64) -> &'static Self {
        let num_finite_buckets =
            std::cmp::max(1, 1 + (max / scale_factor).log(base).ceil() as usize);
        Self::get(0.0, base, scale_factor, num_finite_buckets)
    }

    pub fn powers_of(base: f64) -> &'static Self {
        Self::scaled_powers_of(base, 1.0, u32::MAX as f64)
    }

    pub fn default() -> &'static Self {
        Self::powers_of(4.0)
    }

    pub fn custom(
        width: f64,
        growth_factor: f64,
        scale_factor: f64,
        num_finite_buckets: usize,
    ) -> &'static Self {
        Self::get(width, growth_factor, scale_factor, num_finite_buckets)
    }

    pub fn none() -> &'static Self {
        Self::get(0.0, 0.0, 0.0, 0)
    }

    pub fn width(&self) -> f64 {
        let (width, _, _, _) = self.params;
        width.value
    }

    pub fn growth_factor(&self) -> f64 {
        let (_, growth_factor, _, _) = self.params;
        growth_factor.value
    }

    pub fn scale_factor(&self) -> f64 {
        let (_, _, scale_factor, _) = self.params;
        scale_factor.value
    }

    pub fn num_finite_buckets(&self) -> usize {
        let (_, _, _, num_finite_buckets) = self.params;
        num_finite_buckets
    }

    /// Returns the (inclusive) lower bound of the i-th bucket.
    ///
    /// NOTE: this function doesn't check that `i` is in the range `[0, num_finite_buckets)`, the
    /// caller has to do that.
    pub fn lower_bound(&self, i: isize) -> f64 {
        let i = i as f64;
        let mut result = self.width() * (i + 1.0);
        let growth_factor = self.growth_factor();
        if growth_factor != 0.0 {
            result += self.scale_factor() * growth_factor.powf(i);
        }
        result
    }

    /// Returns the (exclusive) upper bound of the i-th bucket.
    ///
    /// NOTE: this function doesn't check that `i` is in the range `[0, num_finite_buckets)`, the
    /// caller has to do that.
    pub fn upper_bound(&self, i: isize) -> f64 {
        self.lower_bound(i + 1)
    }

    /// Performs a binary search over the buckets and retrieves the one where `sample` falls. If the
    /// returned index is negative the sample falls in the underflow bucket, while if it's greater
    /// than or equal to `num_finite_buckets` it falls in the overflow bucket.
    pub fn get_bucket_for(&self, sample: f64) -> isize {
        let mut i = 0isize;
        let mut j = self.num_finite_buckets() as isize + 1;
        while j > i {
            let k = i + ((j - i) >> 1);
            let l = self.lower_bound(k - 1);
            if sample < l {
                j = k;
            } else if sample > l {
                i = k + 1;
            } else {
                return k;
            }
        }
        i - 1
    }

    /// Serializes the bucketer into a `proto::tsz::Bucketer` proto.
    pub fn encode(&self) -> proto::tsz::Bucketer {
        proto::tsz::Bucketer {
            width: Some(self.width()),
            growth_factor: Some(self.growth_factor()),
            scale_factor: Some(self.scale_factor()),
            num_finite_buckets: Some(self.num_finite_buckets() as u32),
        }
    }

    /// Deserializes a `proto::tsz::Bucketer` proto.
    pub fn decode(proto: &proto::tsz::Bucketer) -> Result<&'static Self> {
        let width = match proto.width {
            Some(width) => Ok(width),
            _ => Err(anyhow!("missing width field from bucketer")),
        }?;
        let growth_factor = match proto.growth_factor {
            Some(growth_factor) => Ok(growth_factor),
            _ => Err(anyhow!("missing growth_factor field from bucketer")),
        }?;
        let scale_factor = match proto.scale_factor {
            Some(scale_factor) => Ok(scale_factor),
            _ => Err(anyhow!("missing scale_factor field from bucketer")),
        }?;
        let num_finite_buckets = match proto.num_finite_buckets {
            Some(num_finite_buckets) => Ok(num_finite_buckets as usize),
            _ => Err(anyhow!("missing num_finite_buckets field from bucketer")),
        }?;
        Ok(Self::get(
            width,
            growth_factor,
            scale_factor,
            num_finite_buckets,
        ))
    }
}

impl Borrow<(F64, F64, F64, usize)> for Pin<Box<Bucketer>> {
    fn borrow(&self) -> &(F64, F64, F64, usize) {
        &self.params
    }
}

/// A smartpointer type that references a `Bucketer`.
///
/// The main purpose of this class is to provide fast bucketer comparison by comparing the
/// bucketers' memory addresses, which is sound because bucketers are canonical and are stored in a
/// static cache where they are pinned and from which are never removed (see the implementation of
/// `Bucketer::get`).
///
/// By encapsulating this type rather than a raw static reference, a struct can easily have
/// `PartialEq` and `Eq` derivations that compare the bucketers' memory addresses rather than their
/// contents. For example:
///
///   #[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
///   pub struct MetricConfig {
///     // ...
///     bucketer: Option<BucketerRef>,
///   }
///
/// `MetricConfig` comparisons compare bucketer memory addresses (if both bucketers are present).
///
/// `BucketerRef` also makes it easy to add a `Default` derivation, because it has `Default` itself
/// (yielding an instance that refers to the default `Bucketer`).
#[derive(Debug, Copy, Clone)]
pub struct BucketerRef {
    pub bucketer: &'static Bucketer,
}

impl Default for BucketerRef {
    fn default() -> Self {
        Self {
            bucketer: Bucketer::default(),
        }
    }
}

impl From<&'static Bucketer> for BucketerRef {
    fn from(bucketer: &'static Bucketer) -> Self {
        Self { bucketer }
    }
}

impl PartialEq for BucketerRef {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.bucketer, other.bucketer)
    }
}

impl Eq for BucketerRef {}

impl Deref for BucketerRef {
    type Target = Bucketer;

    fn deref(&self) -> &Self::Target {
        self.bucketer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_width() {
        let bucketer = Bucketer::fixed_width(1.0, 10);
        assert_eq!(bucketer.width(), 1.0);
        assert_eq!(bucketer.growth_factor(), 0.0);
        assert_eq!(bucketer.scale_factor(), 1.0);
        assert_eq!(bucketer.num_finite_buckets(), 10);
    }

    #[test]
    fn test_scaled_powers_of() {
        let bucketer = Bucketer::scaled_powers_of(2.0, 3.0, 100.0);
        assert_eq!(bucketer.width(), 0.0);
        assert_eq!(bucketer.growth_factor(), 2.0);
        assert_eq!(bucketer.scale_factor(), 3.0);
        assert_eq!(bucketer.num_finite_buckets(), 7);
    }

    #[test]
    fn test_powers_of() {
        let bucketer = Bucketer::powers_of(2.0);
        assert_eq!(bucketer.width(), 0.0);
        assert_eq!(bucketer.growth_factor(), 2.0);
        assert_eq!(bucketer.scale_factor(), 1.0);
        assert_eq!(bucketer.num_finite_buckets(), 33);
    }

    #[test]
    fn test_custom() {
        let bucketer = Bucketer::custom(1.0, 2.0, 0.5, 20);
        assert_eq!(bucketer.width(), 1.0);
        assert_eq!(bucketer.growth_factor(), 2.0);
        assert_eq!(bucketer.scale_factor(), 0.5);
        assert_eq!(bucketer.num_finite_buckets(), 20);
    }

    #[test]
    fn test_default() {
        assert_eq!(Bucketer::default(), Bucketer::powers_of(4.0));
    }

    #[test]
    fn test_none() {
        let bucketer = Bucketer::none();
        assert_eq!(bucketer.width(), 0.0);
        assert_eq!(bucketer.growth_factor(), 0.0);
        assert_eq!(bucketer.scale_factor(), 0.0);
        assert_eq!(bucketer.num_finite_buckets(), 0);
        assert_eq!(bucketer.get_bucket_for(-2.0), -1);
        assert_eq!(bucketer.get_bucket_for(-1.5), -1);
        assert_eq!(bucketer.get_bucket_for(-1.0), -1);
        assert_eq!(bucketer.get_bucket_for(-0.5), -1);
        assert_eq!(bucketer.get_bucket_for(0.0), 0);
        assert_eq!(bucketer.get_bucket_for(0.5), 0);
        assert_eq!(bucketer.get_bucket_for(1.0), 0);
        assert_eq!(bucketer.get_bucket_for(1.5), 0);
        assert_eq!(bucketer.get_bucket_for(2.0), 0);
    }

    #[test]
    fn test_underflow() {
        let bucketer = Bucketer::custom(1.0, 0.0, 1.0, 5);
        assert_eq!(bucketer.get_bucket_for(-0.1), -1);
        assert_eq!(bucketer.get_bucket_for(-1.0), -1);
        assert_eq!(bucketer.get_bucket_for(-1.5), -1);
        assert_eq!(bucketer.get_bucket_for(-2.0), -1);
    }

    #[test]
    fn test_buckets() {
        let bucketer = Bucketer::custom(1.0, 0.0, 1.0, 5);
        assert_eq!(bucketer.get_bucket_for(0.0), 0);
        assert_eq!(bucketer.get_bucket_for(0.5), 0);
        assert_eq!(bucketer.get_bucket_for(0.9), 0);
        assert_eq!(bucketer.get_bucket_for(1.0), 1);
        assert_eq!(bucketer.get_bucket_for(1.5), 1);
        assert_eq!(bucketer.get_bucket_for(1.9), 1);
        assert_eq!(bucketer.get_bucket_for(2.0), 2);
        assert_eq!(bucketer.get_bucket_for(2.5), 2);
        assert_eq!(bucketer.get_bucket_for(2.9), 2);
        assert_eq!(bucketer.get_bucket_for(3.0), 3);
        assert_eq!(bucketer.get_bucket_for(3.5), 3);
        assert_eq!(bucketer.get_bucket_for(3.9), 3);
        assert_eq!(bucketer.get_bucket_for(4.0), 4);
        assert_eq!(bucketer.get_bucket_for(4.5), 4);
        assert_eq!(bucketer.get_bucket_for(4.9), 4);
    }

    #[test]
    fn test_overflow() {
        let bucketer = Bucketer::custom(1.0, 0.0, 1.0, 5);
        assert_eq!(bucketer.get_bucket_for(5.0), 5);
        assert_eq!(bucketer.get_bucket_for(5.5), 5);
        assert_eq!(bucketer.get_bucket_for(6.0), 5);
        assert_eq!(bucketer.get_bucket_for(7.0), 5);
    }

    #[test]
    fn test_encode1() {
        let proto = Bucketer::default().encode();
        assert_eq!(proto.width, Some(0.0));
        assert_eq!(proto.growth_factor, Some(4.0));
        assert_eq!(proto.scale_factor, Some(1.0));
        assert_eq!(proto.num_finite_buckets, Some(17));
    }

    #[test]
    fn test_encode2() {
        let proto = Bucketer::custom(1.0, 2.0, 0.5, 20).encode();
        assert_eq!(proto.width, Some(1.0));
        assert_eq!(proto.growth_factor, Some(2.0));
        assert_eq!(proto.scale_factor, Some(0.5));
        assert_eq!(proto.num_finite_buckets, Some(20));
    }

    #[test]
    fn test_decode1() {
        let b1 = Bucketer::default();
        let proto = b1.encode();
        let b2 = Bucketer::decode(&proto).unwrap();
        assert!(std::ptr::eq(b1, b2));
    }

    #[test]
    fn test_decode2() {
        let b1 = Bucketer::custom(1.0, 2.0, 0.5, 20);
        let proto = b1.encode();
        let b2 = Bucketer::decode(&proto).unwrap();
        assert!(std::ptr::eq(b1, b2));
    }
}
