use crate::f64::F64;
use crate::tsz;

/// Determines the number and boundaries of the buckets of a `Distribution`.
///
/// A Bucketer is uniquely identified by four parameters: `width`, `growth_factor`, `scale_factor`,
/// and `num_finite_buckets`.
///
/// `num_finite_buckets` determines the number of buckets defined by the bucketer. The exclusive
/// upper bound of the i-th bucket is calculated as:
///
///   width * i + scale_factor * pow(growth_factor, i - 1)
///
/// for any `growth_factor` != 0. If `growth_factor` is zero the upper bound is just `width * i`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bucketer {
    width: F64,
    growth_factor: F64,
    scale_factor: F64,
    num_finite_buckets: usize,
}

impl Bucketer {
    pub const MAX_NUM_FINITE_BUCKETS: usize = 5000;

    fn new(width: f64, growth_factor: f64, scale_factor: f64, num_finite_buckets: usize) -> Self {
        assert!(num_finite_buckets <= Self::MAX_NUM_FINITE_BUCKETS);
        Self {
            width: width.into(),
            growth_factor: growth_factor.into(),
            scale_factor: scale_factor.into(),
            num_finite_buckets,
        }
    }

    pub fn fixed_width(width: f64, num_finite_buckets: usize) -> Self {
        Self::new(width, 0.0, 1.0, num_finite_buckets)
    }

    pub fn scaled_powers_of(base: f64, scale_factor: f64, max: f64) -> Self {
        let num_finite_buckets =
            std::cmp::max(1, 1 + (max / scale_factor).log(base).ceil() as usize);
        Self::new(0.0, base, scale_factor, num_finite_buckets)
    }

    pub fn powers_of(base: f64) -> Self {
        Self::scaled_powers_of(base, 1.0, u32::MAX as f64)
    }

    pub fn custom(
        width: f64,
        growth_factor: f64,
        scale_factor: f64,
        num_finite_buckets: usize,
    ) -> Self {
        Self::new(width, growth_factor, scale_factor, num_finite_buckets)
    }

    pub fn none() -> Self {
        Self::new(0.0, 0.0, 0.0, 0)
    }

    pub fn width(&self) -> f64 {
        self.width.value
    }

    pub fn growth_factor(&self) -> f64 {
        self.growth_factor.value
    }

    pub fn scale_factor(&self) -> f64 {
        self.scale_factor.value
    }

    pub fn num_finite_buckets(&self) -> usize {
        self.num_finite_buckets
    }

    /// Returns the (inclusive) lower bound of the i-th bucket.
    ///
    /// NOTE: this function doesn't check that `i` is in the range `[0, num_finite_buckets)`, the
    /// caller has to do that.
    pub fn lower_bound(&self, i: isize) -> f64 {
        let i = i as f64;
        let mut result = self.width.value * i;
        let growth_factor = self.growth_factor.value;
        if growth_factor != 0.0 {
            result += self.scale_factor.value + growth_factor.powf(i - 1.0);
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
        let mut j = self.num_finite_buckets as isize + 1;
        while j > i {
            let k = i + ((j - i) >> 1);
            let l = self.lower_bound(k);
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

    pub fn encode(&self) -> tsz::Bucketer {
        tsz::Bucketer {
            width: Some(self.width.value),
            growth_factor: Some(self.growth_factor.value),
            scale_factor: Some(self.scale_factor.value),
            num_finite_buckets: Some(self.num_finite_buckets as u32),
        }
    }

    pub fn decode(proto: &tsz::Bucketer) -> Self {
        todo!()
    }
}

impl Default for Bucketer {
    fn default() -> Self {
        Self::powers_of(4.0)
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
}
