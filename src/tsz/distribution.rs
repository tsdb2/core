use crate::tsz::{bucketer::Bucketer, bucketer::BucketerRef};
use anyhow::{Result, anyhow};

/// Manages a histogram of sample frequencies. The histogram is conceptually an array of buckets,
/// each bucket being an unsigned integer representing the number of samples in that bucket. The
/// number and boundaries of the buckets are determined by a `Bucketer`.
///
/// Bucketers define a finite number of buckets, but `Distribution` objects keep two extra implicit
/// buckets: samples that fall below the lowest bucket are recorded in an underflow bucket, and
/// samples falling above the highest are recorded in an overflow bucket.
///
/// Distributions also keep track of a few stats related to the recorded samples, namely: their sum,
/// count, mean, and sum of squared deviations from the mean. The latter is used to calculate the
/// mean with the least loss of precision thanks to the method of provisional means (see
/// http://www.pmean.com/04/ProvisionalMeans.html for more info).
#[derive(Debug, Clone)]
pub struct Distribution {
    bucketer: BucketerRef,
    buckets: Vec<usize>,
    underflow: usize,
    overflow: usize,
    count: usize,
    sum: f64,
    mean: f64,
    ssd: f64,
}

impl Distribution {
    pub fn new(bucketer: BucketerRef) -> Self {
        Self {
            bucketer,
            buckets: vec![0usize; bucketer.num_finite_buckets()],
            underflow: 0,
            overflow: 0,
            count: 0,
            sum: 0.0,
            mean: 0.0,
            ssd: 0.0,
        }
    }

    /// Returns the bucketer associated to this distribution.
    pub fn bucketer(&self) -> BucketerRef {
        self.bucketer
    }

    /// Returns the number of buckets. Equivalent to `bucketer().num_finite_buckets()`.
    pub fn num_finite_buckets(&self) -> usize {
        self.bucketer.num_finite_buckets()
    }

    /// Returns the number of samples in the i-th finite bucket. Panics if i is greater than or
    /// equal to `num_finite_buckets`.
    pub fn bucket(&self, i: usize) -> usize {
        self.buckets[i]
    }

    /// Returns the number of samples in the underflow bucket.
    pub fn underflow(&self) -> usize {
        self.underflow
    }

    /// Returns the number of samples in the overflow bucket.
    pub fn overflow(&self) -> usize {
        self.overflow
    }

    /// Returns the sum of all samples.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Returns the sum of the squared deviations from the mean. `Distribution` keeps track of this
    /// information in order to calculate the mean, variance, and standard deviation.
    pub fn sum_of_squared_deviations(&self) -> f64 {
        self.ssd
    }

    /// Returns the number of samples, including the ones in the underflow and overflow buckets.
    pub fn count(&self) -> usize {
        self.count
    }

    /// True iff there are no samples (i.e. `count() == 0`).
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    pub fn variance(&self) -> f64 {
        self.ssd / (self.count as f64)
    }

    pub fn stddev(&self) -> f64 {
        self.variance().sqrt()
    }

    /// Records a sample in the corresponding bucket.
    pub fn record(&mut self, sample: f64) {
        self.record_many(sample, 1);
    }

    /// Records a sample `times` times.
    pub fn record_many(&mut self, sample: f64, times: usize) {
        let bucket = self.bucketer.get_bucket_for(sample);
        self.record_to_bucket(sample, bucket, times);
    }

    /// Records a sample `times` times, forcing it to the specified bucket.
    ///
    /// WARNING: the `bucket` parameter MUST be the index returned by
    /// `bucketer.get_bucket_for(sample)`, otherwise the distribution will start giving incorrect
    /// stats.
    pub fn record_to_bucket(&mut self, sample: f64, bucket: isize, times: usize) {
        if bucket < 0 {
            self.underflow += times;
        } else {
            let i = bucket as usize;
            if i >= self.num_finite_buckets() {
                self.overflow += times;
            } else {
                self.buckets[i] += times;
            }
        }
        self.count += times;
        self.sum += sample * (times as f64);
        let dev = (times as f64) * (sample - self.mean);
        let new_mean = self.mean + dev / (self.count as f64);
        self.ssd += dev * (sample - new_mean);
        self.mean = new_mean;
    }

    /// Adds `other` to this distribution. The two distributions must have the same bucketer,
    /// otherwise the operation will fail with an error status.
    pub fn add(&mut self, other: &Self) -> Result<()> {
        if self.bucketer != other.bucketer {
            return Err(anyhow!("incompatible bucketers"));
        }
        for i in 0..self.num_finite_buckets() {
            self.buckets[i] += other.buckets[i];
        }
        self.underflow += other.underflow;
        self.overflow += other.overflow;
        let old_count = self.count;
        self.count += other.count;
        self.sum += other.sum;
        let old_mean = self.mean;
        if self.count > 0 {
            self.mean = self.sum / (self.count as f64);
        } else {
            self.mean = 0.0;
        }
        let square = (self.mean - old_mean) * (self.mean - other.mean);
        self.ssd += other.ssd + (old_count as f64) * square + (other.count as f64) * square;
        Ok(())
    }

    /// Resets all state to an empty distribution.
    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            *bucket = 0;
        }
        self.underflow = 0;
        self.overflow = 0;
        self.count = 0;
        self.sum = 0.0;
        self.mean = 0.0;
        self.ssd = 0.0;
    }
}

impl Default for Distribution {
    fn default() -> Self {
        Self::new(Bucketer::default().into())
    }
}

impl PartialEq for Distribution {
    fn eq(&self, other: &Self) -> bool {
        self.bucketer == other.bucketer
            && self.buckets == other.buckets
            && self.underflow == other.underflow
            && self.overflow == other.overflow
    }
}

impl Eq for Distribution {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucketer() {
        let bucketer: BucketerRef = Bucketer::custom(1.0, 2.0, 0.5, 20).into();
        let d = Distribution::new(bucketer);
        assert_eq!(d.bucketer(), bucketer);
        assert_eq!(d.num_finite_buckets(), bucketer.num_finite_buckets());
    }

    #[test]
    fn test_default_bucketer() {
        let d = Distribution::default();
        let bucketer: BucketerRef = Bucketer::default().into();
        assert_eq!(d.bucketer(), bucketer);
        assert_eq!(d.num_finite_buckets(), bucketer.num_finite_buckets());
    }

    #[test]
    fn test_initial_state() {
        let d = Distribution::default();
        for i in 0..d.num_finite_buckets() {
            assert_eq!(d.bucket(i), 0);
        }
        assert_eq!(d.underflow(), 0);
        assert_eq!(d.overflow(), 0);
        assert_eq!(d.count(), 0);
        assert_eq!(d.sum(), 0.0);
        assert_eq!(d.mean(), 0.0);
        assert_eq!(d.sum_of_squared_deviations(), 0.0);
        assert!(d.is_empty());
    }

    #[test]
    fn test_record_one_sample() {
        let mut d = Distribution::default();
        d.record(42.0);
        assert_eq!(d.bucket(3), 1);
        assert_eq!(d.sum(), 42.0);
        assert_eq!(d.sum_of_squared_deviations(), 0.0);
        assert_eq!(d.count(), 1);
        assert!(!d.is_empty());
        assert_eq!(d.mean(), 42.0);
    }

    #[test]
    fn test_record_two_samples() {
        let mut d = Distribution::default();
        d.record(1.0);
        d.record(5.0);
        assert_eq!(d.bucket(1), 1);
        assert_eq!(d.bucket(2), 1);
        assert_eq!(d.sum(), 6.0);
        assert_eq!(d.sum_of_squared_deviations(), 8.0);
        assert_eq!(d.count(), 2);
        assert!(!d.is_empty());
        assert_eq!(d.mean(), 3.0);
    }

    #[test]
    fn test_record_one_sample_many_times() {
        let mut d = Distribution::default();
        d.record(1.0);
        d.record_many(5.0, 3);
        assert_eq!(d.bucket(1), 1);
        assert_eq!(d.bucket(2), 3);
        assert_eq!(d.sum(), 16.0);
        assert_eq!(d.sum_of_squared_deviations(), 12.0);
        assert_eq!(d.count(), 4);
        assert!(!d.is_empty());
        assert_eq!(d.mean(), 4.0);
    }

    #[test]
    fn test_add_empty_to_empty() {
        let mut d1 = Distribution::default();
        let d2 = Distribution::default();
        assert!(d1.add(&d2).is_ok());
        assert_eq!(
            d1.num_finite_buckets(),
            Bucketer::default().num_finite_buckets()
        );
        for i in 0..d1.num_finite_buckets() {
            assert_eq!(d1.bucket(i), 0);
        }
        assert_eq!(d1.underflow(), 0);
        assert_eq!(d1.overflow(), 0);
        assert_eq!(d1.count(), 0);
        assert_eq!(d1.sum(), 0.0);
        assert_eq!(d1.mean(), 0.0);
        assert_eq!(d1.sum_of_squared_deviations(), 0.0);
    }

    #[test]
    fn test_add_empty() {
        let mut d1 = Distribution::default();
        d1.record(2.0);
        d1.record(4.0);
        d1.record(6.0);
        d1.record(8.0);
        d1.record(10.0);
        let d2 = Distribution::default();
        assert!(d1.add(&d2).is_ok());
        assert_eq!(
            d1.num_finite_buckets(),
            Bucketer::default().num_finite_buckets()
        );
        assert_eq!(d1.bucket(0), 0);
        assert_eq!(d1.bucket(1), 1);
        assert_eq!(d1.bucket(2), 4);
        for i in 3..d1.num_finite_buckets() {
            assert_eq!(d1.bucket(i), 0);
        }
        assert_eq!(d1.sum(), 30.0);
        assert_eq!(d1.sum_of_squared_deviations(), 40.0);
        assert_eq!(d1.count(), 5);
        assert!(!d1.is_empty());
        assert_eq!(d1.mean(), 6.0);
    }

    #[test]
    fn test_add_to_empty() {
        let mut d1 = Distribution::default();
        let mut d2 = Distribution::default();
        d2.record(2.0);
        d2.record(4.0);
        d2.record(6.0);
        d2.record(8.0);
        d2.record(10.0);
        assert!(d1.add(&d2).is_ok());
        assert_eq!(
            d1.num_finite_buckets(),
            Bucketer::default().num_finite_buckets()
        );
        assert_eq!(d1.bucket(0), 0);
        assert_eq!(d1.bucket(1), 1);
        assert_eq!(d1.bucket(2), 4);
        for i in 3..d1.num_finite_buckets() {
            assert_eq!(d1.bucket(i), 0);
        }
        assert_eq!(d1.sum(), 30.0);
        assert_eq!(d1.sum_of_squared_deviations(), 40.0);
        assert_eq!(d1.count(), 5);
        assert!(!d1.is_empty());
        assert_eq!(d1.mean(), 6.0);
    }

    #[test]
    fn test_add() {
        let mut d1 = Distribution::default();
        d1.record(2.0);
        d1.record(4.0);
        d1.record(6.0);
        d1.record(8.0);
        d1.record(10.0);
        let mut d2 = Distribution::default();
        d2.record(1.0);
        d2.record(3.0);
        d2.record(5.0);
        d2.record(7.0);
        d2.record(9.0);
        d2.record(11.0);
        assert!(d1.add(&d2).is_ok());
        assert_eq!(
            d1.num_finite_buckets(),
            Bucketer::default().num_finite_buckets()
        );
        assert_eq!(d1.bucket(0), 0);
        assert_eq!(d1.bucket(1), 3);
        assert_eq!(d1.bucket(2), 8);
        for i in 3..d1.num_finite_buckets() {
            assert_eq!(d1.bucket(i), 0);
        }
        assert_eq!(d1.sum(), 66.0);
        assert_eq!(d1.sum_of_squared_deviations(), 110.0);
        assert_eq!(d1.count(), 11);
        assert!(!d1.is_empty());
        assert_eq!(d1.mean(), 6.0);
    }

    #[test]
    fn test_clear() {
        let mut d = Distribution::default();
        d.record(1.0);
        d.record(5.0);
        d.clear();
        assert_eq!(d.bucket(1), 0);
        assert_eq!(d.bucket(2), 0);
        assert_eq!(d.sum(), 0.0);
        assert_eq!(d.sum_of_squared_deviations(), 0.0);
        assert_eq!(d.count(), 0);
        assert!(d.is_empty());
    }

    #[test]
    fn test_record_after_clearing() {
        let mut d = Distribution::default();
        d.record(1.0);
        d.record(5.0);
        d.clear();
        d.record(42.0);
        assert_eq!(d.bucket(3), 1);
        assert_eq!(d.sum(), 42.0);
        assert_eq!(d.sum_of_squared_deviations(), 0.0);
        assert_eq!(d.count(), 1);
        assert!(!d.is_empty());
        assert_eq!(d.mean(), 42.0);
    }
}
