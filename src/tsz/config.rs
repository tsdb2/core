use crate::tsz::{bucketer::Bucketer, bucketer::BucketerRef};

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub struct MetricConfig {
    pub cumulative: bool,
    pub skip_stable_cells: bool,
    pub delta_mode: bool,
    pub user_timestamps: bool,
    pub bucketer: Option<BucketerRef>,
}

impl MetricConfig {
    pub fn set_cumulative(mut self, value: bool) -> Self {
        self.cumulative = value;
        self
    }

    pub fn set_skip_stable_cells(mut self, value: bool) -> Self {
        self.skip_stable_cells = value;
        self
    }

    pub fn set_delta_mode(mut self, value: bool) -> Self {
        self.delta_mode = value;
        self
    }

    pub fn set_user_timestamps(mut self, value: bool) -> Self {
        self.user_timestamps = value;
        self
    }

    pub fn set_bucketer(mut self, bucketer: &'static Bucketer) -> Self {
        self.bucketer = Some(bucketer.into());
        self
    }

    pub fn clear_bucketer(mut self) -> Self {
        self.bucketer = None;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MetricConfig::default();
        assert_eq!(config.cumulative, false);
        assert_eq!(config.skip_stable_cells, false);
        assert_eq!(config.delta_mode, false);
        assert_eq!(config.user_timestamps, false);
        assert!(config.bucketer.is_none());
    }

    #[test]
    fn test_cumulative_field() {
        let config = MetricConfig::default().set_cumulative(true);
        assert_eq!(config.cumulative, true);
        assert_eq!(config.skip_stable_cells, false);
        assert_eq!(config.delta_mode, false);
        assert_eq!(config.user_timestamps, false);
        assert!(config.bucketer.is_none());
    }

    #[test]
    fn test_skip_stable_cells_field() {
        let config = MetricConfig::default().set_skip_stable_cells(true);
        assert_eq!(config.cumulative, false);
        assert_eq!(config.skip_stable_cells, true);
        assert_eq!(config.delta_mode, false);
        assert_eq!(config.user_timestamps, false);
        assert!(config.bucketer.is_none());
    }

    #[test]
    fn test_delta_mode_field() {
        let config = MetricConfig::default().set_delta_mode(true);
        assert_eq!(config.cumulative, false);
        assert_eq!(config.skip_stable_cells, false);
        assert_eq!(config.delta_mode, true);
        assert_eq!(config.user_timestamps, false);
        assert!(config.bucketer.is_none());
    }

    #[test]
    fn test_user_timestamps_field() {
        let config = MetricConfig::default().set_user_timestamps(true);
        assert_eq!(config.cumulative, false);
        assert_eq!(config.skip_stable_cells, false);
        assert_eq!(config.delta_mode, false);
        assert_eq!(config.user_timestamps, true);
        assert!(config.bucketer.is_none());
    }

    #[test]
    fn test_set_bucketer() {
        let config = MetricConfig::default().set_bucketer(Bucketer::default());
        assert_eq!(config.cumulative, false);
        assert_eq!(config.skip_stable_cells, false);
        assert_eq!(config.delta_mode, false);
        assert_eq!(config.user_timestamps, false);
        assert_eq!(config.bucketer, Some(BucketerRef::default()));
    }

    #[test]
    fn test_clear_bucketer() {
        let config = MetricConfig::default()
            .set_bucketer(Bucketer::default())
            .clear_bucketer();
        assert_eq!(config.cumulative, false);
        assert_eq!(config.skip_stable_cells, false);
        assert_eq!(config.delta_mode, false);
        assert_eq!(config.user_timestamps, false);
        assert!(config.bucketer.is_none());
    }
}
