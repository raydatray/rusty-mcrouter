use std::sync::Arc;

use rusty_mcrouter_observability::metrics::MetricsText;
use rusty_mcrouter_observability::MetricsSource;
use rusty_mcrouter_observability_primitives::{Counter, Gauge};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ReloadStage {
    Read = 0,
    Parse,
    Validate,
    Apply,
}

impl ReloadStage {
    pub const COUNT: usize = 4;
    pub const ALL: [Self; Self::COUNT] = [Self::Read, Self::Parse, Self::Validate, Self::Apply];

    pub fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Parse => "parse",
            Self::Validate => "validate",
            Self::Apply => "apply",
        }
    }
}

#[derive(Default)]
pub struct ConfigMetrics {
    pub generation: Gauge,
    pub reload_attempts: Counter,
    pub reload_failures: [Counter; ReloadStage::COUNT],
    /// 1 when the config file on disk is the one running.
    pub last_reload_successful: Gauge,
    pub last_success_unix_secs: Gauge,
}

impl ConfigMetrics {
    pub fn started(applied_at: u64) -> Arc<Self> {
        let metrics = Self::default();
        metrics.applied(1, applied_at);
        Arc::new(metrics)
    }

    pub fn applied(&self, generation: u64, applied_at: u64) {
        self.generation.set(generation as i64);
        self.last_success_unix_secs.set(applied_at as i64);
        self.in_sync();
    }

    pub fn in_sync(&self) {
        self.last_reload_successful.set(1);
    }

    pub fn rejected(&self, stage: ReloadStage) {
        self.reload_failures[stage as usize].inc();
        self.last_reload_successful.set(0);
    }
}

/// Projection registered through observability's generic metric-source interface.
pub struct ConfigSource {
    pub metrics: Arc<ConfigMetrics>,
}

impl MetricsSource for ConfigSource {
    fn encode(&self, out: &mut MetricsText) {
        let metrics = &self.metrics;
        out.gauge(
            "rusty_mcrouter_config_generation",
            &[],
            metrics.generation.load(),
        );
        out.counter(
            "rusty_mcrouter_config_reload_attempts_total",
            &[],
            metrics.reload_attempts.load(),
        );
        for stage in ReloadStage::ALL {
            out.counter(
                "rusty_mcrouter_config_reload_failures_total",
                &[("stage", stage.label())],
                metrics.reload_failures[stage as usize].load(),
            );
        }
        out.gauge(
            "rusty_mcrouter_config_last_reload_successful",
            &[],
            metrics.last_reload_successful.load(),
        );
        out.gauge(
            "rusty_mcrouter_config_last_success_timestamp_seconds",
            &[],
            metrics.last_success_unix_secs.load(),
        );
    }
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_observability::MetricsRegistry;

    use super::*;

    #[test]
    fn config_source_golden() {
        let metrics = Arc::new(ConfigMetrics::default());
        metrics.applied(3, 1_700_000_000);
        metrics.reload_attempts.add(3);
        metrics.rejected(ReloadStage::Parse);
        let mut registry = MetricsRegistry::new();
        registry.register(Box::new(ConfigSource { metrics }));
        assert_eq!(
            registry.render(),
            "rusty_mcrouter_config_generation 3\n\
             rusty_mcrouter_config_reload_attempts_total 3\n\
             rusty_mcrouter_config_reload_failures_total{stage=\"read\"} 0\n\
             rusty_mcrouter_config_reload_failures_total{stage=\"parse\"} 1\n\
             rusty_mcrouter_config_reload_failures_total{stage=\"validate\"} 0\n\
             rusty_mcrouter_config_reload_failures_total{stage=\"apply\"} 0\n\
             rusty_mcrouter_config_last_reload_successful 0\n\
             rusty_mcrouter_config_last_success_timestamp_seconds 1700000000\n"
        );
    }

    #[test]
    fn config_metrics_return_to_in_sync() {
        let metrics = ConfigMetrics::started(1_700_000_000);
        assert_eq!(metrics.generation.load(), 1);
        assert_eq!(metrics.last_reload_successful.load(), 1);
        assert_eq!(metrics.last_success_unix_secs.load(), 1_700_000_000);
        metrics.rejected(ReloadStage::Read);
        assert_eq!(metrics.last_reload_successful.load(), 0);
        metrics.in_sync();
        assert_eq!(metrics.last_reload_successful.load(), 1);
        assert_eq!(
            metrics.generation.load(),
            1,
            "in sync is not a new generation"
        );
    }
}
