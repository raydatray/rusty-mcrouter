use std::sync::Arc;

use rusty_mcrouter_observability_primitives::Counter;

pub const FAILOVER_POLICY_COUNT: usize = 2;
pub const FAILOVER_ERROR_CLASS_COUNT: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FailoverPolicyKind {
    InOrder = 0,
    LeastFailures,
}

impl FailoverPolicyKind {
    pub const ALL: [Self; FAILOVER_POLICY_COUNT] = [Self::InOrder, Self::LeastFailures];

    pub fn prometheus_label(self) -> &'static str {
        match self {
            Self::InOrder => "inorder",
            Self::LeastFailures => "least_failures",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FailoverErrorClass {
    Result = 0,
    Tko,
}

impl FailoverErrorClass {
    pub const ALL: [Self; FAILOVER_ERROR_CLASS_COUNT] = [Self::Result, Self::Tko];

    pub fn prometheus_label(self) -> &'static str {
        match self {
            Self::Result => "result",
            Self::Tko => "tko",
        }
    }
}

pub struct RoutingMetricsLayout {
    pool_names: Vec<String>,
}

impl RoutingMetricsLayout {
    pub fn new(pool_names: impl IntoIterator<Item = String>) -> Arc<Self> {
        Arc::new(Self {
            pool_names: pool_names.into_iter().collect(),
        })
    }

    // used by route builder - not on hot path when routing
    pub fn pool_metrics_index(&self, name: &str) -> Option<usize> {
        self.pool_names
            .iter()
            .position(|candidate| candidate == name)
    }

    pub fn pool_name(&self, index: usize) -> Option<&str> {
        self.pool_names.get(index).map(String::as_str)
    }

    pub fn pools_len(&self) -> usize {
        self.pool_names.len()
    }

    pub fn pool_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.pool_names.iter().map(String::as_str)
    }
}

#[derive(Default)]
pub struct PoolMetrics {
    pub requests: Counter,
    pub duration_us_sum: Counter,
    pub completed_requests: Counter,
    pub final_errors: Counter,
    pub total_duration_us_sum: Counter,
}

#[repr(align(64))]
pub struct RoutingMetricsShard {
    layout: Arc<RoutingMetricsLayout>,
    pub pools: Vec<PoolMetrics>,
    pub dev_null_requests: Counter,
    pub failover: [Counter; FAILOVER_POLICY_COUNT],
    pub failover_exhausted: [Counter; FAILOVER_POLICY_COUNT],
    pub failover_policy_errors: [Counter; FAILOVER_ERROR_CLASS_COUNT],
}

impl RoutingMetricsShard {
    pub fn new(layout: Arc<RoutingMetricsLayout>) -> Arc<Self> {
        let pools = (0..layout.pools_len())
            .map(|_| PoolMetrics::default())
            .collect();

        Arc::new(Self {
            layout,
            pools,
            dev_null_requests: Counter::default(),
            failover: Default::default(),
            failover_exhausted: Default::default(),
            failover_policy_errors: Default::default(),
        })
    }

    pub fn layout(&self) -> &Arc<RoutingMetricsLayout> {
        &self.layout
    }

    pub fn pool(&self, index: usize) -> Option<&PoolMetrics> {
        self.pools.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_resolves_pool_names_to_stable_indexes() {
        let layout = RoutingMetricsLayout::new(["primary".to_string(), "backup".to_string()]);

        assert_eq!(layout.pool_metrics_index("primary"), Some(0));
        assert_eq!(layout.pool_metrics_index("backup"), Some(1));
        assert_eq!(layout.pool_metrics_index("missing"), None);
        assert_eq!(layout.pool_name(1), Some("backup"));
    }

    #[test]
    fn shard_has_one_pool_block_per_layout_entry() {
        let layout = RoutingMetricsLayout::new(["a".to_string(), "b".to_string()]);
        let shard = RoutingMetricsShard::new(layout);
        assert_eq!(shard.pools.len(), 2);
    }

    #[test]
    fn distinct_shards_do_not_share_pool_counters() {
        let layout = RoutingMetricsLayout::new(["pool".to_string()]);
        let first = RoutingMetricsShard::new(Arc::clone(&layout));
        let second = RoutingMetricsShard::new(layout);
        first.pools[0].requests.inc();

        assert_eq!(first.pools[0].requests.load(), 1);
        assert_eq!(second.pools[0].requests.load(), 0);
    }

    #[test]
    fn shard_is_cache_line_aligned() {
        assert!(std::mem::align_of::<RoutingMetricsShard>() >= 64);
    }
}
