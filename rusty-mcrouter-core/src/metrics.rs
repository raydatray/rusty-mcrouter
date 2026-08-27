use std::sync::Arc;

use rusty_mcrouter_config::{ConfigDocument, PoolId};
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

struct PoolMetricsLayout {
    id: PoolId,
    name: String,
}

pub struct RoutingMetricsLayout {
    pools: Vec<PoolMetricsLayout>,
}

impl RoutingMetricsLayout {
    pub fn new(config: &ConfigDocument) -> Arc<Self> {
        Arc::new(Self {
            pools: config
                .pools()
                .map(|(id, pool)| PoolMetricsLayout {
                    id,
                    name: pool.name().to_string(),
                })
                .collect(),
        })
    }

    pub fn empty() -> Arc<Self> {
        Arc::new(Self { pools: Vec::new() })
    }

    pub fn pool_name(&self, id: PoolId) -> Option<&str> {
        self.pools
            .get(id.index())
            .filter(|pool| pool.id == id)
            .map(|pool| pool.name.as_str())
    }

    pub fn pools_len(&self) -> usize {
        self.pools.len()
    }

    pub fn pools(&self) -> impl ExactSizeIterator<Item = (PoolId, &str)> {
        self.pools.iter().map(|pool| (pool.id, pool.name.as_str()))
    }
}

#[cfg(test)]
pub(crate) fn test_metrics_layout(names: &[&str]) -> Arc<RoutingMetricsLayout> {
    let pools = names
        .iter()
        .map(|name| {
            (
                (*name).to_string(),
                serde_json::json!({ "servers": [format!("{name}:1")] }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let config = rusty_mcrouter_config::parse(
        &serde_json::json!({ "pools": pools, "route": "NullRoute" }).to_string(),
    )
    .unwrap();
    RoutingMetricsLayout::new(&config)
}

#[cfg(test)]
pub(crate) fn test_pool_id(layout: &RoutingMetricsLayout, name: &str) -> PoolId {
    layout
        .pools()
        .find_map(|(id, candidate)| (candidate == name).then_some(id))
        .unwrap()
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
    pools: Vec<PoolMetrics>,
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

    pub fn pool(&self, id: PoolId) -> &PoolMetrics {
        &self.pools[id.index()]
    }

    pub fn pools(&self) -> impl ExactSizeIterator<Item = (PoolId, &PoolMetrics)> {
        self.layout
            .pools()
            .map(|(id, _)| (id, &self.pools[id.index()]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_mcrouter_config::parse;

    fn layout() -> Arc<RoutingMetricsLayout> {
        let config = parse(
            r#"{
                "pools": {
                    "primary": { "servers": ["primary:1"] },
                    "backup": { "servers": ["backup:1"] }
                },
                "route": "NullRoute"
            }"#,
        )
        .unwrap();
        RoutingMetricsLayout::new(&config)
    }

    #[test]
    fn layout_resolves_pool_ids_to_stable_indexes() {
        let config = parse(
            r#"{
                "pools": {
                    "primary": { "servers": ["primary:1"] },
                    "backup": { "servers": ["backup:1"] }
                },
                "route": "NullRoute"
            }"#,
        )
        .unwrap();
        let layout = RoutingMetricsLayout::new(&config);
        let primary = config.pool_id("primary").unwrap();
        let backup = config.pool_id("backup").unwrap();

        assert_eq!(layout.pool_name(primary), Some("primary"));
        assert_eq!(layout.pool_name(backup), Some("backup"));
    }

    #[test]
    fn shard_has_one_pool_block_per_layout_entry() {
        let layout = layout();
        let shard = RoutingMetricsShard::new(layout);
        assert_eq!(shard.pools().len(), 2);
    }

    #[test]
    fn distinct_shards_do_not_share_pool_counters() {
        let layout = layout();
        let pool = test_pool_id(&layout, "backup");
        let first = RoutingMetricsShard::new(Arc::clone(&layout));
        let second = RoutingMetricsShard::new(layout);
        first.pool(pool).requests.inc();

        assert_eq!(first.pool(pool).requests.load(), 1);
        assert_eq!(second.pool(pool).requests.load(), 0);
    }

    #[test]
    fn shard_is_cache_line_aligned() {
        assert!(std::mem::align_of::<RoutingMetricsShard>() >= 64);
    }
}
