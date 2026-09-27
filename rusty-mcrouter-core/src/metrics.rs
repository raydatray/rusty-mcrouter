use std::collections::BTreeMap;
use std::ops::Index;
use std::sync::{Arc, Mutex, Weak};

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

#[derive(Default)]
pub struct PoolMetrics {
    pub requests: Counter,
    pub duration_us_sum: Counter,
    pub completed_requests: Counter,
    pub final_errors: Counter,
    pub total_duration_us_sum: Counter,
}

/// One config's pool blocks, indexed by that config's `PoolId`. PoolIds
/// renumber across reloads; the blocks are shared by pool name.
pub struct PoolMetricsTable {
    pools: Vec<Arc<PoolMetrics>>,
}

impl Index<PoolId> for PoolMetricsTable {
    type Output = PoolMetrics;

    fn index(&self, pool: PoolId) -> &PoolMetrics {
        &self.pools[pool.index()]
    }
}

#[repr(align(64))]
pub struct RoutingMetricsShard {
    // locked at generation build and at scrape, never per request. weak, so a
    // removed pool leaves the scrape with the last generation naming it
    pools: Mutex<BTreeMap<Arc<str>, Weak<PoolMetrics>>>,
    pub dev_null_requests: Counter,
    pub failover: [Counter; FAILOVER_POLICY_COUNT],
    pub failover_exhausted: [Counter; FAILOVER_POLICY_COUNT],
    pub failover_policy_errors: [Counter; FAILOVER_ERROR_CLASS_COUNT],
}

impl RoutingMetricsShard {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            pools: Mutex::new(BTreeMap::new()),
            dev_null_requests: Counter::default(),
            failover: Default::default(),
            failover_exhausted: Default::default(),
            failover_policy_errors: Default::default(),
        })
    }

    /// A pool that survives a reload gets its live block, so its counters
    /// continue instead of resetting.
    pub fn table_for(&self, config: &ConfigDocument) -> PoolMetricsTable {
        let mut registry = self.pools.lock().unwrap();
        registry.retain(|_, block| block.strong_count() > 0);

        let pools = config
            .pools()
            .enumerate()
            .map(|(position, (id, pool))| {
                debug_assert_eq!(id.index(), position, "tables index by PoolId");
                if let Some(live) = registry.get(pool.name()).and_then(Weak::upgrade) {
                    return live;
                }
                let block = Arc::new(PoolMetrics::default());
                registry.insert(Arc::from(pool.name()), Arc::downgrade(&block));
                block
            })
            .collect();

        PoolMetricsTable { pools }
    }

    pub fn pool_blocks(&self) -> Vec<(Arc<str>, Arc<PoolMetrics>)> {
        let registry = self.pools.lock().unwrap();
        registry
            .iter()
            .filter_map(|(name, block)| Some((Arc::clone(name), block.upgrade()?)))
            .collect()
    }
}

#[cfg(test)]
pub(crate) fn test_config(names: &[&str]) -> ConfigDocument {
    let pools = names
        .iter()
        .map(|name| {
            (
                (*name).to_string(),
                serde_json::json!({ "servers": [format!("{name}:1")] }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    rusty_mcrouter_config::parse(
        &serde_json::json!({ "pools": pools, "route": "NullRoute" }).to_string(),
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_resolves_this_configs_pool_ids() {
        let config = test_config(&["primary", "backup"]);
        let primary = config.pool_id("primary").unwrap();
        let backup = config.pool_id("backup").unwrap();
        let table = RoutingMetricsShard::new().table_for(&config);

        table[primary].requests.inc();

        assert_eq!(table[primary].requests.load(), 1);
        assert_eq!(table[backup].requests.load(), 0);
    }

    #[test]
    fn table_covers_every_configured_pool() {
        let shard = RoutingMetricsShard::new();
        let _table = shard.table_for(&test_config(&["primary", "backup"]));

        let names: Vec<_> = shard
            .pool_blocks()
            .into_iter()
            .map(|(name, _)| name.to_string())
            .collect();
        assert_eq!(names, ["backup", "primary"]);
    }

    #[test]
    fn distinct_shards_do_not_share_pool_counters() {
        let config = test_config(&["primary", "backup"]);
        let pool = config.pool_id("backup").unwrap();
        let first = RoutingMetricsShard::new().table_for(&config);
        let second = RoutingMetricsShard::new().table_for(&config);
        first[pool].requests.inc();

        assert_eq!(first[pool].requests.load(), 1);
        assert_eq!(second[pool].requests.load(), 0);
    }

    #[test]
    fn surviving_pool_keeps_its_block_when_its_pool_id_changes() {
        let shard = RoutingMetricsShard::new();
        let v1 = test_config(&["users"]);
        let v2 = test_config(&["aaa", "users"]);
        let (old_id, new_id) = (v1.pool_id("users").unwrap(), v2.pool_id("users").unwrap());
        assert_ne!(old_id, new_id);

        let old = shard.table_for(&v1);
        old[old_id].requests.add(5);
        let new = shard.table_for(&v2);
        // an old-generation request still in flight writes the same block
        old[old_id].requests.inc();
        drop(old);

        assert_eq!(new[new_id].requests.load(), 6);
    }

    #[test]
    fn removed_pool_leaves_the_scrape_when_its_last_table_drops() {
        let shard = RoutingMetricsShard::new();
        let old = shard.table_for(&test_config(&["kept", "removed"]));
        let new = shard.table_for(&test_config(&["kept"]));
        assert_eq!(shard.pool_blocks().len(), 2, "the old generation is alive");

        drop(old);
        let names: Vec<_> = shard
            .pool_blocks()
            .into_iter()
            .map(|(name, _)| name.to_string())
            .collect();
        assert_eq!(names, ["kept"]);
        drop(new);
        assert!(shard.pool_blocks().is_empty());
    }

    #[test]
    fn shard_is_cache_line_aligned() {
        assert!(std::mem::align_of::<RoutingMetricsShard>() >= 64);
    }
}
