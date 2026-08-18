use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use rusty_mcrouter_observability_primitives::{Counter, Gauge};

use crate::{
    classify::{ResultCode, RESULT_CODE_COUNT},
    tko::TkoTracker,
};

pub struct DestinationMetrics {
    pub tracker: Arc<TkoTracker>,
    pub requests: [Counter; RESULT_CODE_COUNT],
    pub probes_sent: Gauge,
    pub connects: Counter,
    pub idle_closes: Counter,
    pub latency_us_sum: Counter,
    pub inflight_reqs: Gauge,
}

impl DestinationMetrics {
    fn new(tracker: Arc<TkoTracker>) -> Arc<Self> {
        Arc::new(Self {
            tracker,
            requests: Default::default(),
            probes_sent: Gauge::default(),
            connects: Counter::default(),
            idle_closes: Counter::default(),
            latency_us_sum: Counter::default(),
            inflight_reqs: Gauge::default(),
        })
    }

    pub fn destination(&self) -> &str {
        self.tracker.key()
    }

    pub fn record_send(&self, code: ResultCode, latency_us: u64) {
        self.requests[code as usize].inc();
        self.latency_us_sum.add(latency_us);
    }

    pub fn record_result(&self, code: ResultCode) {
        self.requests[code as usize].inc();
    }

    pub fn result_count(&self, code: ResultCode) -> u64 {
        self.requests[code as usize].load()
    }
}

#[derive(Default)]
pub struct DestinationMetricsRegistry {
    inner: Mutex<HashMap<Arc<str>, Weak<DestinationMetrics>>>,
}

impl DestinationMetricsRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn metrics_for(&self, tracker: &Arc<TkoTracker>) -> Arc<DestinationMetrics> {
        let mut inner = self
            .inner
            .lock()
            .expect("destination metrics registry poisoned");

        if let Some(existing) = inner.get(tracker.key()).and_then(Weak::upgrade) {
            return existing;
        }

        let metrics = DestinationMetrics::new(Arc::clone(tracker));
        inner.insert(Arc::clone(tracker.key()), Arc::downgrade(&metrics));

        metrics
    }

    pub fn snapshot(&self) -> Vec<Arc<DestinationMetrics>> {
        let mut inner = self
            .inner
            .lock()
            .expect("destination metrics registry poisoned");

        let mut live = Vec::with_capacity(inner.len());

        inner.retain(|_, weak| match weak.upgrade() {
            Some(metrics) => {
                live.push(metrics);
                true
            }
            None => false,
        });

        live
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tko::TkoTrackerMap;

    fn tracker_for(map: &Arc<TkoTrackerMap>, addr: &Arc<str>) -> Arc<TkoTracker> {
        map.tracker_for(addr, 3)
    }

    fn addr(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    /// same addr -> same block; counts from both handles sum.
    #[test]
    fn same_addr_shares_one_block() {
        let registry = DestinationMetricsRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a = addr("10.0.0.1:11211");
        let tracker = tracker_for(&tko_map, &a);

        let m1 = registry.metrics_for(&tracker);
        let m2 = registry.metrics_for(&tracker);
        assert!(Arc::ptr_eq(&m1, &m2));

        m1.record_send(ResultCode::Success, 100);
        m2.record_send(ResultCode::Success, 200);
        assert_eq!(m1.requests[ResultCode::Success as usize].load(), 2);
        assert_eq!(m1.latency_us_sum.load(), 300);
    }

    #[test]
    fn different_addrs_get_distinct_blocks() {
        let registry = DestinationMetricsRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a1 = addr("10.0.0.1:11211");
        let a2 = addr("10.0.0.2:11211");
        let m1 = registry.metrics_for(&tracker_for(&tko_map, &a1));
        let m2 = registry.metrics_for(&tracker_for(&tko_map, &a2));
        assert!(!Arc::ptr_eq(&m1, &m2));
    }

    /// THE lifecycle test: dropping the last owner removes the server
    /// from the next snapshot - dead destinations leave /metrics.
    #[test]
    fn dead_blocks_leave_the_snapshot() {
        let registry = DestinationMetricsRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a1 = addr("10.0.0.1:11211");
        let a2 = addr("10.0.0.2:11211");

        let m1 = registry.metrics_for(&tracker_for(&tko_map, &a1));
        let m2 = registry.metrics_for(&tracker_for(&tko_map, &a2));
        assert_eq!(registry.snapshot().len(), 2);

        drop(m1);
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].destination(), "10.0.0.2:11211");

        // and the map entry is actually gone, not just skipped
        assert_eq!(registry.inner.lock().unwrap().len(), 1);
        drop(m2);
    }

    /// a re-added server gets a FRESH block, not resurrected counts.
    #[test]
    fn readded_addr_starts_fresh() {
        let registry = DestinationMetricsRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a = addr("10.0.0.1:11211");
        let tracker = tracker_for(&tko_map, &a);

        let m1 = registry.metrics_for(&tracker);
        m1.record_send(ResultCode::Success, 100);
        drop(m1);

        let m2 = registry.metrics_for(&tracker);
        assert_eq!(m2.requests[ResultCode::Success as usize].load(), 0);
    }

    /// registry is shared across proxy threads - blocks from two
    /// threads naming one server are the same block.
    #[test]
    fn cross_thread_sharing() {
        let registry = DestinationMetricsRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a = addr("10.0.0.1:11211");
        let tracker = tracker_for(&tko_map, &a);

        let handle = {
            let (registry, tracker) = (Arc::clone(&registry), Arc::clone(&tracker));
            std::thread::spawn(move || registry.metrics_for(&tracker))
        };
        let theirs = handle.join().unwrap();
        let ours = registry.metrics_for(&tracker);
        assert!(Arc::ptr_eq(&ours, &theirs));
    }
}
