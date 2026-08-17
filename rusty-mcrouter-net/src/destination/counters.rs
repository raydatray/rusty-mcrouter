use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};

use crate::{
    classify::{ResultCode, RESULT_CODE_COUNT},
    tko::TkoTracker,
};

pub struct DestinationCounters {
    pub addr: Arc<str>,
    pub tracker: Arc<TkoTracker>,
    pub requests: [AtomicU64; RESULT_CODE_COUNT],
    pub probes_sent: AtomicU64,
    pub connects: AtomicU64,
    pub idle_closes: AtomicU64,
    pub latency_us_sum: AtomicU64,
    pub inflight_reqs: AtomicI64,
}

impl DestinationCounters {
    fn new(addr: Arc<str>, tracker: Arc<TkoTracker>) -> Arc<Self> {
        Arc::new(Self {
            addr,
            tracker,
            requests: [const { AtomicU64::new(0) }; RESULT_CODE_COUNT],
            probes_sent: AtomicU64::new(0),
            connects: AtomicU64::new(0),
            idle_closes: AtomicU64::new(0),
            latency_us_sum: AtomicU64::new(0),
            inflight_reqs: AtomicI64::new(0),
        })
    }

    pub fn record_send(&self, code: ResultCode, latency_us: u64) {
        self.requests[code as usize].fetch_add(1, Ordering::Relaxed);
        self.latency_us_sum.fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_result(&self, code: ResultCode) {
        self.requests[code as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn result_count(&self, code: ResultCode) -> u64 {
        self.requests[code as usize].load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct DestinationCountersRegistry {
    inner: Mutex<HashMap<Arc<str>, Weak<DestinationCounters>>>,
}

impl DestinationCountersRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn counters_for(
        &self,
        addr: &Arc<str>,
        tracker: &Arc<TkoTracker>,
    ) -> Arc<DestinationCounters> {
        let mut inner = self
            .inner
            .lock()
            .expect("destination counters registry poisoned");

        if let Some(existing) = inner.get(addr).and_then(Weak::upgrade) {
            return existing;
        }

        let counters = DestinationCounters::new(Arc::clone(addr), Arc::clone(tracker));
        inner.insert(Arc::clone(addr), Arc::downgrade(&counters));

        counters
    }

    pub fn snapshot(&self) -> Vec<Arc<DestinationCounters>> {
        let mut inner = self
            .inner
            .lock()
            .expect("destination counters registry poisoned");

        let mut live = Vec::with_capacity(inner.len());

        inner.retain(|_, weak| match weak.upgrade() {
            Some(counters) => {
                live.push(counters);
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
        let registry = DestinationCountersRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a = addr("10.0.0.1:11211");
        let tracker = tracker_for(&tko_map, &a);

        let m1 = registry.counters_for(&a, &tracker);
        let m2 = registry.counters_for(&a, &tracker);
        assert!(Arc::ptr_eq(&m1, &m2));

        m1.record_send(ResultCode::Success, 100);
        m2.record_send(ResultCode::Success, 200);
        assert_eq!(
            m1.requests[ResultCode::Success as usize].load(Ordering::Relaxed),
            2
        );
        assert_eq!(m1.latency_us_sum.load(Ordering::Relaxed), 300);
    }

    #[test]
    fn different_addrs_get_distinct_blocks() {
        let registry = DestinationCountersRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a1 = addr("10.0.0.1:11211");
        let a2 = addr("10.0.0.2:11211");
        let m1 = registry.counters_for(&a1, &tracker_for(&tko_map, &a1));
        let m2 = registry.counters_for(&a2, &tracker_for(&tko_map, &a2));
        assert!(!Arc::ptr_eq(&m1, &m2));
    }

    /// THE lifecycle test: dropping the last owner removes the server
    /// from the next snapshot - dead destinations leave /metrics.
    #[test]
    fn dead_blocks_leave_the_snapshot() {
        let registry = DestinationCountersRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a1 = addr("10.0.0.1:11211");
        let a2 = addr("10.0.0.2:11211");

        let m1 = registry.counters_for(&a1, &tracker_for(&tko_map, &a1));
        let m2 = registry.counters_for(&a2, &tracker_for(&tko_map, &a2));
        assert_eq!(registry.snapshot().len(), 2);

        drop(m1);
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(&*snap[0].addr, "10.0.0.2:11211");

        // and the map entry is actually gone, not just skipped
        assert_eq!(registry.inner.lock().unwrap().len(), 1);
        drop(m2);
    }

    /// a re-added server gets a FRESH block, not resurrected counts.
    #[test]
    fn readded_addr_starts_fresh() {
        let registry = DestinationCountersRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a = addr("10.0.0.1:11211");
        let tracker = tracker_for(&tko_map, &a);

        let m1 = registry.counters_for(&a, &tracker);
        m1.record_send(ResultCode::Success, 100);
        drop(m1);

        let m2 = registry.counters_for(&a, &tracker);
        assert_eq!(
            m2.requests[ResultCode::Success as usize].load(Ordering::Relaxed),
            0
        );
    }

    /// registry is shared across proxy threads - blocks from two
    /// threads naming one server are the same block.
    #[test]
    fn cross_thread_sharing() {
        let registry = DestinationCountersRegistry::new();
        let tko_map = TkoTrackerMap::new();
        let a = addr("10.0.0.1:11211");
        let tracker = tracker_for(&tko_map, &a);

        let handle = {
            let (registry, a, tracker) =
                (Arc::clone(&registry), Arc::clone(&a), Arc::clone(&tracker));
            std::thread::spawn(move || registry.counters_for(&a, &tracker))
        };
        let theirs = handle.join().unwrap();
        let ours = registry.counters_for(&a, &tracker);
        assert!(Arc::ptr_eq(&ours, &theirs));
    }
}
