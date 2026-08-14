use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use crate::tko::{
    counters::TkoCounters,
    events::{default_sink, TkoEventRecord, TkoEventSink},
    pool::PoolTkoTracker,
    tracker::TkoTracker,
};

pub struct TkoTrackerMap {
    trackers: Mutex<HashMap<Arc<str>, Weak<TkoTracker>>>,
    pool_trackers: Mutex<HashMap<Arc<str>, Weak<PoolTkoTracker>>>,
    global: Arc<TkoCounters>,
    sink: TkoEventSink,
}

impl TkoTrackerMap {
    pub fn new() -> Arc<TkoTrackerMap> {
        Self::with_sink(default_sink())
    }

    // tests inject a collecting sink -  production swaps in tracing/metrics later
    pub fn with_sink(sink: TkoEventSink) -> Arc<TkoTrackerMap> {
        Arc::new(TkoTrackerMap {
            trackers: Mutex::new(HashMap::new()),
            pool_trackers: Mutex::new(HashMap::new()),
            global: Arc::new(TkoCounters::default()),
            sink,
        })
    }

    pub fn global_tkos(&self) -> &Arc<TkoCounters> {
        &self.global
    }

    pub fn tracker_for(self: &Arc<Self>, host_port: &str, threshold: u64) -> Arc<TkoTracker> {
        let mut trackers = self.trackers.lock().unwrap();
        if let Some(existing) = trackers.get(host_port).and_then(Weak::upgrade) {
            return existing;
        }

        let key: Arc<str> = Arc::from(host_port);
        let tracker = Arc::new(TkoTracker::new(
            threshold,
            Arc::clone(&self.global),
            Arc::clone(&key),
            Arc::downgrade(self),
        ));

        trackers.insert(key, Arc::downgrade(&tracker));

        tracker
    }

    pub fn pool_tracker_for(
        self: &Arc<Self>,
        pool_name: &str,
        enter: u64,
        exit: u64,
    ) -> Arc<PoolTkoTracker> {
        let mut pools = self.pool_trackers.lock().unwrap();
        if let Some(existing) = pools.get(pool_name).and_then(Weak::upgrade) {
            return existing;
        }

        let name: Arc<str> = Arc::from(pool_name);
        let tracker = Arc::new(PoolTkoTracker::new(Arc::clone(&name), enter, exit));

        pools.insert(name, Arc::downgrade(&tracker));

        tracker
    }

    pub fn sus_servers(&self) -> Vec<(Arc<str>, bool, u64)> {
        let trackers: Vec<_> = self
            .trackers
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(key, weak)| weak.upgrade().map(|t| (Arc::clone(key), t)))
            .collect();

        trackers
            .into_iter()
            .filter_map(|(key, t)| {
                let failures = t.consecutive_failures();
                (failures > 0).then(|| (key, t.is_tko(), failures))
            })
            .collect()
    }

    pub(crate) fn emit(&self, record: &TkoEventRecord<'_>) {
        (self.sink)(record)
    }

    pub(crate) fn remove_dead(&self, key: &str) {
        let mut trackers = self.trackers.lock().unwrap();

        if let Some(weak) = trackers.get(key) {
            if weak.strong_count() == 0 {
                trackers.remove(key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::ResultCode;
    use crate::tko::tracker::DestToken;

    fn null_sink() -> TkoEventSink {
        Box::new(|_| {})
    }

    #[test]
    fn tracker_for_dedups_to_same_arc() {
        let map = TkoTrackerMap::with_sink(null_sink());
        let a = map.tracker_for("s:1", 3);
        let b = map.tracker_for("s:1", 3);
        assert!(Arc::ptr_eq(&a, &b), "same server must share one tracker");
        let c = map.tracker_for("s:2", 3);
        assert!(!Arc::ptr_eq(&a, &c));
    }

    /// A dropped tracker deregisters itself; the next tracker_for gets a
    /// FRESH tracker (dead state is not resurrected).
    #[test]
    fn dead_tracker_is_replaced_with_fresh_state() {
        let map = TkoTrackerMap::with_sink(null_sink());
        let a = map.tracker_for("s:1", 1);
        assert!(a.record_soft_failure(DestToken::allocate(), ResultCode::Timeout));
        assert!(a.is_tko());
        drop(a); // ~TkoTracker -> remove_dead

        let b = map.tracker_for("s:1", 1);
        assert!(!b.is_tko(), "replacement tracker must start clean");
    }

    #[test]
    fn pool_tracker_for_dedups_by_name() {
        let map = TkoTrackerMap::with_sink(null_sink());
        let a = map.pool_tracker_for("pool", 3, 1);
        let b = map.pool_tracker_for("pool", 3, 1);
        assert!(Arc::ptr_eq(&a, &b), "same pool must share one gate");
        let c = map.pool_tracker_for("other", 3, 1);
        assert!(!Arc::ptr_eq(&a, &c));
    }

    #[test]
    fn sus_servers_reports_only_failing_trackers() {
        let map = TkoTrackerMap::with_sink(null_sink());
        let bad = map.tracker_for("bad:1", 3);
        let _good = map.tracker_for("good:1", 3);
        assert!(!bad.record_soft_failure(DestToken::allocate(), ResultCode::Timeout));

        let sus = map.sus_servers();
        assert_eq!(sus.len(), 1);
        assert_eq!(&*sus[0].0, "bad:1");
        assert!(!sus[0].1, "one failure of three is suspect, not TKO");
        assert_eq!(sus[0].2, 1);
    }
}
