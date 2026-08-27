use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use rusty_mcrouter_config::PoolId;

use crate::tko::{
    FailOpenThresholds, GlobalTkoMetrics, PoolTkoTracker, TkoEventRecord, TkoEventSink, TkoTracker,
};

pub struct TkoTrackerMap {
    trackers: Mutex<HashMap<Arc<str>, Weak<TkoTracker>>>,
    pool_trackers: Mutex<HashMap<PoolId, Weak<PoolTkoTracker>>>,
    metrics: Arc<GlobalTkoMetrics>,
    sink: TkoEventSink,
}

impl TkoTrackerMap {
    pub fn new(sink: TkoEventSink) -> Arc<TkoTrackerMap> {
        Arc::new(TkoTrackerMap {
            trackers: Mutex::new(HashMap::new()),
            pool_trackers: Mutex::new(HashMap::new()),
            metrics: Arc::new(GlobalTkoMetrics::default()),
            sink,
        })
    }

    pub fn global_metrics(&self) -> &Arc<GlobalTkoMetrics> {
        &self.metrics
    }

    pub fn tracker_for(self: &Arc<Self>, host_port: &str, threshold: u64) -> Arc<TkoTracker> {
        let mut trackers = self.trackers.lock().unwrap();
        if let Some(existing) = trackers.get(host_port).and_then(Weak::upgrade) {
            return existing;
        }

        let key: Arc<str> = Arc::from(host_port);
        let tracker = Arc::new(TkoTracker::new(
            threshold,
            Arc::clone(&self.metrics),
            Arc::clone(&key),
            Arc::downgrade(self),
        ));

        trackers.insert(key, Arc::downgrade(&tracker));

        tracker
    }

    pub fn pool_tracker_for(
        self: &Arc<Self>,
        id: PoolId,
        pool_name: &str,
        thresholds: FailOpenThresholds,
    ) -> Arc<PoolTkoTracker> {
        let mut pools = self.pool_trackers.lock().unwrap();
        if let Some(existing) = pools.get(&id).and_then(Weak::upgrade) {
            debug_assert_eq!(existing.name().as_ref(), pool_name);
            return existing;
        }

        let name: Arc<str> = Arc::from(pool_name);
        let tracker = Arc::new(PoolTkoTracker::new(Arc::clone(&name), thresholds));

        pools.insert(id, Arc::downgrade(&tracker));

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

    pub fn pool_snapshot(&self) -> Vec<Arc<PoolTkoTracker>> {
        let mut pools = self.pool_trackers.lock().unwrap();

        let mut live = Vec::with_capacity(pools.len());

        pools.retain(|_, weak| match weak.upgrade() {
            Some(gate) => {
                live.push(gate);
                true
            }
            None => false,
        });

        live
    }

    pub(crate) fn emit(&self, record: TkoEventRecord) {
        self.sink.emit(record)
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
    use rusty_mcrouter_observability_primitives::test_support::{noop_sink, recording_sink_with};

    use super::*;
    use crate::classify::ResultCode;
    use crate::test_support::{pool_id, pool_ids};
    use crate::tko::DestToken;

    #[test]
    fn tracker_for_dedups_to_same_arc() {
        let map = TkoTrackerMap::new(noop_sink());
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
        let map = TkoTrackerMap::new(noop_sink());
        let a = map.tracker_for("s:1", 1);
        assert!(a.record_soft_failure(DestToken::allocate(), ResultCode::Timeout));
        assert!(a.is_tko());
        drop(a); // ~TkoTracker -> remove_dead

        let b = map.tracker_for("s:1", 1);
        assert!(!b.is_tko(), "replacement tracker must start clean");
    }

    #[test]
    fn pool_tracker_for_dedups_by_id() {
        let map = TkoTrackerMap::new(noop_sink());
        let ids = pool_ids(&["pool", "other"]);
        let a = map.pool_tracker_for(ids[0], "pool", FailOpenThresholds { enter: 3, exit: 1 });
        let b = map.pool_tracker_for(ids[0], "pool", FailOpenThresholds { enter: 3, exit: 1 });
        assert!(Arc::ptr_eq(&a, &b), "same pool must share one gate");
        let c = map.pool_tracker_for(ids[1], "other", FailOpenThresholds { enter: 3, exit: 1 });
        assert!(!Arc::ptr_eq(&a, &c));
    }

    /// pool_snapshot returns live gates and prunes dead entries in the same
    /// pass - a dropped pool leaves the scrape output AND the map.
    #[test]
    fn pool_snapshot_returns_live_and_prunes_dead() {
        let map = TkoTrackerMap::new(noop_sink());
        let ids = pool_ids(&["pool_a", "pool_b"]);
        let a = map.pool_tracker_for(ids[0], "pool_a", FailOpenThresholds { enter: 3, exit: 1 });
        let _b = map.pool_tracker_for(ids[1], "pool_b", FailOpenThresholds { enter: 3, exit: 1 });

        let snap = map.pool_snapshot();
        assert_eq!(snap.len(), 2);

        drop(snap); // snapshot Arcs must not keep pool_a alive
        drop(a);
        let snap = map.pool_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(&**snap[0].name(), "pool_b");

        // and the entry is actually gone, not just skipped
        assert_eq!(map.pool_trackers.lock().unwrap().len(), 1);
    }

    #[test]
    fn sus_servers_reports_only_failing_trackers() {
        let map = TkoTrackerMap::new(noop_sink());
        let bad = map.tracker_for("bad:1", 3);
        let _good = map.tracker_for("good:1", 3);
        assert!(!bad.record_soft_failure(DestToken::allocate(), ResultCode::Timeout));

        let sus = map.sus_servers();
        assert_eq!(sus.len(), 1);
        assert_eq!(&*sus[0].0, "bad:1");
        assert!(!sus[0].1, "one failure of three is suspect, not TKO");
        assert_eq!(sus[0].2, 1);
    }

    // ── contention suite: std::thread, no tokio ─────────────────────────

    use crate::tko::TkoEvent;
    use std::sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc, Barrier,
    };
    use std::time::Duration;

    /// THE invariant the whole encoding exists for: across N threads
    /// hammering soft failures with distinct tokens, every TKO episode has
    /// EXACTLY one responsible destination — proven because record_success
    /// (owner-only unmark) must return true for every winner, and the
    /// underflow debug_asserts in decrement_tko_count arm the double-unmark
    /// case. The gauge draining to zero proves mark/unmark pairing.
    #[test]
    fn responsibility_is_unique_under_contention() {
        let map = TkoTrackerMap::new(noop_sink());
        let tracker = map.tracker_for("s:1", 3);
        let target_wins = 200u64;
        let total_wins = AtomicU64::new(0);

        std::thread::scope(|s| {
            for _ in 0..8 {
                let tracker = Arc::clone(&tracker);
                let total_wins = &total_wins;
                s.spawn(move || {
                    let token = DestToken::allocate();
                    while total_wins.load(Ordering::SeqCst) < target_wins {
                        if tracker.record_soft_failure(token, ResultCode::Timeout) {
                            // we won responsibility: we and ONLY we may unmark
                            assert!(tracker.record_success(token));
                            total_wins.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                });
            }
        });

        assert!(!tracker.is_tko());
        assert_eq!(map.global_metrics().total(), 0, "gauge must drain to zero");
    }

    /// Reservation/undo balance: a lost CAS must return its pool
    /// reservation. With headroom (enter=8) and only 4 concurrent
    /// reservers, ANY leak accumulates round over round and trips
    /// fail-open within ~8 rounds of 200 — asserted as: no EnterFailOpen
    /// ever, and full capacity still available afterwards.
    #[test]
    fn pool_reservation_undo_balances_under_contention() {
        let (sink, events) = recording_sink_with(|record: TkoEventRecord| record.event);
        let map = TkoTrackerMap::new(sink);
        let gate = map.pool_tracker_for(
            pool_id("pool"),
            "pool",
            FailOpenThresholds { enter: 8, exit: 1 },
        );
        let tracker = map.tracker_for("s:1", 1); // threshold 1: every attempt reserves
        tracker.set_pool_tracker(Arc::clone(&gate));
        let target_wins = 200u64;
        let total_wins = AtomicU64::new(0);

        std::thread::scope(|s| {
            for _ in 0..4 {
                let tracker = Arc::clone(&tracker);
                let total_wins = &total_wins;
                s.spawn(move || {
                    let token = DestToken::allocate();
                    while total_wins.load(Ordering::SeqCst) < target_wins {
                        if tracker.record_soft_failure(token, ResultCode::Timeout) {
                            assert!(tracker.record_success(token));
                            total_wins.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                });
            }
        });

        assert!(
            !events.lock().unwrap().contains(&TkoEvent::EnterFailOpen),
            "a leaked reservation accumulated into spurious fail-open"
        );
        // capacity probe: the pool count drained fully, so 8 fresh boxes
        // can all be marked
        for i in 0..8 {
            let t = map.tracker_for(&format!("probe:{i}"), 1);
            t.set_pool_tracker(Arc::clone(&gate));
            assert!(
                t.record_soft_failure(DestToken::allocate(), ResultCode::Timeout),
                "probe box {i} refused: pool count did not drain to zero"
            );
        }
    }

    /// End-to-end hysteresis with the event stream: gate {enter:3, exit:1},
    /// kill 3 -> marked; the 4th keeps failing UNMARKED and EnterFailOpen
    /// fires exactly once; recover down to the exit threshold ->
    /// ExitFailOpen exactly once -> marking admitted again.
    #[test]
    fn fail_open_hysteresis_emits_enter_and_exit_exactly_once() {
        let (sink, events) = recording_sink_with(|record: TkoEventRecord| record.event);
        let map = TkoTrackerMap::new(sink);
        let gate = map.pool_tracker_for(
            pool_id("pool"),
            "pool",
            FailOpenThresholds { enter: 3, exit: 1 },
        );

        let boxes: Vec<_> = (0..5)
            .map(|i| {
                let t = map.tracker_for(&format!("s:{i}"), 1);
                t.set_pool_tracker(Arc::clone(&gate));
                (t, DestToken::allocate())
            })
            .collect();

        for (t, tok) in boxes.iter().take(3) {
            assert!(t.record_hard_failure(*tok, ResultCode::ConnectError));
        }
        // 4th and 5th: refused, unmarked; only the crossing call emits
        assert!(!boxes[3]
            .0
            .record_hard_failure(boxes[3].1, ResultCode::ConnectError));
        assert!(!boxes[3].0.is_tko());
        assert!(!boxes[4]
            .0
            .record_hard_failure(boxes[4].1, ResultCode::ConnectError));
        assert_eq!(
            *events.lock().unwrap(),
            vec![TkoEvent::EnterFailOpen],
            "enter must fire exactly once"
        );
        // the scrape accessors inherit the same exactly-once choreography
        assert!(gate.fail_open());
        assert_eq!(gate.fail_open_entered_total(), 1);
        assert_eq!(gate.fail_open_exited_total(), 0);

        // recover marked boxes; the drain to exit=1 flips the gate back
        for (t, tok) in boxes.iter().take(3) {
            assert!(t.record_success(*tok));
        }
        assert_eq!(
            *events.lock().unwrap(),
            vec![TkoEvent::EnterFailOpen, TkoEvent::ExitFailOpen],
            "exit must fire exactly once"
        );
        assert!(!gate.fail_open());
        assert_eq!(gate.fail_open_entered_total(), 1);
        assert_eq!(gate.fail_open_exited_total(), 1);

        // gate admits marks again
        assert!(boxes[3]
            .0
            .record_hard_failure(boxes[3].1, ResultCode::ConnectError));
        assert!(boxes[3].0.is_hard_tko());
    }

    /// Hard marking under contention: N threads race record_hard_failure
    /// with distinct tokens; exactly one wins, the global hard gauge counts
    /// one, and the winner's token is the only one that can unmark.
    #[test]
    fn hard_failure_single_winner_under_contention() {
        let map = TkoTrackerMap::new(noop_sink());
        let tracker = map.tracker_for("s:1", 3);
        let tokens: Vec<DestToken> = (0..8).map(|_| DestToken::allocate()).collect();
        let wins = AtomicUsize::new(0);
        let winner_idx = AtomicUsize::new(usize::MAX);

        std::thread::scope(|s| {
            for (i, token) in tokens.iter().enumerate() {
                let tracker = Arc::clone(&tracker);
                let (wins, winner_idx) = (&wins, &winner_idx);
                let token = *token;
                s.spawn(move || {
                    if tracker.record_hard_failure(token, ResultCode::ConnectError) {
                        wins.fetch_add(1, Ordering::SeqCst);
                        winner_idx.store(i, Ordering::SeqCst);
                    }
                });
            }
        });

        assert_eq!(wins.load(Ordering::SeqCst), 1, "exactly one winner");
        assert_eq!(map.global_metrics().hard_tkos.load(), 1);
        assert!(tracker.is_hard_tko());
        assert!(tracker.record_success(tokens[winner_idx.load(Ordering::SeqCst)]));
        assert_eq!(map.global_metrics().total(), 0);
    }

    #[test]
    fn sus_servers_does_not_deadlock_with_final_owner_drops() {
        const WORKERS: usize = 4;
        const ROUNDS: usize = 5_000;

        let map = TkoTrackerMap::new(noop_sink());
        let active = Arc::new(AtomicUsize::new(WORKERS));
        let start = Arc::new(Barrier::new(WORKERS + 1));
        let (done_tx, done_rx) = mpsc::channel();
        let mut threads = Vec::with_capacity(WORKERS + 1);

        for worker in 0..WORKERS {
            let map = Arc::clone(&map);
            let active = Arc::clone(&active);
            let start = Arc::clone(&start);
            let done_tx = done_tx.clone();
            threads.push(std::thread::spawn(move || {
                start.wait();
                for round in 0..ROUNDS {
                    let tracker = map.tracker_for(&format!("churn:{worker}:{round}"), 3);
                    tracker.record_soft_failure(DestToken::allocate(), ResultCode::Timeout);
                    std::thread::yield_now();
                    drop(tracker);
                }
                active.fetch_sub(1, Ordering::SeqCst);
                done_tx.send(()).unwrap();
            }));
        }

        let scan_map = Arc::clone(&map);
        let scan_active = Arc::clone(&active);
        let scan_start = Arc::clone(&start);
        let scan_done = done_tx.clone();
        threads.push(std::thread::spawn(move || {
            scan_start.wait();
            while scan_active.load(Ordering::SeqCst) != 0 {
                let _ = scan_map.sus_servers();
                std::thread::yield_now();
            }
            scan_done.send(()).unwrap();
        }));
        drop(done_tx);

        for _ in 0..WORKERS + 1 {
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("tracker churn and suspect scans must not deadlock");
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }
}
