use std::{
    cell::RefCell,
    collections::HashMap,
    rc::{Rc, Weak},
    sync::Arc,
    time::Duration,
};

use tokio::time::Instant;

use crate::{
    counters::BackendCounterShard,
    destination::{
        config::Config, destination::Destination, key::Key, DestinationCountersRegistry,
    },
    tko::{PoolTkoTracker, TkoTrackerMap},
};

pub struct Map {
    tko_map: Arc<TkoTrackerMap>,
    counters_registry: Arc<DestinationCountersRegistry>,
    shard_counters: Arc<BackendCounterShard>,
    destinations: RefCell<HashMap<Key, Weak<Destination>>>,
}

impl Map {
    pub fn new(
        tko_map: Arc<TkoTrackerMap>,
        shard_counters: Arc<BackendCounterShard>,
        counters_registry: Arc<DestinationCountersRegistry>,
    ) -> Rc<Self> {
        Rc::new(Self {
            tko_map,
            shard_counters,
            counters_registry,
            destinations: RefCell::new(HashMap::new()),
        })
    }

    /// The factory resolves pool fail-open gates through here
    /// (pool_tracker_for), keeping one registry handle per thread.
    pub fn tko_map(&self) -> &Arc<TkoTrackerMap> {
        &self.tko_map
    }

    pub fn destination(
        &self,
        key: Key,
        cfg: &Config,
        pool_tracker: Option<Arc<PoolTkoTracker>>,
    ) -> Rc<Destination> {
        if let Some(existing) = self.destinations.borrow().get(&key).and_then(Weak::upgrade) {
            if let Some(gate) = pool_tracker {
                existing.tracker().set_pool_tracker(gate);
            }
            return existing;
        }

        let tracker = self.tko_map.tracker_for(&key.addr, cfg.failures_until_tko);

        if let Some(gate) = pool_tracker {
            tracker.set_pool_tracker(gate);
        }

        let counters = self.counters_registry.counters_for(&key.addr, &tracker);
        let dest = Destination::new(
            key.clone(),
            cfg.clone(),
            tracker,
            counters,
            Arc::clone(&self.shard_counters),
        );

        self.destinations
            .borrow_mut()
            .insert(key, Rc::downgrade(&dest));

        dest
    }

    pub fn spawn_idle_sweep(
        self: &Rc<Self>,
        interval: Duration,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if interval.is_zero() {
            return None;
        }

        let weak = Rc::downgrade(self);

        Some(tokio::task::spawn_local(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(map) = weak.upgrade() else { return };
                map.sweep_idle(interval);
            }
        }))
    }

    fn sweep_idle(&self, interval: Duration) {
        let now = Instant::now();

        self.destinations.borrow_mut().retain(|_, weak| {
            let Some(destination) = weak.upgrade() else {
                return false;
            };

            if now.duration_since(destination.idle_since()) >= interval {
                destination.close_idle_connection();
            }

            true
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use rusty_mcrouter_protocol::test_support::get;

    use super::*;
    use crate::classify::ResultCode;
    use crate::error::{RequestError, SendError};
    use crate::test_support::{run_local, scripted_backend_serial, Step};
    use crate::tko::{DestToken, FailOpenThresholds};

    fn tko_map() -> Arc<TkoTrackerMap> {
        TkoTrackerMap::with_sink(Box::new(|_| {}))
    }

    fn test_map() -> Rc<Map> {
        Map::new(
            tko_map(),
            BackendCounterShard::new(),
            DestinationCountersRegistry::new(),
        )
    }

    fn test_cfg() -> Config {
        Config {
            connect_timeout: Some(Duration::from_millis(1000)),
            reply_timeout: Some(Duration::from_millis(1000)),
            connect_timeout_retries: 0,
            failures_until_tko: 1,
            probe_delay_initial: Duration::from_secs(10),
            probe_delay_max: Duration::from_secs(60),
            disable_tko_tracking: false,
        }
    }

    fn key_for(addr: &str, reply_timeout_ms: u64) -> Key {
        Key {
            addr: Arc::from(addr),
            reply_timeout: Some(Duration::from_millis(reply_timeout_ms)),
        }
    }

    /// THE regression test: dedup must hit for pools WITHOUT a tko_tracker
    /// gate (the default) — an earlier draft only returned the existing
    /// destination inside the gate branch, silently duplicating connections.
    #[tokio::test]
    async fn destination_dedups_to_same_rc_without_gate() {
        run_local(async {
            let map = test_map();
            let a = map.destination(key_for("127.0.0.1:9", 1000), &test_cfg(), None);
            let b = map.destination(key_for("127.0.0.1:9", 1000), &test_cfg(), None);
            assert!(Rc::ptr_eq(&a, &b), "same key must share one destination");
        })
        .await;
    }

    #[tokio::test]
    async fn different_reply_timeout_gets_distinct_destination() {
        run_local(async {
            let map = test_map();
            let a = map.destination(key_for("127.0.0.1:9", 100), &test_cfg(), None);
            let b = map.destination(key_for("127.0.0.1:9", 200), &test_cfg(), None);
            assert!(
                !Rc::ptr_eq(&a, &b),
                "different latency contracts must not share a FIFO"
            );
        })
        .await;
    }

    /// two timeout-variants of one server are distinct destinations (own
    /// FIFOs) but ONE counter block: the `destination` label means "server",
    /// not "server x timeout".
    #[tokio::test]
    async fn timeout_variants_share_one_counter_block() {
        run_local(async {
            let map = test_map();
            let a = map.destination(key_for("127.0.0.1:9", 100), &test_cfg(), None);
            let b = map.destination(key_for("127.0.0.1:9", 200), &test_cfg(), None);
            assert!(!Rc::ptr_eq(&a, &b));
            assert!(
                Arc::ptr_eq(a.counters(), b.counters()),
                "same server must share one counter block"
            );
        })
        .await;
    }

    /// mcrouter updateTracker semantics: a later pool naming an EXISTING
    /// destination attaches its gate to the shared tracker. Proven via
    /// capacity: the gate has one slot; the existing destination's mark
    /// consumes it; a second server's mark is then refused.
    #[tokio::test]
    async fn dedup_attaches_gate_to_existing_destination() {
        run_local(async {
            let tko = tko_map();
            let map = Map::new(
                Arc::clone(&tko),
                BackendCounterShard::new(),
                DestinationCountersRegistry::new(),
            );
            let gate = tko.pool_tracker_for("pool", FailOpenThresholds { enter: 1, exit: 1 });

            let a1 = map.destination(key_for("127.0.0.1:9", 1000), &test_cfg(), None);
            let a2 = map.destination(
                key_for("127.0.0.1:9", 1000),
                &test_cfg(),
                Some(Arc::clone(&gate)),
            );
            assert!(Rc::ptr_eq(&a1, &a2));

            let b = map.destination(
                key_for("127.0.0.1:10", 1000),
                &test_cfg(),
                Some(Arc::clone(&gate)),
            );
            assert!(a1
                .tracker()
                .record_hard_failure(DestToken::allocate(), ResultCode::ConnectError));
            assert!(
                !b.tracker()
                    .record_hard_failure(DestToken::allocate(), ResultCode::ConnectError),
                "gate slot must have been consumed through the EXISTING destination"
            );
        })
        .await;
    }

    /// The sweep closes a connection idle past the interval — benignly:
    /// no Down, no TKO, and the next send silently reconnects.
    #[tokio::test]
    async fn sweep_closes_idle_and_next_send_reconnects() {
        run_local(async {
            let server = scripted_backend_serial(vec![
                vec![
                    Step::ReadRequests(1),
                    Step::Write(b"EN\r\n"),
                    Step::ReadRequests(2), // parks until the client closes
                ],
                vec![Step::ReadRequests(1), Step::Write(b"EN\r\n")],
            ])
            .await;
            let map = test_map();
            let dest = map.destination(key_for(&server.addr.to_string(), 1000), &test_cfg(), None);

            dest.send(get(b"a")).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            map.sweep_idle(Duration::from_millis(50)); // idle 60ms >= 50ms

            // CloseIdle is processed by the actor asynchronously
            for _ in 0..2000 {
                if dest.counters().idle_closes.load(Ordering::Relaxed) == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert_eq!(dest.counters().idle_closes.load(Ordering::Relaxed), 1);
            assert!(!dest.is_tko(), "an idle close is never health evidence");

            dest.send(get(b"b")).await.unwrap();
            assert_eq!(server.accept_count(), 2);
        })
        .await;
    }

    #[tokio::test]
    async fn sweep_reaps_tombstones_and_probe_reconnects() {
        run_local(async {
            let server = scripted_backend_serial(vec![
                vec![Step::ReadRequests(usize::MAX)],
                vec![Step::ReadRequests(1), Step::Write(b"VERSION 1.6.39\r\n")],
            ])
            .await;
            let mut cfg = test_cfg();
            cfg.connect_timeout = Some(Duration::from_millis(100));
            cfg.reply_timeout = Some(Duration::from_millis(20));
            cfg.probe_delay_initial = Duration::from_millis(5);
            cfg.probe_delay_max = Duration::from_millis(100);

            let map = test_map();
            let dest = map.destination(key_for(&server.addr.to_string(), 20), &cfg, None);
            let tracker = Arc::clone(dest.tracker());

            let result = dest.send(get(b"a")).await;
            assert!(matches!(
                result,
                Err(SendError::Request(RequestError::Timeout { sent: true }))
            ));
            assert!(tracker.is_soft_tko());

            let interval = Duration::from_millis(35);
            let deadline = Instant::now() + Duration::from_secs(2);
            while dest.counters().idle_closes.load(Ordering::Relaxed) == 0 {
                assert!(
                    Instant::now() < deadline,
                    "tombstone-only connection was never reaped"
                );
                if Instant::now().duration_since(dest.idle_since()) >= interval {
                    map.sweep_idle(interval);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            while tracker.is_tko() {
                assert!(
                    Instant::now() < deadline,
                    "probe did not reconnect after the idle close"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert_eq!(server.accept_count(), 2);
        })
        .await;
    }

    /// Activity refreshes the idle clock: a destination used more recently
    /// than the interval is left alone.
    #[tokio::test]
    async fn sweep_leaves_recently_active_destinations_alone() {
        run_local(async {
            let server = scripted_backend_serial(vec![vec![
                Step::ReadRequests(1),
                Step::Write(b"EN\r\n"),
                Step::ReadRequests(2), // parks
            ]])
            .await;
            let map = test_map();
            let dest = map.destination(key_for(&server.addr.to_string(), 1000), &test_cfg(), None);

            dest.send(get(b"a")).await.unwrap(); // last_active = now
            map.sweep_idle(Duration::from_millis(50)); // idle ~0ms < 50ms

            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(dest.counters().idle_closes.load(Ordering::Relaxed), 0);
            assert_eq!(server.accept_count(), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn sweep_prunes_dead_entries() {
        run_local(async {
            let map = test_map();
            let dest = map.destination(key_for("127.0.0.1:9", 1000), &test_cfg(), None);
            assert_eq!(map.destinations.borrow().len(), 1);

            drop(dest);
            map.sweep_idle(Duration::from_millis(1));
            assert_eq!(map.destinations.borrow().len(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn sweep_task_exits_when_map_drops() {
        run_local(async {
            let map = test_map();
            let handle = map.spawn_idle_sweep(Duration::from_millis(10)).unwrap();
            assert!(map.spawn_idle_sweep(Duration::ZERO).is_none());

            drop(map);
            tokio::time::timeout(Duration::from_millis(200), handle)
                .await
                .expect("sweep task must exit once the map is gone")
                .unwrap();
        })
        .await;
    }
}
