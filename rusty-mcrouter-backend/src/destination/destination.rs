use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::{Rc, Weak},
    sync::Arc,
};

use rusty_mcrouter_protocol::{Reply, Request, RequestKind};
use tokio::time::Instant;

use crate::{
    backend::{PreparedSend, TkoRejection},
    classify::{code_of, ResultCode},
    client::{Config as ClientConfig, ConnectionEvent, ConnectionHandle, DownReason},
    destination::{probe, Config, DestinationKey, DestinationMetrics},
    error::{ConnectError, LocalError, SendError},
    metrics::BackendMetricsShard,
    tko::{DestToken, TkoEvent, TkoTracker},
    Backend,
};

pub struct Destination {
    key: DestinationKey,
    token: DestToken,
    tracker: Arc<TkoTracker>,
    conn: ConnectionHandle,
    cfg: Config,
    probe: RefCell<Option<tokio::task::JoinHandle<()>>>,
    metrics: Arc<DestinationMetrics>,
    shard_metrics: Arc<BackendMetricsShard>,
    last_active: Cell<Instant>,
}

impl Destination {
    pub fn new(
        key: DestinationKey,
        cfg: Config,
        tracker: Arc<TkoTracker>,
        metrics: Arc<DestinationMetrics>,
        shard_metrics: Arc<BackendMetricsShard>,
    ) -> Rc<Self> {
        Rc::new_cyclic(|weak: &Weak<Destination>| {
            let events = {
                let weak = weak.clone();
                Box::new(move |ev| {
                    if let Some(dest) = weak.upgrade() {
                        dest.on_conn_event(ev);
                    }
                }) as Box<dyn Fn(ConnectionEvent)>
            };

            let client_cfg = ClientConfig {
                connect_timeout: cfg.connect_timeout,
                connect_timeout_retries: cfg.connect_timeout_retries,
                write_timeout: cfg.reply_timeout,
                reply_timeout: cfg.reply_timeout,
                ..ClientConfig::default()
            };

            let addr = Arc::clone(&key.addr);
            Destination {
                key,
                token: DestToken::allocate(),
                tracker,
                conn: ConnectionHandle::spawn(addr, client_cfg, events, Arc::clone(&shard_metrics)),
                cfg,
                probe: RefCell::new(None),
                metrics,
                shard_metrics,
                last_active: Cell::new(Instant::now()),
            }
        })
    }

    pub(crate) fn is_tko(&self) -> bool {
        self.tracker.is_tko()
    }

    pub(crate) fn tracker(&self) -> &Arc<TkoTracker> {
        &self.tracker
    }

    pub(crate) fn idle_since(&self) -> Instant {
        self.last_active.get()
    }

    pub fn key(&self) -> &DestinationKey {
        &self.key
    }

    pub fn metrics(&self) -> &Arc<DestinationMetrics> {
        &self.metrics
    }

    pub(crate) fn close_idle_connection(&self) {
        self.conn.close_idle()
    }

    async fn send_prepared(
        self: &Rc<Self>,
        request: Request,
        kind: RequestKind,
    ) -> Result<Reply, SendError> {
        let start = Instant::now();
        let inflight = InflightGuard::new(&self.metrics);
        let result = self.conn.send(request).await;
        drop(inflight);
        let code = code_of(&result);
        let latency_us = start.elapsed().as_micros() as u64;

        if matches!(&result, Err(SendError::Local(LocalError::QueueFull))) {
            self.shard_metrics.queue_full.inc();
        }

        self.metrics.record_send(code, latency_us);
        self.shard_metrics.record_send(kind, code, latency_us);
        self.handle_tko(code, false);

        result
    }

    pub(crate) async fn send_probe(self: &Rc<Self>) {
        self.last_active.set(Instant::now());
        self.metrics.probes_sent.inc();

        let inflight = InflightGuard::new(&self.metrics);
        let result = self.conn.send_probe().await;
        drop(inflight);
        let code = code_of(&result);

        self.handle_tko(code, true);
    }

    fn handle_tko(self: &Rc<Self>, code: ResultCode, is_probe: bool) {
        if self.cfg.disable_tko_tracking {
            return;
        }

        if code.is_error() {
            if code.is_hard_tko_error() && self.tracker.record_hard_failure(self.token, code) {
                self.tracker.emit(TkoEvent::MarkHardTko, code, None);
                self.start_probing();
            } else if code.is_soft_tko_error() && self.tracker.record_soft_failure(self.token, code)
            {
                self.tracker.emit(TkoEvent::MarkSoftTko, code, None);
                self.start_probing();
            }
            return;
        }

        if self.tracker.is_tko() {
            if is_probe && self.tracker.record_success(self.token) {
                self.tracker.emit(TkoEvent::UnMarkTko, code, None);
                self.stop_probing();
            }
            return;
        }

        self.tracker.record_success(self.token);
    }

    fn on_conn_event(self: &Rc<Self>, ev: ConnectionEvent) {
        match ev {
            ConnectionEvent::Up => {
                self.metrics.connects.inc();
            }
            ConnectionEvent::Closed => {
                self.metrics.idle_closes.inc();
            }
            ConnectionEvent::Down(reason) => {
                let code = match reason {
                    DownReason::ConnectFailed(ConnectError::Timeout) => ResultCode::ConnectTimeout,
                    _ => ResultCode::ConnectError,
                };

                self.handle_tko(code, /* is_probe */ false);
            }
        }
    }

    fn start_probing(self: &Rc<Self>) {
        let task = tokio::task::spawn_local(probe::probe_loop(
            Rc::downgrade(self),
            self.cfg.probe_delay_initial,
            self.cfg.probe_delay_max,
        ));

        if let Some(prev) = self.probe.borrow_mut().replace(task) {
            prev.abort();
        }
    }

    fn stop_probing(&self) {
        self.metrics.probes_sent.set(0);
        if let Some(task) = self.probe.borrow_mut().take() {
            task.abort();
        }
    }
}

impl Backend for Rc<Destination> {
    fn prepare_send(
        &self,
        request: Request,
    ) -> Result<PreparedSend<impl Future<Output = Result<Reply, SendError>> + '_>, TkoRejection>
    {
        self.last_active.set(Instant::now());
        let kind = request.kind();

        if !self.cfg.disable_tko_tracking && self.tracker.is_tko() {
            self.metrics.record_result(ResultCode::Tko);
            self.shard_metrics.record_result(kind, ResultCode::Tko);
            // theres a small race window here
            // destination may be un/marked TKO immediately after check,
            // diagnostic reason may also change, but thats ok, TKO admission
            // is eventually consistent
            return Err(TkoRejection {
                reason: self.tracker.reason(),
            });
        }

        Ok(PreparedSend::new(async move {
            self.send_prepared(request, kind).await
        }))
    }
}

impl Drop for Destination {
    fn drop(&mut self) {
        // the ordering matters, first clear TKO ownership (since we need the
        // tracker is live and token meaningful), then kill the probe task
        if self.tracker.remove_destination(self.token) {
            self.tracker
                .emit(TkoEvent::RemoveFromConfig, self.tracker.reason(), None);
        }

        if let Some(task) = self.probe.borrow_mut().take() {
            task.abort();
        }
    }
}

// we need to decrement on drop so an aborted send (probe task abort) can't
// leak the inflight gauge
struct InflightGuard<'a>(&'a DestinationMetrics);

impl<'a> InflightGuard<'a> {
    fn new(metrics: &'a DestinationMetrics) -> Self {
        metrics.inflight_reqs.inc();
        Self(metrics)
    }
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.inflight_reqs.dec();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use rusty_mcrouter_protocol::test_support::{get, store};

    use super::*;
    use crate::destination::DestinationMetricsRegistry;
    use crate::test_support::{run_local, scripted_backend_serial, ScriptedServer, Step};
    use crate::tko::{TkoEventSink, TkoTrackerMap};

    async fn send(dest: &Rc<Destination>, request: Request) -> Result<Reply, SendError> {
        match dest.prepare_send(request) {
            Ok(prepared) => prepared.send().await,
            Err(rejection) => Err(rejection.into()),
        }
    }

    fn collecting_sink() -> (TkoEventSink, Arc<Mutex<Vec<TkoEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = {
            let events = Arc::clone(&events);
            TkoEventSink::new(move |rec: crate::tko::TkoEventRecord| {
                events.lock().unwrap().push(rec.event);
            })
        };
        (sink, events)
    }

    fn cfg(failures_until_tko: u64, reply_timeout_ms: u64, probe_initial_ms: u64) -> Config {
        Config {
            connect_timeout: Some(Duration::from_millis(1000)),
            reply_timeout: Some(Duration::from_millis(reply_timeout_ms)),
            connect_timeout_retries: 0,
            failures_until_tko,
            probe_delay_initial: Duration::from_millis(probe_initial_ms),
            probe_delay_max: Duration::from_millis(probe_initial_ms * 5),
            disable_tko_tracking: false,
        }
    }

    /// Tracker + destination wired the way Map does it, plus the event log.
    /// The TkoTrackerMap must stay alive (the tracker emits events through a
    /// Weak to it), so it is returned for the test to hold.
    #[allow(clippy::type_complexity)]
    fn dest_for(
        server: &ScriptedServer,
        cfg: Config,
    ) -> (
        Arc<TkoTrackerMap>,
        Arc<TkoTracker>,
        Rc<Destination>,
        Arc<Mutex<Vec<TkoEvent>>>,
    ) {
        let (sink, events) = collecting_sink();
        let map = TkoTrackerMap::with_sink(sink);
        let addr: Arc<str> = Arc::from(server.addr.to_string());
        let tracker = map.tracker_for(&addr, cfg.failures_until_tko);
        let metrics = DestinationMetricsRegistry::new().metrics_for(&tracker);
        let key = DestinationKey {
            addr,
            reply_timeout: cfg.reply_timeout,
        };
        let dest = Destination::new(
            key,
            cfg,
            Arc::clone(&tracker),
            metrics,
            BackendMetricsShard::new(),
        );
        (map, tracker, dest, events)
    }

    async fn wait_until(mut cond: impl FnMut() -> bool) {
        for _ in 0..2000 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("condition not met within 2s");
    }

    /// A marked destination fails fast: no connect, no write, no I/O at all.
    #[tokio::test]
    async fn tko_fast_fails_with_zero_backend_io() {
        run_local(async {
            let server =
                scripted_backend_serial(vec![vec![Step::ReadRequests(1), Step::Close]]).await;
            // probe delay far beyond test duration: probes stay out of frame
            let (_map, tracker, dest, _events) = dest_for(&server, cfg(3, 1000, 10_000));

            // mid-use close: request fails Dropped (non-TKO), the Down(Eof)
            // event is the hard evidence that marks instantly
            let r = send(&dest, get(b"a")).await;
            assert!(matches!(r, Err(SendError::Request(_))), "got {r:?}");
            wait_until(|| tracker.is_tko()).await;
            assert!(tracker.is_hard_tko());

            let accepts = server.accept_count();
            for _ in 0..5 {
                let r = send(&dest, get(b"x")).await;
                assert!(matches!(r, Err(SendError::Tko { .. })), "got {r:?}");
            }
            let prepared = dest.prepare_send(get(b"observed"));
            assert!(matches!(prepared, Err(TkoRejection { .. })));
            assert_eq!(server.accept_count(), accepts, "fast-fail must do zero I/O");
            assert_eq!(dest.metrics().result_count(ResultCode::Tko), 6);
        })
        .await;
    }

    /// The crown jewel: kill -> hard mark -> probe reconnects and unmarks ->
    /// traffic resumes. Event stream asserted exactly.
    #[tokio::test]
    async fn kill_probe_recover_roundtrip() {
        run_local(async {
            let server = scripted_backend_serial(vec![
                vec![Step::ReadRequests(1), Step::Close], // conn1: mid-use kill
                vec![Step::ReadRequests(1), Step::Write(b"VERSION 1.6.39\r\n")], // conn2: probe
                vec![Step::ReadRequests(1), Step::Write(b"EN\r\n")], // conn3: recovered traffic
            ])
            .await;
            let (_map, tracker, dest, events) = dest_for(&server, cfg(3, 1000, 20));

            let _ = send(&dest, get(b"a")).await;
            wait_until(|| tracker.is_tko()).await;

            // probe fires after ~20-30ms, reconnects, VERSION succeeds
            wait_until(|| !tracker.is_tko()).await;

            assert_eq!(
                *events.lock().unwrap(),
                vec![TkoEvent::MarkHardTko, TkoEvent::UnMarkTko]
            );
            assert_eq!(
                dest.metrics().probes_sent.load(),
                0,
                "probes_sent resets on unmark"
            );

            assert!(send(&dest, get(b"b")).await.is_ok());
            assert_eq!(server.accept_count(), 3);
        })
        .await;
    }

    /// Reply timeouts are soft evidence: they mark only at the CONSECUTIVE
    /// threshold, and the mark carries reason Timeout.
    #[tokio::test]
    async fn timeouts_mark_soft_at_threshold() {
        run_local(async {
            let server =
                scripted_backend_serial(vec![vec![Step::ReadRequests(2), Step::Hang]]).await;
            let (_map, tracker, dest, events) = dest_for(&server, cfg(2, 50, 10_000));

            let r = send(&dest, get(b"a")).await;
            assert!(matches!(
                r,
                Err(SendError::Request(crate::error::RequestError::Timeout {
                    sent: true
                }))
            ));
            assert!(!tracker.is_tko(), "one timeout of two must not mark");

            let _ = send(&dest, get(b"b")).await;
            wait_until(|| tracker.is_tko()).await;
            assert!(tracker.is_soft_tko());
            assert_eq!(tracker.reason(), ResultCode::Timeout);
            assert_eq!(*events.lock().unwrap(), vec![TkoEvent::MarkSoftTko]);

            // timeouts never tore the connection down
            assert_eq!(server.accept_count(), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn reply_timeout_also_bounds_socket_writes() {
        run_local(async {
            // Accept but never read, forcing a large batch to remain in write_all.
            let server = scripted_backend_serial(vec![vec![Step::Hang]]).await;
            let (_map, _tracker, dest, _events) = dest_for(&server, cfg(100, 50, 10_000));
            let request = store(b"key", &vec![b'x'; 1024 * 1024]);
            let start = Instant::now();
            let mut sends = Vec::new();

            for _ in 0..32 {
                let dest = Rc::clone(&dest);
                let request = request.clone();
                sends.push(tokio::task::spawn_local(async move {
                    let _ = send(&dest, request).await;
                }));
            }
            for send in sends {
                send.await.unwrap();
            }

            assert!(
                start.elapsed() < Duration::from_millis(500),
                "write used the 1s client default instead of the 50ms destination timeout: {:?}",
                start.elapsed()
            );
        })
        .await;
    }

    /// Config reload drops a TKO'd-and-responsible destination: the mark
    /// must not be orphaned on the shared tracker.
    #[tokio::test]
    async fn drop_while_responsible_unmarks_and_emits_remove_from_config() {
        run_local(async {
            let server =
                scripted_backend_serial(vec![vec![Step::ReadRequests(1), Step::Close]]).await;
            let (_map, tracker, dest, events) = dest_for(&server, cfg(3, 1000, 10_000));

            let _ = send(&dest, get(b"a")).await;
            wait_until(|| tracker.is_tko()).await;

            drop(dest);
            assert!(!tracker.is_tko(), "a dying owner must not orphan its TKO");
            assert_eq!(
                *events.lock().unwrap(),
                vec![TkoEvent::MarkHardTko, TkoEvent::RemoveFromConfig]
            );
        })
        .await;
    }

    /// Two destinations for the same server share one tracker: A's mark
    /// fast-fails B without B ever touching the network. (Same-thread stand-in
    /// for the cross-proxy-thread sharing the Arc<TkoTracker> exists for.)
    #[tokio::test]
    async fn two_destinations_share_one_verdict() {
        run_local(async {
            let server =
                scripted_backend_serial(vec![vec![Step::ReadRequests(1), Step::Close]]).await;
            let (_map, tracker, dest_a, _events) = dest_for(&server, cfg(3, 1000, 10_000));

            let addr_b: Arc<str> = Arc::from(server.addr.to_string());
            let metrics_b = DestinationMetricsRegistry::new().metrics_for(&tracker);
            let key_b = DestinationKey {
                addr: addr_b,
                reply_timeout: Some(Duration::from_millis(1000)),
            };
            let dest_b = Destination::new(
                key_b,
                cfg(3, 1000, 10_000),
                Arc::clone(&tracker),
                metrics_b,
                BackendMetricsShard::new(),
            );

            let _ = send(&dest_a, get(b"a")).await;
            wait_until(|| tracker.is_tko()).await;
            let accepts = server.accept_count();

            let r = send(&dest_b, get(b"b")).await;
            assert!(matches!(r, Err(SendError::Tko { .. })), "got {r:?}");
            assert_eq!(server.accept_count(), accepts, "B must never connect");
            assert_eq!(dest_b.metrics().result_count(ResultCode::Tko), 1);
        })
        .await;
    }

    /// sends record into the thread shard: {command x result} cell plus the
    /// latency sum - and a TKO fast-fail bumps its cell WITHOUT contributing
    /// latency (pins the record_result/record_send split).
    #[tokio::test]
    async fn send_records_into_the_thread_shard() {
        run_local(async {
            let server = scripted_backend_serial(vec![vec![
                Step::ReadRequests(1),
                Step::Write(b"EN\r\n"), // send 1: clean miss
                Step::ReadRequests(1),
                Step::Close, // send 2: mid-use kill -> hard mark
            ]])
            .await;

            let (sink, _events) = collecting_sink();
            let map = TkoTrackerMap::with_sink(sink);
            let addr: Arc<str> = Arc::from(server.addr.to_string());
            let tracker = map.tracker_for(&addr, 3);
            let metrics = DestinationMetricsRegistry::new().metrics_for(&tracker);
            let shard = BackendMetricsShard::new();
            let key = DestinationKey {
                addr,
                reply_timeout: Some(Duration::from_millis(1000)),
            };
            let dest = Destination::new(
                key,
                cfg(3, 1000, 10_000),
                Arc::clone(&tracker),
                metrics,
                Arc::clone(&shard),
            );

            let get_cell = |code: ResultCode| {
                shard.requests[rusty_mcrouter_protocol::RequestKind::Get as usize][code as usize]
                    .load()
            };

            send(&dest, get(b"a")).await.unwrap();
            assert_eq!(get_cell(ResultCode::Success), 1);
            let latency_after_success = shard.latency_us_sum.load();
            assert!(latency_after_success > 0, "a real send must record latency");

            let _ = send(&dest, get(b"b")).await; // killed mid-use
            wait_until(|| tracker.is_tko()).await;
            let latency_after_mark = shard.latency_us_sum.load();

            let r = send(&dest, get(b"c")).await;
            assert!(matches!(r, Err(SendError::Tko { .. })), "got {r:?}");
            assert_eq!(get_cell(ResultCode::Tko), 1);
            assert_eq!(
                shard.latency_us_sum.load(),
                latency_after_mark,
                "fast-fail must not contribute latency"
            );
        })
        .await;
    }

    /// THE guard test: a send future dropped mid-await (aborted task - the
    /// probe-abort path is the production case) must not leak the inflight
    /// gauge. Without InflightGuard this wedges at 1 forever.
    #[tokio::test]
    async fn inflight_gauge_survives_task_abort() {
        run_local(async {
            // accepts, reads the request, never replies - the send parks
            let server =
                scripted_backend_serial(vec![vec![Step::ReadRequests(1), Step::Hang]]).await;
            // huge reply timeout: the future can only end by abort
            let (_map, _tracker, dest, _events) = dest_for(&server, cfg(100, 10_000, 10_000));
            let metrics = Arc::clone(dest.metrics());

            let task = {
                let dest = Rc::clone(&dest);
                tokio::task::spawn_local(async move {
                    let _ = send(&dest, get(b"a")).await;
                })
            };
            wait_until(|| metrics.inflight_reqs.load() == 1).await;

            task.abort();
            wait_until(|| metrics.inflight_reqs.load() == 0).await;
        })
        .await;
    }

    /// the boring sibling: a send that completes normally also settles the
    /// gauge back to zero.
    #[tokio::test]
    async fn inflight_gauge_settles_after_completed_send() {
        run_local(async {
            let server =
                scripted_backend_serial(vec![vec![Step::ReadRequests(1), Step::Write(b"EN\r\n")]])
                    .await;
            let (_map, _tracker, dest, _events) = dest_for(&server, cfg(100, 1000, 10_000));

            send(&dest, get(b"a")).await.unwrap();
            assert_eq!(dest.metrics().inflight_reqs.load(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn probe_is_not_counted_as_a_routed_request() {
        run_local(async {
            let server = scripted_backend_serial(vec![vec![
                Step::ReadRequests(1),
                Step::Write(b"VERSION 1.6.39\r\n"),
            ]])
            .await;
            let (_map, _tracker, dest, _events) = dest_for(&server, cfg(3, 1000, 10_000));

            dest.send_probe().await;

            let backend_requests: u64 = dest
                .shard_metrics
                .requests
                .iter()
                .flatten()
                .map(|counter| counter.load())
                .sum();
            let destination_requests: u64 = dest
                .metrics
                .requests
                .iter()
                .map(|counter| counter.load())
                .sum();

            assert_eq!(backend_requests, 0);
            assert_eq!(destination_requests, 0);
            assert_eq!(dest.shard_metrics.latency_us_sum.load(), 0);
            assert_eq!(dest.metrics.latency_us_sum.load(), 0);
            assert_eq!(dest.metrics.probes_sent.load(), 1);
        })
        .await;
    }
}
