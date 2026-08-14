use std::{sync::Arc, time::Duration};

use rusty_mcrouter_protocol::{Reply, Request};
use tokio::{
    sync::{
        mpsc::{self, error::TrySendError},
        oneshot,
    },
    time::Instant,
};

use crate::{
    client::{
        config::Config,
        connection::Connection,
        types::{Command, ConnectionCommand, ConnectionEvent, Payload},
    },
    error::SendError,
};

#[derive(Clone)]
pub struct ConnectionHandle {
    tx: mpsc::Sender<ConnectionCommand>,
    reply_timeout: Option<Duration>,
}

impl ConnectionHandle {
    pub fn spawn(
        addr: Arc<str>,
        cfg: Config,
        events: Box<dyn Fn(ConnectionEvent)>,
    ) -> ConnectionHandle {
        let (tx, rx) = mpsc::channel(cfg.max_pending);
        let reply_timeout = cfg.reply_timeout;

        tokio::task::spawn_local(Connection::new(addr, cfg, rx, events).run());

        ConnectionHandle { tx, reply_timeout }
    }

    pub async fn send(&self, request: Request) -> Result<Reply, SendError> {
        self.submit(Payload::Request(request)).await
    }

    pub async fn send_probe(&self) -> Result<Reply, SendError> {
        self.submit(Payload::VersionProbe).await
    }

    async fn submit(&self, payload: Payload) -> Result<Reply, SendError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        let cmd = Command {
            payload,
            reply_tx,
            deadline: self.reply_timeout.map(|d| Instant::now() + d),
        };

        // fail fast on a full queue
        self.tx
            .try_send(ConnectionCommand::Command(cmd))
            .map_err(|e| match e {
                TrySendError::Full(_) => SendError::Local(crate::error::LocalError::QueueFull),
                TrySendError::Closed(_) => SendError::Local(crate::error::LocalError::Shutdown),
            })?;
        reply_rx
            .await
            .unwrap_or(Err(SendError::Local(crate::error::LocalError::Shutdown)))
    }

    /// Fire-and-forget from the idle sweep. try_send, not send: the async
    /// send would need an await (the previous version dropped its future
    /// unawaited, sending nothing), and a full queue is proof we're not
    /// idle anyway.
    #[allow(dead_code)] // production caller is DestinationMap's idle sweep (next step)
    pub(crate) fn close_idle(&self) {
        let _ = self.tx.try_send(ConnectionCommand::CloseIdle);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use bytes::Bytes;
    use rusty_mcrouter_protocol::reply::GetReply;
    use rusty_mcrouter_protocol::test_support::get;
    use rusty_mcrouter_protocol::Reply;

    use super::*;
    use crate::client::types::DownReason;
    use crate::error::{ConnectError, RequestError};
    use crate::test_support::{
        event_log, run_local, scripted_backend_serial, ScriptedServer, Step,
    };

    fn spawn_to(
        server: &ScriptedServer,
        cfg: Config,
    ) -> (ConnectionHandle, Rc<RefCell<Vec<ConnectionEvent>>>) {
        let (sink, log) = event_log();
        let handle = ConnectionHandle::spawn(Arc::from(server.addr.to_string()), cfg, sink);
        (handle, log)
    }

    fn hit_data(reply: Reply) -> Bytes {
        let Reply::Get(GetReply::Hit(hit)) = reply else {
            panic!("expected get hit, got {reply:?}");
        };
        hit.value.expect("hit with value")
    }

    async fn wait_for(log: &Rc<RefCell<Vec<ConnectionEvent>>>, ev: &ConnectionEvent) {
        while !log.borrow().contains(ev) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[tokio::test]
    async fn lazy_connect_no_io_until_first_send() {
        run_local(async {
            let server = scripted_backend_serial(vec![vec![
                Step::ReadRequests(1),
                Step::Write(b"EN\r\n"),
            ]])
            .await;
            let (handle, _log) = spawn_to(&server, Config::default());

            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(server.accept_count(), 0, "spawn must perform no I/O");

            assert_eq!(
                handle.send(get(b"a")).await.unwrap(),
                Reply::Get(GetReply::Miss)
            );
            assert_eq!(server.accept_count(), 1);
        })
        .await;
    }

    /// THE D1 regression test: an idle remote close is benign — Closed, no
    /// Down, and the next send silently reconnects. (mcrouter hard-TKOs
    /// idle EOFs; this divergence is deliberate.)
    #[tokio::test]
    async fn idle_eof_is_benign_and_reconnects() {
        run_local(async {
            let server = scripted_backend_serial(vec![
                vec![Step::ReadRequests(1), Step::Write(b"EN\r\n"), Step::Close],
                vec![Step::ReadRequests(1), Step::Write(b"EN\r\n")],
            ])
            .await;
            let (handle, log) = spawn_to(&server, Config::default());

            handle.send(get(b"a")).await.unwrap();
            // wait until the actor OBSERVES the idle EOF; racing into the
            // next send would exercise the mid-use path instead
            wait_for(&log, &ConnectionEvent::Closed).await;

            handle.send(get(b"b")).await.unwrap();

            assert_eq!(server.accept_count(), 2);
            assert!(
                !log.borrow()
                    .iter()
                    .any(|e| matches!(e, ConnectionEvent::Down(_))),
                "idle close must never produce Down: {:?}",
                log.borrow(),
            );
        })
        .await;
    }

    /// The faithful contrast to the test above: a close with a request
    /// inflight IS health evidence — the caller fails Dropped and Down(Eof)
    /// fires (mcrouter: REMOTE_ERROR + onDown -> handleTko).
    #[tokio::test]
    async fn mid_use_drop_fails_dropped_and_emits_down() {
        run_local(async {
            let server =
                scripted_backend_serial(vec![vec![Step::ReadRequests(1), Step::Close]]).await;
            let (handle, log) = spawn_to(&server, Config::default());

            let result = handle.send(get(b"a")).await;
            assert!(
                matches!(
                    result,
                    Err(SendError::Request(RequestError::Dropped { .. }))
                ),
                "got {result:?}"
            );
            wait_for(&log, &ConnectionEvent::Down(DownReason::Eof)).await;
        })
        .await;
    }

    /// The tombstone triad end to end: expire_deadlines tombstones B,
    /// next_deadline skips it, deliver_replies consumes B's LATE reply so
    /// C's caller gets C — and the connection survives the timeout
    /// (accept_count stays 1).
    ///
    /// Real time, not start_paused: paused-time auto-advance races real
    /// loopback I/O (the old suite documented the same hazard).
    #[tokio::test]
    async fn late_reply_to_timed_out_request_keeps_fifo_aligned() {
        run_local(async {
            let server = scripted_backend_serial(vec![vec![
                Step::ReadRequests(1),
                Step::Write(b"VA 1\r\nA\r\n"),
                Step::ReadRequests(3), // B and C arrive; no reply until C is in
                Step::Write(b"VA 1\r\nB\r\n"), // B's late reply
                Step::Write(b"VA 1\r\nC\r\n"),
            ]])
            .await;
            let cfg = Config {
                reply_timeout: Some(Duration::from_millis(100)),
                ..Config::default()
            };
            let (handle, _log) = spawn_to(&server, cfg);

            assert_eq!(hit_data(handle.send(get(b"k")).await.unwrap()).as_ref(), b"A");

            let b = handle.send(get(b"k")).await;
            assert!(
                matches!(
                    b,
                    Err(SendError::Request(RequestError::Timeout { sent: true }))
                ),
                "got {b:?}"
            );

            // without the tombstone, C's caller would receive "B" here
            assert_eq!(hit_data(handle.send(get(b"k")).await.unwrap()).as_ref(), b"C");
            assert_eq!(
                server.accept_count(),
                1,
                "a reply timeout must not tear down the connection"
            );
        })
        .await;
    }

    /// Refused is a definitive kernel answer: fail immediately, burn no
    /// retry budget (mcrouter retries connect TIMEOUTS only,
    /// AsyncMcClientImpl.cpp:574-577).
    #[tokio::test]
    async fn connect_refused_fails_fast_without_retry() {
        run_local(async {
            // bind-then-drop: the port is (almost certainly) unbound
            let addr = {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                listener.local_addr().unwrap()
            };
            let cfg = Config {
                connect_timeout: Some(Duration::from_secs(5)),
                connect_timeout_retries: 3, // must NOT be consumed by refusal
                ..Config::default()
            };
            let (sink, log) = event_log();
            let handle = ConnectionHandle::spawn(Arc::from(addr.to_string()), cfg, sink);

            let start = Instant::now();
            let result = handle.send(get(b"a")).await;
            assert!(
                matches!(result, Err(SendError::Connect(ConnectError::Failed(_)))),
                "got {result:?}"
            );
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "refused must not wait out connect_timeout or retries"
            );
            assert!(log
                .borrow()
                .iter()
                .any(|e| matches!(e, ConnectionEvent::Down(DownReason::ConnectFailed(_)))));
        })
        .await;
    }

    /// Retries apply to connect timeouts ONLY, and each one costs a full
    /// connect_timeout: total wait = connect_timeout * (retries + 1).
    /// 192.0.2.0/24 is TEST-NET-1 (RFC 5737): routed and dropped, so the
    /// connect hangs until the deadline.
    #[tokio::test(start_paused = true)]
    async fn connect_timeout_consumes_retries_then_fails() {
        run_local(async {
            let cfg = Config {
                connect_timeout: Some(Duration::from_millis(100)),
                connect_timeout_retries: 2,
                ..Config::default()
            };
            let (sink, log) = event_log();
            let handle = ConnectionHandle::spawn(Arc::from("192.0.2.1:12345"), cfg, sink);

            let start = Instant::now();
            let result = handle.send(get(b"a")).await;
            assert!(
                matches!(result, Err(SendError::Connect(ConnectError::Timeout))),
                "got {result:?}"
            );
            assert!(
                start.elapsed() >= Duration::from_millis(300),
                "2 retries must cost 3 connect_timeouts, elapsed {:?}",
                start.elapsed()
            );
            assert!(log.borrow().contains(&ConnectionEvent::Down(
                DownReason::ConnectFailed(ConnectError::Timeout)
            )));
        })
        .await;
    }

    /// A malformed reply poisons the stream: the request fails with the
    /// real decode error, Down(Protocol) fires — and unlike the old client,
    /// the next send gets a fresh connection instead of ClientClosed.
    #[tokio::test]
    async fn protocol_error_tears_down_and_reconnects() {
        run_local(async {
            let server = scripted_backend_serial(vec![
                vec![Step::ReadRequests(1), Step::Write(b"WAT\r\n")],
                vec![Step::ReadRequests(1), Step::Write(b"EN\r\n")],
            ])
            .await;
            let (handle, log) = spawn_to(&server, Config::default());

            let poisoned = handle.send(get(b"a")).await;
            assert!(
                matches!(
                    poisoned,
                    Err(SendError::Protocol(crate::error::ProtocolError::Decode(_)))
                ),
                "got {poisoned:?}"
            );
            wait_for(&log, &ConnectionEvent::Down(DownReason::Protocol)).await;

            assert_eq!(
                handle.send(get(b"b")).await.unwrap(),
                Reply::Get(GetReply::Miss)
            );
            assert_eq!(server.accept_count(), 2);
        })
        .await;
    }

    /// Dropping every handle closes the channel; the actor exits instead of
    /// reconnecting. (Indirect observation: no further accepts occur even
    /// though a second script is available.)
    #[tokio::test]
    async fn dropping_handle_stops_actor() {
        run_local(async {
            let server = scripted_backend_serial(vec![
                vec![Step::ReadRequests(1), Step::Write(b"EN\r\n"), Step::Close],
                vec![Step::ReadRequests(1), Step::Write(b"EN\r\n")],
            ])
            .await;
            let (handle, _log) = spawn_to(&server, Config::default());

            handle.send(get(b"a")).await.unwrap();
            drop(handle);

            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(server.accept_count(), 1, "dead actor must not reconnect");
        })
        .await;
    }
}
