use std::sync::Arc;
use std::{collections::BTreeMap, rc::Rc};

use bytes::{Bytes, BytesMut};
use rusty_mcrouter_core::{DynRoute, RoutingState};
use rusty_mcrouter_protocol::meta::{
    DecodedMetaCommand, MetaReplyEncoder, MetaReplyPlan, MetaRequestDecodeError, MetaRequestDecoder,
};
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::mpsc,
};

use crate::{
    config::ThreadMode, proxy_set::ProxySet, routing::complete_route, FrontendError,
    FrontendMetricsShard, ProxyHandle,
};

const READ_BUF_INITIAL_CAPACITY: usize = 4096;
const COMPLETED_CHANNEL_CAPACITY: usize = 1024;

/// one client connection's lifecycle:
/// - decode pipelined Meta commands
/// - dispatch routable requests to a proxy (local inline or remote via the
///   proxy queue); answer `mn` and recoverable parse errors locally
/// - encode replies against each slot's retained reply plan, in request order
pub struct Connection {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    // routing context (set at creation time)
    current_id: usize,
    local_route: Rc<dyn DynRoute>,
    routing_state: Rc<RoutingState>,
    proxies: ProxySet,
    mode: ThreadMode,
    // pipeline state
    buf: BytesMut,
    write_buf: BytesMut,
    decoder: MetaRequestDecoder,
    encoder: MetaReplyEncoder,
    /// The only per-request state. A slot is created at decode time with its
    /// hop-local `MetaReplyPlan` (never routed, never crosses threads) and
    /// flips to `Ready` when its outcome exists.
    slots: BTreeMap<usize, Slot>,
    metrics: Arc<FrontendMetricsShard>,
    next_seq: usize,
    next_write: usize,
    in_flight: usize,
    input_closed: bool,
    completed_tx: mpsc::Sender<(usize, Reply)>,
    completed_rx: mpsc::Receiver<(usize, Reply)>,
}

struct Slot {
    plan: MetaReplyPlan,
    state: SlotState,
}

enum SlotState {
    InFlight,
    Ready(SlotOutcome),
}

enum SlotOutcome {
    Reply(Reply),
    /// `mn`: session-local, answered with `MN` in pipeline order.
    NoOp,
}

impl Connection {
    pub fn new(
        stream: tokio::net::TcpStream,
        current_id: usize,
        local_route: Rc<dyn DynRoute>,
        routing_state: Rc<RoutingState>,
        proxies: ProxySet,
        mode: ThreadMode,
        metrics: Arc<FrontendMetricsShard>,
    ) -> Self {
        let (reader, writer) = stream.into_split();
        let (completed_tx, completed_rx) = mpsc::channel(COMPLETED_CHANNEL_CAPACITY);

        Self {
            reader,
            writer,
            current_id,
            local_route,
            routing_state,
            proxies,
            mode,
            buf: BytesMut::with_capacity(READ_BUF_INITIAL_CAPACITY),
            write_buf: BytesMut::new(),
            decoder: MetaRequestDecoder::new(),
            encoder: MetaReplyEncoder::new(),
            slots: BTreeMap::new(),
            metrics,
            next_seq: 0,
            next_write: 0,
            in_flight: 0,
            input_closed: false,
            completed_tx,
            completed_rx,
        }
    }

    pub async fn run(mut self) -> Result<(), FrontendError> {
        loop {
            if !self.input_closed {
                self.drain_input();
            }

            self.flush_ready().await?;

            if self.input_closed && self.slots.is_empty() {
                return Ok(());
            }

            // select! touches reader/buf and completed_rx as disjoint fields
            // directly; the two arms can't be factored into &mut self methods.
            tokio::select! {
                read = self.reader.read_buf(&mut self.buf), if !self.input_closed => {
                    if read? == 0 {
                        self.input_closed = true;
                        // A partial frame at EOF has no valid answer; drain
                        // whatever is already in flight, then close.
                        // todo - logger for decode_eof violations
                        let _ = self.decoder.decode_eof(&self.buf);
                    }
                }
                maybe_completed = self.completed_rx.recv(), if self.in_flight > 0 => {
                    match maybe_completed {
                        Some((seq, reply)) => {
                            self.complete(seq, reply);
                            while let Ok((seq, reply)) = self.completed_rx.try_recv() {
                                self.complete(seq, reply);
                            }
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    /// decode every complete command currently buffered and act on it without
    /// waiting for replies (pipelining).
    fn drain_input(&mut self) {
        loop {
            match self.decoder.decode(&mut self.buf) {
                Ok(Some(DecodedMetaCommand::Request {
                    request,
                    reply_plan,
                })) => {
                    self.metrics.requests[request.kind() as usize].inc();
                    self.metrics.processing.inc();
                    let seq = self.take_seq();
                    self.slots.insert(
                        seq,
                        Slot {
                            plan: reply_plan,
                            state: SlotState::InFlight,
                        },
                    );
                    self.in_flight += 1;
                    self.submit_single(seq, request);
                }
                Ok(Some(DecodedMetaCommand::NoOp)) => {
                    self.metrics.noops.inc();
                    let seq = self.take_seq();
                    self.slots.insert(seq, Slot::ready(SlotOutcome::NoOp));
                }
                Ok(None) => return,
                // one malformed command was consumed; its error joins the
                // pipeline in order and decoding continues.
                Err(MetaRequestDecodeError::Recoverable(error)) => {
                    self.metrics.parse_errors.inc();
                    let seq = self.take_seq();
                    self.slots
                        .insert(seq, Slot::ready(SlotOutcome::Reply(Reply::Error(error))));
                }
                // frame alignment is untrustworthy: stop consuming input,
                // finish what is owed, then close.
                Err(MetaRequestDecodeError::Fatal(_)) => {
                    self.input_closed = true;
                    return;
                }
            }
        }
    }

    fn take_seq(&mut self) -> usize {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        seq
    }

    fn complete(&mut self, seq: usize, reply: Reply) {
        self.in_flight = self.in_flight.saturating_sub(1);
        self.metrics.processing.dec();
        if let Some(slot) = self.slots.get_mut(&seq) {
            slot.state = SlotState::Ready(SlotOutcome::Reply(reply));
        }
    }

    /// resolves `req`'s target
    /// - which proxy handles it
    /// - if its the same thread
    /// - the local route
    fn route_target(&self, request: &Request) -> RouteTarget {
        let handle = self.proxies.choose(self.mode, self.current_id, request);
        if handle.id() == self.current_id {
            RouteTarget::Local {
                route: Rc::clone(&self.local_route),
                routing_state: Rc::clone(&self.routing_state),
            }
        } else {
            RouteTarget::Remote { handle }
        }
    }

    fn submit_single(&self, seq: usize, request: Request) {
        let target = self.route_target(&request);
        let completed_tx = self.completed_tx.clone();

        tokio::task::spawn_local(async move {
            let reply = route_one(target, request).await;

            let _ = completed_tx.send((seq, reply)).await;
        });
    }

    /// flush replies that are ready in request order, advancing `next_write`.
    /// A suppressed (quiet) reply writes nothing but still advances.
    async fn flush_ready(&mut self) -> Result<(), FrontendError> {
        self.write_buf.clear();
        while matches!(
            self.slots.get(&self.next_write),
            Some(Slot {
                state: SlotState::Ready(_),
                ..
            })
        ) {
            let slot = self.slots.remove(&self.next_write).expect("checked above");
            let SlotState::Ready(outcome) = slot.state else {
                unreachable!("matched Ready above");
            };
            match outcome {
                SlotOutcome::NoOp => self.encoder.encode_noop(&mut self.write_buf),
                SlotOutcome::Reply(reply) => {
                    if matches!(reply, Reply::Error(_)) {
                        self.metrics.failed.inc();
                    }
                    if self
                        .encoder
                        .encode(&reply, &slot.plan, &mut self.write_buf)
                        .is_err()
                    {
                        if !matches!(reply, Reply::Error(_)) {
                            self.metrics.failed.inc();
                        }
                        // the reply cannot satisfy this slot's plan (for
                        // example a backend omitted a projected field):
                        // degrade this slot only, never the connection.
                        let fallback = Reply::Error(ErrorReply::Server(Some(Bytes::from_static(
                            b"proxy reply encoding failed",
                        ))));
                        let _ = self.encoder.encode(
                            &fallback,
                            &MetaReplyPlan::default(),
                            &mut self.write_buf,
                        );
                    }
                }
            }
            self.next_write = self.next_write.wrapping_add(1);
        }
        if !self.write_buf.is_empty() {
            self.writer.write_all(&self.write_buf).await?;
        }
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.metrics.processing.sub(self.in_flight as i64);
    }
}

impl Slot {
    fn ready(outcome: SlotOutcome) -> Self {
        Self {
            plan: MetaReplyPlan::default(),
            state: SlotState::Ready(outcome),
        }
    }
}

enum RouteTarget {
    Local {
        route: Rc<dyn DynRoute>,
        routing_state: Rc<RoutingState>,
    },
    Remote {
        handle: ProxyHandle,
    },
}

async fn route_one(target: RouteTarget, request: Request) -> Reply {
    match target {
        RouteTarget::Local {
            route,
            routing_state,
        } => {
            let context = routing_state.context();
            let result = route.route_dyn(&context, request).await;
            complete_route(context, result)
        }
        RouteTarget::Remote { handle } => handle.send_request(request).await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rusty_mcrouter_backend::destination;
    use rusty_mcrouter_backend::test_support::{run_local, MockBackendFactory};
    use rusty_mcrouter_config::parse;
    use rusty_mcrouter_core::{build_route, RoutingMetricsLayout, RoutingMetricsShard};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::message::{ProxyCommand, ProxyRequest};

    /// a real Connection over a localhost socket pair, with a SameThread
    /// route into a mock backend. the proxy handle channel is never used
    /// (SameThread routes inline) but ProxySet demands one.
    async fn session(
        metrics: Arc<FrontendMetricsShard>,
    ) -> (
        tokio::net::TcpStream,
        tokio::task::JoinHandle<()>,
        Arc<RoutingMetricsShard>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let config =
            parse(r#"{"pools": {"pool": {"servers": ["unused:1"]}}, "route": "PoolRoute|pool"}"#)
                .unwrap();
        let layout = RoutingMetricsLayout::new(["pool".to_string()]);
        let routing_metrics = RoutingMetricsShard::new(layout);
        let routing_state = RoutingState::new(Arc::clone(&routing_metrics));
        let route = build_route(
            &config,
            &MockBackendFactory::new(),
            &destination::Config::default(),
            routing_state.layout(),
        )
        .unwrap();
        let (request_tx, _request_rx) = mpsc::channel::<ProxyRequest>(1);
        let (command_tx, _command_rx) = mpsc::channel::<ProxyCommand>(1);
        let proxies = ProxySet::new(vec![ProxyHandle::new(0, request_tx, command_tx)]);

        let conn = Connection::new(
            server_stream,
            0,
            route,
            routing_state,
            proxies,
            ThreadMode::SameThread,
            metrics,
        );
        let task = tokio::task::spawn_local(async move {
            let _ = conn.run().await;
        });
        (client, task, routing_metrics)
    }

    async fn read_lines(client: &mut tokio::net::TcpStream, n: usize) -> Vec<String> {
        let mut buf = Vec::new();
        loop {
            let text = String::from_utf8_lossy(&buf);
            if text.matches("\r\n").count() >= n {
                return text.split("\r\n").take(n).map(str::to_owned).collect();
            }
            let mut chunk = [0u8; 1024];
            let read = client.read(&mut chunk).await.unwrap();
            assert!(read > 0, "connection closed before {n} replies");
            buf.extend_from_slice(&chunk[..read]);
        }
    }

    /// THE frontend metrics test: a pipelined session of mg + mn + garbage
    /// counts one of each fact, failed counts the CLIENT_ERROR, replies come
    /// back in pipeline order, and the gauges settle to zero.
    #[tokio::test]
    async fn frontend_metrics_account_a_pipelined_session() {
        run_local(async {
            let metrics = FrontendMetricsShard::new();
            let (mut client, task, routing_metrics) = session(Arc::clone(&metrics)).await;

            client
                .write_all(b"mg foo v\r\nmn\r\nnot_a_command\r\n")
                .await
                .unwrap();

            let lines = read_lines(&mut client, 3).await;
            assert_eq!(lines[0], "EN", "mg miss");
            assert_eq!(lines[1], "MN", "mn answered in pipeline order");
            // unknown command -> memcached's bare ERROR (CLIENT_ERROR is for
            // malformed KNOWN commands); either way it's a recoverable parse
            // error and a client-visible error reply
            assert_eq!(lines[2], "ERROR", "garbage must answer in pipeline order");

            assert_eq!(
                metrics.requests[rusty_mcrouter_protocol::RequestKind::Get as usize].load(),
                1
            );
            assert_eq!(metrics.noops.load(), 1);
            assert_eq!(metrics.parse_errors.load(), 1);
            assert_eq!(
                metrics.failed.load(),
                1,
                "the CLIENT_ERROR is a client-visible error reply"
            );
            assert_eq!(metrics.processing.load(), 0);
            assert_eq!(routing_metrics.pools[0].requests.load(), 1);
            assert_eq!(routing_metrics.pools[0].completed_requests.load(), 1);
            assert_eq!(routing_metrics.pools[0].final_errors.load(), 0);

            // client disconnect ends the session; the gauge must not leak
            drop(client);
            task.await.unwrap();
            assert_eq!(metrics.processing.load(), 0);
        })
        .await;
    }
}
