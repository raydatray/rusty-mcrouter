use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};
use rusty_mcrouter_protocol::meta::{
    DecodedMetaCommand, MetaReplyEncoder, MetaReplyPlan, MetaRequestDecodeError, MetaRequestDecoder,
};
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
};

use crate::context::ProxyContext;
use crate::routing::dispatch;
use crate::{FrontendConnectionOptions, FrontendError};

pub(crate) struct FrontendConnectionSetup {
    pub(crate) stream: tokio::net::TcpStream,
    pub(crate) context: ProxyContext,
    pub(crate) options: FrontendConnectionOptions,
}

/// one client connection's lifecycle:
/// - decode pipelined Meta commands
/// - dispatch routable requests to a proxy (local inline or remote via the
///   proxy queue); answer `mn` and recoverable parse errors locally
/// - encode replies against each slot's retained reply plan, in request order
pub(crate) struct Connection {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    context: ProxyContext,
    // pipeline state
    buf: BytesMut,
    write_buf: BytesMut,
    decoder: MetaRequestDecoder,
    encoder: MetaReplyEncoder,
    /// The only per-request state. A slot is created at decode time with its
    /// hop-local `MetaReplyPlan` (never routed, never crosses threads) and
    /// flips to `Ready` when its outcome exists.
    slots: BTreeMap<usize, Slot>,
    next_seq: usize,
    next_write: usize,
    in_flight: usize,
    input_closed: bool,
    completed_tx: Sender<(usize, Reply)>,
    completed_rx: Receiver<(usize, Reply)>,
    requests: JoinSet<()>,
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
    pub(crate) fn new(setup: FrontendConnectionSetup) -> Self {
        let FrontendConnectionSetup {
            stream,
            context,
            options,
        } = setup;
        let (reader, writer) = stream.into_split();
        let (completed_tx, completed_rx) = mpsc::channel(options.completed_capacity);
        context.metrics.client_connections.inc();

        Self {
            reader,
            writer,
            context,
            buf: BytesMut::with_capacity(options.read_buf_initial_capacity),
            write_buf: BytesMut::new(),
            decoder: MetaRequestDecoder::new(),
            encoder: MetaReplyEncoder::new(),
            slots: BTreeMap::new(),
            next_seq: 0,
            next_write: 0,
            in_flight: 0,
            input_closed: false,
            completed_tx,
            completed_rx,
            requests: JoinSet::new(),
        }
    }

    pub(crate) async fn run(mut self) -> Result<(), FrontendError> {
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
                Some(result) = self.requests.join_next(), if !self.requests.is_empty() => {
                    result?;
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
                    self.context.metrics.requests[request.kind() as usize].inc();
                    self.context.metrics.processing.inc();
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
                    self.context.metrics.noops.inc();
                    let seq = self.take_seq();
                    self.slots.insert(seq, Slot::ready(SlotOutcome::NoOp));
                }
                Ok(None) => return,
                // one malformed command was consumed; its error joins the
                // pipeline in order and decoding continues.
                Err(MetaRequestDecodeError::Recoverable(error)) => {
                    self.context.metrics.parse_errors.inc();
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
        self.context.metrics.processing.dec();
        if let Some(slot) = self.slots.get_mut(&seq) {
            slot.state = SlotState::Ready(SlotOutcome::Reply(reply));
        }
    }

    fn submit_single(&mut self, seq: usize, request: Request) {
        let target = self.context.target(&request);
        let completed_tx = self.completed_tx.clone();

        self.requests.spawn_local(async move {
            let reply = dispatch(target, request).await;

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
                        self.context.metrics.failed.inc();
                    }
                    if self
                        .encoder
                        .encode(&reply, &slot.plan, &mut self.write_buf)
                        .is_err()
                    {
                        if !matches!(reply, Reply::Error(_)) {
                            self.context.metrics.failed.inc();
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
        let metrics = &self.context.metrics;
        metrics.processing.sub(self.in_flight as i64);
        metrics.client_connections.dec();
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

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Arc;

    use rusty_mcrouter_backend::destination;
    use rusty_mcrouter_backend::test_support::{run_local, MockBackendFactory};
    use rusty_mcrouter_config::{parse, PoolId};
    use rusty_mcrouter_core::{build_route, RoutingMetricsShard, RoutingState};
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::generation::{RouteGeneration, RouteSlot};
    use crate::{FrontendMetricsShard, ProxyHandle};

    fn generation(generation: u64, config: &str) -> Rc<RouteGeneration> {
        let config = parse(config).unwrap();
        let route = build_route(
            &config,
            &MockBackendFactory::new(),
            &destination::DestinationConfig::default(),
        )
        .unwrap();
        let state = RoutingState::new(RoutingMetricsShard::new(), Rc::new(noop_sink()), &config);
        Rc::new(RouteGeneration {
            generation,
            route,
            state,
        })
    }

    const POOL_CONFIG: &str =
        r#"{"pools": {"pool": {"servers": ["unused:1"]}}, "route": "PoolRoute|pool"}"#;

    fn pool_id() -> PoolId {
        parse(POOL_CONFIG).unwrap().pool_id("pool").unwrap()
    }

    /// a real Connection over a localhost socket pair, with a SameThread
    /// route into a mock backend. the proxy handle channel is never used
    /// (SameThread routes inline) but ProxySet demands one.
    async fn session(
        metrics: Arc<FrontendMetricsShard>,
        routes: Rc<RouteSlot>,
    ) -> (tokio::net::TcpStream, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let (handle, _inbox) = ProxyHandle::allocate(0);
        let conn = Connection::new(FrontendConnectionSetup {
            stream: server_stream,
            context: ProxyContext::solo(handle, routes, metrics),
            options: FrontendConnectionOptions::default(),
        });
        let task = tokio::task::spawn_local(async move {
            let _ = conn.run().await;
        });
        (client, task)
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
            let routes = RouteSlot::new(generation(1, POOL_CONFIG));
            let routing = Rc::clone(&routes.current().state);
            let pool = pool_id();
            let (mut client, task) = session(Arc::clone(&metrics), routes).await;

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
            assert_eq!(metrics.client_connections.load(), 1);
            assert_eq!(routing.pool(pool).requests.load(), 1);
            assert_eq!(routing.pool(pool).completed_requests.load(), 1);
            assert_eq!(routing.pool(pool).final_errors.load(), 0);

            // client disconnect ends the session; the gauges must not leak
            drop(client);
            task.await.unwrap();
            assert_eq!(metrics.processing.load(), 0);
            assert_eq!(metrics.client_connections.load(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn existing_connection_uses_the_new_generation_on_its_next_request() {
        run_local(async {
            let routes = RouteSlot::new(generation(1, r#"{"route": "NullRoute"}"#));
            let (mut client, _task) =
                session(FrontendMetricsShard::new(), Rc::clone(&routes)).await;

            client.write_all(b"mg foo v\r\n").await.unwrap();
            assert_eq!(read_lines(&mut client, 1).await, ["EN"]);

            let previous = routes.replace(generation(2, r#"{"route": "ErrorRoute|two"}"#));
            assert_eq!(previous.generation, 1);
            drop(previous);

            client.write_all(b"mg foo v\r\n").await.unwrap();
            assert_eq!(read_lines(&mut client, 1).await, ["SERVER_ERROR two"]);
        })
        .await;
    }
}
