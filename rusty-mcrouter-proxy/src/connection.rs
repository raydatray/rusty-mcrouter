use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{collections::BTreeMap, rc::Rc};

use bytes::{Bytes, BytesMut};
use rusty_mcrouter_core::DynRoute;
use rusty_mcrouter_net::counters::CommandKind;
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
    config::ThreadMode, proxy_set::ProxySet, FrontendCounters, FrontendError, ProxyHandle,
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
    counters: Arc<FrontendCounters>, //temp - moved out soon
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
        proxies: ProxySet,
        mode: ThreadMode,
        counters: Arc<FrontendCounters>,
    ) -> Self {
        let (reader, writer) = stream.into_split();
        let (completed_tx, completed_rx) = mpsc::channel(COMPLETED_CHANNEL_CAPACITY);

        Self {
            reader,
            writer,
            current_id,
            local_route,
            proxies,
            mode,
            buf: BytesMut::with_capacity(READ_BUF_INITIAL_CAPACITY),
            write_buf: BytesMut::new(),
            decoder: MetaRequestDecoder::new(),
            encoder: MetaReplyEncoder::new(),
            slots: BTreeMap::new(),
            counters,
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
                    self.counters.request[CommandKind::of(&request) as usize]
                        .fetch_add(1, Ordering::Relaxed);
                    self.counters.processing.fetch_add(1, Ordering::Relaxed);
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
                    self.counters.noops.fetch_add(1, Ordering::Relaxed);
                    let seq = self.take_seq();
                    self.slots.insert(seq, Slot::ready(SlotOutcome::NoOp));
                }
                Ok(None) => return,
                // one malformed command was consumed; its error joins the
                // pipeline in order and decoding continues.
                Err(MetaRequestDecodeError::Recoverable(error)) => {
                    self.counters.parse_errors.fetch_add(1, Ordering::Relaxed);
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
        self.counters.processing.fetch_sub(1, Ordering::Relaxed);
        if let Some(slot) = self.slots.get_mut(&seq) {
            slot.state = SlotState::Ready(SlotOutcome::Reply(reply));
        }
    }

    /// resolves `req`'s target
    /// - which proxy handles it
    /// - if its the same thread
    /// - the local route
    fn route_target(&self, req: &Request) -> RouteTarget {
        let handle = self.proxies.choose(self.mode, self.current_id, req);
        let same_thread = handle.id() == self.current_id;

        RouteTarget {
            handle,
            same_thread,
            route: Rc::clone(&self.local_route),
        }
    }

    fn submit_single(&self, seq: usize, req: Request) {
        let target = self.route_target(&req);
        let completed_tx = self.completed_tx.clone();

        tokio::task::spawn_local(async move {
            let reply = route_one(target, req).await;

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
                        self.counters.failed.fetch_add(1, Ordering::Relaxed);
                    }
                    if self
                        .encoder
                        .encode(&reply, &slot.plan, &mut self.write_buf)
                        .is_err()
                    {
                        if !matches!(reply, Reply::Error(_)) {
                            self.counters.failed.fetch_add(1, Ordering::Relaxed);
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
        self.counters
            .processing
            .fetch_sub(self.in_flight as i64, Ordering::Relaxed);
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

struct RouteTarget {
    handle: ProxyHandle,
    same_thread: bool,
    route: Rc<dyn DynRoute>,
}

async fn route_one(target: RouteTarget, req: Request) -> Reply {
    let RouteTarget {
        handle,
        same_thread,
        route,
    } = target;

    if same_thread {
        route.route_dyn(req).await.unwrap_or_else(|_| {
            Reply::Error(ErrorReply::Server(Some(Bytes::from_static(
                b"backend unavailable",
            ))))
        })
    } else {
        handle.send_request(req).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use rusty_mcrouter_core::{DestinationRoute, Route};
    use rusty_mcrouter_net::counters::CommandKind;
    use rusty_mcrouter_net::test_support::{run_local, MockBackend};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::message::ProxyMessage;

    /// a real Connection over a localhost socket pair, with a SameThread
    /// route into a mock backend. the proxy handle channel is never used
    /// (SameThread routes inline) but ProxySet demands one.
    async fn session(
        counters: Arc<FrontendCounters>,
    ) -> (tokio::net::TcpStream, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let route = DestinationRoute::new(MockBackend::miss()).into_dyn();
        let (tx, _rx) = mpsc::channel::<ProxyMessage>(1);
        let proxies = ProxySet::new(vec![ProxyHandle::new(0, tx)]);

        let conn = Connection::new(
            server_stream,
            0,
            route,
            proxies,
            ThreadMode::SameThread,
            counters,
        );
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

    /// THE frontend counters test: a pipelined session of mg + mn + garbage
    /// counts one of each fact, failed counts the CLIENT_ERROR, replies come
    /// back in pipeline order, and the gauges settle to zero.
    #[tokio::test]
    async fn frontend_counters_account_a_pipelined_session() {
        run_local(async {
            let counters = FrontendCounters::new();
            let (mut client, task) = session(Arc::clone(&counters)).await;

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
                counters.request[CommandKind::Get as usize].load(Ordering::Relaxed),
                1
            );
            assert_eq!(counters.noops.load(Ordering::Relaxed), 1);
            assert_eq!(counters.parse_errors.load(Ordering::Relaxed), 1);
            assert_eq!(
                counters.failed.load(Ordering::Relaxed),
                1,
                "the CLIENT_ERROR is a client-visible error reply"
            );
            assert_eq!(counters.processing.load(Ordering::Relaxed), 0);

            // client disconnect ends the session; the gauge must not leak
            drop(client);
            task.await.unwrap();
            assert_eq!(counters.processing.load(Ordering::Relaxed), 0);
        })
        .await;
    }
}
