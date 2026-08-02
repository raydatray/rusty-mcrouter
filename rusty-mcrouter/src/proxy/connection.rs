use std::{
    collections::VecDeque,
    future::{poll_fn, Future},
    pin::Pin,
    rc::Rc,
    task::Poll,
};

use bytes::{Bytes, BytesMut};
use futures_util::{stream::FuturesUnordered, FutureExt, Stream, StreamExt};
use rusty_mcrouter_core::DynRoute;
use rusty_mcrouter_net::NetError;
use rusty_mcrouter_protocol::meta::{
    DecodedMetaCommand, MetaReplyEncoder, MetaReplyPlan, MetaRequestDecodeError,
    MetaRequestDecoder,
};
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::proxy::{config::ThreadMode, proxy_set::ProxySet, ProxyHandle};

const READ_BUF_INITIAL_CAPACITY: usize = 4096;

/// An in-flight route continuation, owned and polled by its connection.
type InFlightRoute = Pin<Box<dyn Future<Output = (usize, Reply)>>>;

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
    /// The only per-request state, a dense ring ordered by sequence number:
    /// index 0 is seq `next_write`, and a request's slot lives at
    /// `seq - next_write`. A slot is created at decode time with its
    /// hop-local `MetaReplyPlan` (never routed, never crosses threads) and
    /// flips to `Ready` when its outcome exists.
    slots: VecDeque<Slot>,
    /// In-flight route continuations, owned and polled by this connection —
    /// the fiber-pool analogue. No task is spawned and no completion channel
    /// exists per request; completions surface as direct `complete()` calls,
    /// and dropping the connection cancels its in-flight work.
    routes: FuturesUnordered<InFlightRoute>,
    next_seq: usize,
    next_write: usize,
    input_closed: bool,
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
    ) -> Self {
        let (reader, writer) = stream.into_split();

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
            slots: VecDeque::new(),
            routes: FuturesUnordered::new(),
            next_seq: 0,
            next_write: 0,
            input_closed: false,
        }
    }

    pub async fn run(mut self) -> Result<(), NetError> {
        loop {
            if !self.input_closed {
                self.drain_input();
            }

            self.flush_ready().await?;

            if self.input_closed && self.slots.is_empty() {
                return Ok(());
            }

            // select! touches reader/buf and routes as disjoint fields
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
                first = self.routes.next(), if !self.routes.is_empty() => {
                    if let Some((seq, reply)) = first {
                        self.complete(seq, reply);
                    }
                    // drain everything already completed before the single
                    // batched flush at the top of the loop.
                    self.drain_completed_routes().await;
                }
            }
        }
    }

    /// Collects every already-completed route without waiting. Runs under a
    /// real poll context (not a no-op waker) so the in-flight set keeps a
    /// waker that can actually wake this connection for later completions.
    async fn drain_completed_routes(&mut self) {
        poll_fn(|cx| {
            while let Poll::Ready(Some((seq, reply))) = Pin::new(&mut self.routes).poll_next(cx) {
                self.complete(seq, reply);
            }
            Poll::Ready(())
        })
        .await;
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
                    let seq = self.take_seq();
                    self.slots.push_back(Slot {
                        plan: reply_plan,
                        state: SlotState::InFlight,
                    });
                    self.submit_single(seq, request);
                }
                Ok(Some(DecodedMetaCommand::NoOp)) => {
                    self.take_seq();
                    self.slots.push_back(Slot::ready(SlotOutcome::NoOp));
                }
                Ok(None) => return,
                // one malformed command was consumed; its error joins the
                // pipeline in order and decoding continues.
                Err(MetaRequestDecodeError::Recoverable(error)) => {
                    self.take_seq();
                    self.slots
                        .push_back(Slot::ready(SlotOutcome::Reply(Reply::Error(error))));
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
        // Slots are dense and never removed before completing, so a live seq
        // always sits `seq - next_write` from the front of the ring.
        let index = seq.wrapping_sub(self.next_write);
        if let Some(slot) = self.slots.get_mut(index) {
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

    fn submit_single(&mut self, seq: usize, req: Request) {
        let target = self.route_target(&req);
        let mut route = Box::pin(async move { (seq, route_one(target, req).await) });

        // The first poll happens inline: immediately-ready routes (null,
        // error) complete with no queueing at all, and backend-bound routes
        // enqueue their request during decode rather than on a later tick.
        // A future parked here re-registers real wakers on its first poll
        // from `routes`, which is guaranteed before this connection parks.
        match route.as_mut().now_or_never() {
            Some((seq, reply)) => self.complete(seq, reply),
            None => self.routes.push(route),
        }
    }

    /// flush replies that are ready in request order, advancing `next_write`.
    /// A suppressed (quiet) reply writes nothing but still advances.
    async fn flush_ready(&mut self) -> Result<(), NetError> {
        self.write_buf.clear();
        while matches!(
            self.slots.front(),
            Some(Slot {
                state: SlotState::Ready(_),
                ..
            })
        ) {
            let slot = self.slots.pop_front().expect("checked above");
            let SlotState::Ready(outcome) = slot.state else {
                unreachable!("matched Ready above");
            };
            match outcome {
                SlotOutcome::NoOp => self.encoder.encode_noop(&mut self.write_buf),
                SlotOutcome::Reply(reply) => {
                    if self
                        .encoder
                        .encode(&reply, &slot.plan, &mut self.write_buf)
                        .is_err()
                    {
                        // the reply cannot satisfy this slot's plan (for
                        // example a backend omitted a projected field):
                        // degrade this slot only, never the connection.
                        let fallback = Reply::Error(ErrorReply::Server(Some(
                            Bytes::from_static(b"proxy reply encoding failed"),
                        )));
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
