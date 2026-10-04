use std::{collections::BTreeMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use rusty_mcrouter_protocol::meta::{
    DecodedMetaCommand, MetaReplyEncoder, MetaReplyPlan, MetaRequestDecodeError, MetaRequestDecoder,
};
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::mpsc::Sender,
    task::JoinSet,
};

use crate::{
    send_request, FrontendConnectionOptions, FrontendError, FrontendMetricsShard, ProxyRequest,
};

pub struct FrontendConnectionSetup {
    pub stream: tokio::net::TcpStream,
    pub request_tx: Sender<ProxyRequest>,
    pub metrics: Arc<FrontendMetricsShard>,
    pub options: FrontendConnectionOptions,
}

/// one client connection's lifecycle:
/// - decode pipelined Meta commands
/// - submit routable requests through a worker mailbox; answer `mn` and
///   recoverable parse errors locally
/// - encode replies against each slot's retained reply plan, in request order
pub struct Connection {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    request_tx: Sender<ProxyRequest>,
    metrics: Arc<FrontendMetricsShard>,
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
    requests: JoinSet<(usize, Reply)>,
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
    pub fn new(setup: FrontendConnectionSetup) -> Self {
        let FrontendConnectionSetup {
            stream,
            request_tx,
            metrics,
            options,
        } = setup;
        let (reader, writer) = stream.into_split();
        metrics.client_connections.inc();

        Self {
            reader,
            writer,
            request_tx,
            metrics,
            buf: BytesMut::with_capacity(options.read_buf_initial_capacity),
            write_buf: BytesMut::new(),
            decoder: MetaRequestDecoder::new(),
            encoder: MetaReplyEncoder::new(),
            slots: BTreeMap::new(),
            next_seq: 0,
            next_write: 0,
            in_flight: 0,
            input_closed: false,
            requests: JoinSet::new(),
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

            // select! touches reader/buf and requests as disjoint fields
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
                Some(result) = self.requests.join_next(), if !self.requests.is_empty() => {
                    let (seq, reply) = result?;
                    self.complete(seq, reply);
                    while let Some(result) = self.requests.try_join_next() {
                        let (seq, reply) = result?;
                        self.complete(seq, reply);
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

    fn submit_single(&mut self, seq: usize, request: Request) {
        let request_tx = self.request_tx.clone();

        self.requests.spawn_local(async move {
            let reply = send_request(&request_tx, request).await;
            (seq, reply)
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
        let metrics = &self.metrics;
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
    use std::time::Duration;

    use rusty_mcrouter_protocol::test_support::get_miss;
    use rusty_mcrouter_protocol::RequestKind;
    use tokio::sync::mpsc;
    use tokio::task::LocalSet;
    use tokio::time::timeout;

    use super::*;

    async fn session(
        metrics: Arc<FrontendMetricsShard>,
    ) -> (
        tokio::net::TcpStream,
        mpsc::Receiver<ProxyRequest>,
        tokio::task::JoinHandle<Result<(), FrontendError>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let (request_tx, request_rx) = mpsc::channel(8);
        let conn = Connection::new(FrontendConnectionSetup {
            stream: server_stream,
            request_tx,
            metrics,
            options: FrontendConnectionOptions::default(),
        });
        let task = tokio::task::spawn_local(conn.run());
        (client, request_rx, task)
    }

    async fn read_lines(client: &mut tokio::net::TcpStream, n: usize) -> Vec<String> {
        timeout(Duration::from_secs(2), async {
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
        })
        .await
        .expect("timed out waiting for replies")
    }

    #[tokio::test]
    async fn frontend_metrics_account_a_pipelined_session() {
        LocalSet::new()
            .run_until(async {
                let metrics = FrontendMetricsShard::new();
                let (mut client, mut requests, task) = session(Arc::clone(&metrics)).await;

                client
                    .write_all(b"mg foo v\r\nmn\r\nnot_a_command\r\n")
                    .await
                    .unwrap();

                let request = requests.recv().await.unwrap();
                assert_eq!(request.request.key().as_bytes(), b"foo");
                request.reply_tx.send(get_miss()).unwrap();

                let lines = read_lines(&mut client, 3).await;
                assert_eq!(lines[0], "EN", "mg miss");
                assert_eq!(lines[1], "MN", "mn answered in pipeline order");
                assert_eq!(lines[2], "ERROR", "garbage must answer in pipeline order");

                assert_eq!(metrics.requests[RequestKind::Get as usize].load(), 1);
                assert_eq!(metrics.noops.load(), 1);
                assert_eq!(metrics.parse_errors.load(), 1);
                assert_eq!(metrics.failed.load(), 1);
                assert_eq!(metrics.processing.load(), 0);
                assert_eq!(metrics.client_connections.load(), 1);
                assert!(
                    requests.try_recv().is_err(),
                    "local commands must not be routed"
                );

                drop(client);
                task.await.unwrap().unwrap();
                assert_eq!(metrics.processing.load(), 0);
                assert_eq!(metrics.client_connections.load(), 0);
            })
            .await;
    }

    #[tokio::test]
    async fn out_of_order_replies_keep_plans_and_local_commands_in_order() {
        LocalSet::new()
            .run_until(async {
                let metrics = FrontendMetricsShard::new();
                let (mut client, mut requests, task) = session(Arc::clone(&metrics)).await;
                let commands = concat!(
                    "mg first v Oone\r\n",
                    "mg second v Otwo\r\n",
                    "mn\r\n",
                    "mg quiet v q\r\n",
                    "not_a_command\r\n",
                );
                client.write_all(commands.as_bytes()).await.unwrap();

                let (mut first, mut second, mut quiet) = (None, None, None);
                for _ in 0..3 {
                    let message = requests.recv().await.unwrap();
                    match message.request.key().as_bytes() {
                        b"first" => first = Some(message),
                        b"second" => second = Some(message),
                        b"quiet" => quiet = Some(message),
                        key => panic!("unexpected key: {key:?}"),
                    }
                }
                second.unwrap().reply_tx.send(get_miss()).unwrap();
                quiet.unwrap().reply_tx.send(get_miss()).unwrap();
                timeout(Duration::from_secs(2), async {
                    while metrics.processing.load() != 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();

                let error = client.try_read(&mut [0u8; 64]).unwrap_err();
                assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

                first.unwrap().reply_tx.send(get_miss()).unwrap();
                assert_eq!(
                    read_lines(&mut client, 4).await,
                    ["EN Oone", "EN Otwo", "MN", "ERROR"]
                );
                assert_eq!(metrics.requests[RequestKind::Get as usize].load(), 3);
                assert_eq!(metrics.processing.load(), 0);
                drop(client);
                task.await.unwrap().unwrap();
            })
            .await;
    }

    #[tokio::test]
    async fn input_eof_drains_pending_replies_and_ignores_a_partial_frame() {
        LocalSet::new()
            .run_until(async {
                let metrics = FrontendMetricsShard::new();
                let (mut client, mut requests, task) = session(Arc::clone(&metrics)).await;
                client
                    .write_all(b"mg foo v\r\nmn\r\nmg partial")
                    .await
                    .unwrap();
                client.shutdown().await.unwrap();
                requests
                    .recv()
                    .await
                    .unwrap()
                    .reply_tx
                    .send(get_miss())
                    .unwrap();

                let mut replies = Vec::new();
                timeout(Duration::from_secs(2), client.read_to_end(&mut replies))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(replies, b"EN\r\nMN\r\n");
                task.await.unwrap().unwrap();
                assert_eq!(metrics.processing.load(), 0);
                assert_eq!(metrics.client_connections.load(), 0);
            })
            .await;
    }

    #[tokio::test]
    async fn dropping_a_connection_cancels_waiters_and_balances_gauges() {
        LocalSet::new()
            .run_until(async {
                let metrics = FrontendMetricsShard::new();
                let (mut client, mut requests, task) = session(Arc::clone(&metrics)).await;
                client.write_all(b"mg foo v\r\n").await.unwrap();
                let request = requests.recv().await.unwrap();
                assert_eq!(metrics.processing.load(), 1);

                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
                assert!(request.reply_tx.is_closed());
                assert_eq!(metrics.processing.load(), 0);
                assert_eq!(metrics.client_connections.load(), 0);
            })
            .await;
    }
}
