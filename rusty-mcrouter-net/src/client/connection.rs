use std::{collections::VecDeque, io, sync::Arc};

use bytes::BytesMut;
use rusty_mcrouter_protocol::meta::{MetaReplyDecoder, MetaRequestEncoder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpStream},
    sync::mpsc,
    time::{sleep_until, Instant},
};

use crate::{
    client::{
        config::Config,
        types::{Command, ConnectionCommand, ConnectionEvent, DownReason, Inflight, Payload},
    },
    error::{ConnectError, LocalError, ProtocolError, RequestError, SendError},
};

pub(crate) struct Connection {
    addr: Arc<str>,
    cfg: Config,
    rx: mpsc::Receiver<ConnectionCommand>,
    events: Box<dyn Fn(ConnectionEvent)>,
    pending: VecDeque<Command>,   // accepted, not yet written
    inflight: VecDeque<Inflight>, // written, awaiting reply
    encoder: MetaRequestEncoder,
    decoder: MetaReplyDecoder,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

enum PipelineExit {
    Down(DownReason),
    Closed,
    HandlesDropped,
}

impl Connection {
    pub(crate) fn new(
        addr: Arc<str>,
        cfg: Config,
        rx: mpsc::Receiver<ConnectionCommand>,
        events: Box<dyn Fn(ConnectionEvent)>,
    ) -> Connection {
        let read_buf = BytesMut::with_capacity(cfg.read_buf_initial_capacity);
        Connection {
            addr,
            cfg,
            rx,
            events,
            pending: VecDeque::new(),
            inflight: VecDeque::new(),
            encoder: MetaRequestEncoder::new(),
            decoder: MetaReplyDecoder::new(),
            read_buf,
            write_buf: BytesMut::new(),
        }
    }

    pub(crate) async fn run(mut self) {
        'lifecycle: loop {
            // unconnected - lazy wait for a request
            while self.pending.is_empty() {
                match self.rx.recv().await {
                    Some(ConnectionCommand::Command(cmd)) => self.pending.push_back(cmd),
                    Some(ConnectionCommand::CloseIdle) => {} // already closed
                    None => return,                          // dropped
                }
            }
            self.drain_channel();

            // connecting
            let stream = match self.connect_with_retries().await {
                Ok(s) => s,
                Err(err) => {
                    // fail all pending on down
                    self.fail_pending(SendError::Connect(err.clone()));
                    (self.events)(ConnectionEvent::Down(DownReason::ConnectFailed(err)));
                    continue 'lifecycle;
                }
            };
            (self.events)(ConnectionEvent::Up);

            // up
            let exit = self.pipeline(stream).await;
            self.reset_stream_state(); // ALWAYS clear read buffer and ask for a fresh decoder

            match exit {
                PipelineExit::Down(reason) => {
                    // for a Down(Protocol), pipeline already failed
                    // inflight with the real decode error, so the queue
                    // is empty and this is a deliberate no-op
                    self.fail_inflight_dropped(&reason);
                    (self.events)(ConnectionEvent::Down(reason));
                }
                PipelineExit::Closed => (self.events)(ConnectionEvent::Closed),
                PipelineExit::HandlesDropped => return,
            }
        }
    }

    async fn connect_with_retries(&self) -> Result<TcpStream, ConnectError> {
        let mut retries_left = self.cfg.connect_timeout_retries;

        loop {
            let attempt = TcpStream::connect(&*self.addr);
            let result = match self.cfg.connect_timeout {
                Some(dur) => match tokio::time::timeout(dur, attempt).await {
                    Ok(res) => res,
                    Err(_elapsed) if retries_left > 0 => {
                        retries_left -= 1;
                        continue;
                    }
                    Err(_elapsed) => return Err(ConnectError::Timeout),
                },
                None => attempt.await,
            };
            match result {
                Ok(stream) => {
                    let _ = stream.set_nodelay(true);
                    return Ok(stream);
                }
                // refused/retry/unreachable/resolution failure - immediately
                // fail with no retry
                Err(e) => return Err(ConnectError::Failed(e.kind())),
            }
        }
    }

    async fn pipeline(&mut self, stream: TcpStream) -> PipelineExit {
        let (mut reader, mut writer) = stream.into_split();

        // a slow connect may have consumed the entire deadline budget fail
        // overshooters immediately instead of writing dead requests to wire
        self.expire_deadlines();

        if let Err(r) = self.flush_pending(&mut writer).await {
            return PipelineExit::Down(r);
        }

        loop {
            let deadline = self.next_deadline();
            tokio::select! {
                maybe_cmd = self.rx.recv() => match maybe_cmd {
                    Some(ConnectionCommand::Command(cmd)) => {
                        self.pending.push_back(cmd);
                        self.drain_channel();
                        if let Err(r) = self.flush_pending(&mut writer).await {
                            return PipelineExit::Down(r);
                        }
                    }
                    Some(ConnectionCommand::CloseIdle) => {
                        debug_assert!(self.pending.is_empty(), "pending must be flushed before closing idle");
                        // Timed-out slots remain only to preserve FIFO alignment;
                        // once every slot is a tombstone, closing is quiescent.
                        if self.inflight.iter().all(|slot| slot.reply_tx.is_none()) {
                            // Their replies belong to the old stream and must not
                            // become expectations on the replacement connection.
                            self.inflight.clear();
                            return PipelineExit::Closed;
                        }
                    }
                    None => return PipelineExit::HandlesDropped,
                },
                res = reader.read_buf(&mut self.read_buf) => match res {
                    // this is deliberately different from mcrouter
                    // an idle remote close is benign (closed, with no TKO)
                    // mcrouter instead hard TKOs idle EOFs. we dont want to
                    // recycle a connection to mark a healthy box down
                    Ok(0) if self.inflight.is_empty() => return PipelineExit::Closed,
                    Ok(0) => return PipelineExit::Down(DownReason::Eof),
                    Ok(_) => {
                        if let Err(e) = self.deliver_replies() {
                            self.fail_inflight(SendError::Protocol(e));
                            return PipelineExit::Down(DownReason::Protocol);
                        }
                    }
                    Err(_e) if self.inflight.is_empty() => {
                        return PipelineExit::Closed
                    }
                    Err(e) => return PipelineExit::Down(DownReason::Stream(e.kind())),
                },
                _ = sleep_until(deadline.unwrap_or_else(far_future)), if deadline.is_some() => {
                    self.expire_deadlines();
                }
            }
        }
    }

    // empty the channel without blocking so all queued requests coalesces into
    // one write batch
    fn drain_channel(&mut self) {
        while let Ok(cmd) = self.rx.try_recv() {
            match cmd {
                ConnectionCommand::Command(cmd) => self.pending.push_back(cmd),
                // this is queued behind requests, so we are NOT idle. ignore this
                ConnectionCommand::CloseIdle => {}
            }
        }
    }

    async fn flush_pending(&mut self, writer: &mut OwnedWriteHalf) -> Result<(), DownReason> {
        self.write_buf.clear();

        for command in self.pending.drain(..) {
            let expectation = match command.payload {
                Payload::Request(request) => {
                    match self.encoder.encode(&request, &mut self.write_buf) {
                        Ok(expectation) => expectation,
                        Err(err) => {
                            // fail per request
                            let _ = command
                                .reply_tx
                                .send(Err(SendError::Local(LocalError::Encode(err))));
                            continue;
                        }
                    }
                }
                Payload::VersionProbe => self.encoder.encode_version_probe(&mut self.write_buf),
            };
            self.inflight.push_back(Inflight {
                expectation,
                reply_tx: Some(command.reply_tx),
                deadline: command.deadline,
            });
        }

        if self.write_buf.is_empty() {
            // every drained command failed to encode - nothing went on wire
            return Ok(());
        }

        let write = writer.write_all(&self.write_buf);
        match self.cfg.write_timeout {
            Some(dur) => match tokio::time::timeout(dur, write).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(DownReason::Stream(e.kind())),
                Err(_elapsed) => Err(DownReason::Stream(io::ErrorKind::TimedOut)),
            },
            None => write.await.map_err(|e| DownReason::Stream(e.kind())),
        }
    }

    fn deliver_replies(&mut self) -> Result<(), ProtocolError> {
        loop {
            let Some(front) = self.inflight.front() else {
                if self.read_buf.is_empty() {
                    return Ok(());
                }

                return Err(ProtocolError::Desync(
                    "reply bytes with no inflight request",
                ));
            };
            match self
                .decoder
                .decode(&front.expectation, &mut self.read_buf)?
            {
                Some(reply) => {
                    let slot = self.inflight.pop_front().expect("front checked above");
                    match slot.reply_tx {
                        Some(tx) => {
                            let _ = tx.send(Ok(reply));
                        }
                        // tombstone, late reply. decode and discard to keep
                        // FIFO aligned
                        None => {}
                    }
                }
                None => return Ok(()), // partial frame - wait
            }
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        let pending = self.pending.front().and_then(|c| c.deadline);
        let in_flight = self
            .inflight
            .iter()
            .find(|s| s.reply_tx.is_some())
            .and_then(|s| s.deadline);

        match (pending, in_flight) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn expire_deadlines(&mut self) {
        let now = Instant::now();

        while self
            .pending
            .front()
            .is_some_and(|c| c.deadline.is_some_and(|d| d <= now))
        {
            let cmd = self.pending.pop_front().expect("front checked above");
            let _ = cmd
                .reply_tx
                .send(Err(SendError::Request(RequestError::Timeout {
                    sent: false,
                })));
        }

        for slot in self.inflight.iter_mut() {
            if slot.deadline.is_some_and(|d| d <= now) {
                if let Some(tx) = slot.reply_tx.take() {
                    let _ = tx.send(Err(SendError::Request(RequestError::Timeout {
                        sent: true,
                    })));
                }
            }
        }
    }

    fn fail_pending(&mut self, err: SendError) {
        for command in self.pending.drain(..) {
            let _ = command.reply_tx.send(Err(err.clone()));
        }
    }

    fn fail_inflight(&mut self, err: SendError) {
        for inflight in self.inflight.drain(..) {
            if let Some(reply_tx) = inflight.reply_tx {
                let _ = reply_tx.send(Err(err.clone()));
            }
        }
    }

    fn fail_inflight_dropped(&mut self, reason: &DownReason) {
        let kind = match reason {
            DownReason::Stream(kind) => *kind,
            DownReason::Eof => io::ErrorKind::UnexpectedEof,
            DownReason::Protocol => io::ErrorKind::InvalidData,
            // this is unreachable - inflight is always empty on connect fails
            DownReason::ConnectFailed(_) => io::ErrorKind::NotConnected,
        };

        self.fail_inflight(SendError::Request(RequestError::Dropped { kind }));
    }

    fn reset_stream_state(&mut self) {
        self.read_buf.clear();
        self.decoder = MetaReplyDecoder::new();
    }
}

fn far_future() -> Instant {
    Instant::now() + std::time::Duration::from_secs(86_400)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rusty_mcrouter_protocol::meta::MetaReplyExpectation;
    use rusty_mcrouter_protocol::Reply;
    use tokio::sync::oneshot;

    use super::*;
    use crate::client::config::Config;

    type ReplyRx = oneshot::Receiver<Result<Reply, SendError>>;

    fn connection() -> Connection {
        let (_tx, rx) = mpsc::channel(1);
        Connection::new(
            Arc::from("127.0.0.1:0"),
            Config::default(),
            rx,
            Box::new(|_| {}),
        )
    }

    fn live_slot(deadline: Option<Instant>) -> (Inflight, ReplyRx) {
        let (tx, rx) = oneshot::channel();
        let slot = Inflight {
            expectation: MetaReplyExpectation::Delete,
            reply_tx: Some(tx),
            deadline,
        };
        (slot, rx)
    }

    fn tombstone(deadline: Option<Instant>) -> Inflight {
        Inflight {
            expectation: MetaReplyExpectation::Delete,
            reply_tx: None,
            deadline,
        }
    }

    fn pending_cmd(deadline: Option<Instant>) -> (Command, ReplyRx) {
        let (tx, rx) = oneshot::channel();
        let cmd = Command {
            payload: Payload::VersionProbe,
            reply_tx: tx,
            deadline,
        };
        (cmd, rx)
    }

    /// The regression test for `min` over `Option<Instant>`: `None < Some`
    /// under Option's Ord, so a naive min(pending, inflight) returns None
    /// whenever pending is empty — the STEADY STATE — silently disarming
    /// the reply-timeout timer.
    #[tokio::test]
    async fn next_deadline_takes_min_and_survives_missing_sides() {
        let mut c = connection();
        let t1 = Instant::now() + Duration::from_millis(100);
        let t2 = Instant::now() + Duration::from_millis(200);

        assert_eq!(c.next_deadline(), None);

        // inflight only — the (None, Some) case
        let (slot, _rx) = live_slot(Some(t2));
        c.inflight.push_back(slot);
        assert_eq!(c.next_deadline(), Some(t2));

        // pending earlier than inflight — real min
        let (cmd, _rx2) = pending_cmd(Some(t1));
        c.pending.push_back(cmd);
        assert_eq!(c.next_deadline(), Some(t1));
    }

    /// Tombstones' deadlines are spent; only the first LIVE slot arms the
    /// timer, and an all-tombstone queue arms nothing.
    #[tokio::test]
    async fn next_deadline_skips_tombstones() {
        let mut c = connection();
        let t_tomb = Instant::now() + Duration::from_millis(10);
        let t_live = Instant::now() + Duration::from_millis(200);

        c.inflight.push_back(tombstone(Some(t_tomb)));
        let (slot, _rx) = live_slot(Some(t_live));
        c.inflight.push_back(slot);
        assert_eq!(c.next_deadline(), Some(t_live));

        c.inflight.pop_back();
        assert_eq!(c.next_deadline(), None);
    }

    /// The two queues get opposite treatments, keyed by the idempotency
    /// signal: pending was never written (remove entirely, sent: false);
    /// inflight is owed a reply by the server (tombstone the slot so the
    /// late reply is decoded and discarded, sent: true).
    #[tokio::test(start_paused = true)]
    async fn expire_pops_pending_but_tombstones_inflight() {
        let mut c = connection();
        let past = Instant::now();
        let future = Instant::now() + Duration::from_secs(60);

        let (cmd_expired, mut rx_pending) = pending_cmd(Some(past));
        let (cmd_live, _rx_keep) = pending_cmd(Some(future));
        c.pending.push_back(cmd_expired);
        c.pending.push_back(cmd_live);

        let (slot_expired, mut rx_inflight) = live_slot(Some(past));
        let (slot_live, _rx_live) = live_slot(Some(future));
        c.inflight.push_back(slot_expired);
        c.inflight.push_back(slot_live);

        tokio::time::advance(Duration::from_millis(1)).await;
        c.expire_deadlines();

        // pending: expired front REMOVED, never-sent signal
        assert_eq!(c.pending.len(), 1);
        assert!(matches!(
            rx_pending.try_recv().unwrap(),
            Err(SendError::Request(RequestError::Timeout { sent: false }))
        ));

        // inflight: slot KEPT for FIFO alignment, caller failed as sent
        assert_eq!(c.inflight.len(), 2);
        assert!(c.inflight[0].reply_tx.is_none(), "expired slot must become a tombstone");
        assert!(c.inflight[1].reply_tx.is_some(), "unexpired slot must stay live");
        assert!(matches!(
            rx_inflight.try_recv().unwrap(),
            Err(SendError::Request(RequestError::Timeout { sent: true }))
        ));

        // deadlines without a timeout configured never expire
        let (cmd_no_deadline, mut rx_nd) = pending_cmd(None);
        c.pending.push_front(cmd_no_deadline);
        c.expire_deadlines();
        assert_eq!(c.pending.len(), 2);
        assert!(rx_nd.try_recv().is_err(), "no-deadline command must not be failed");
    }
}
