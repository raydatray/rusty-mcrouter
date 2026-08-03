use std::collections::VecDeque;

use crate::{NetError, Result, TimeoutPhase};
use bytes::BytesMut;
use rusty_mcrouter_protocol::meta::{MetaReplyDecoder, MetaReplyExpectation, MetaRequestEncoder};
use rusty_mcrouter_protocol::Reply;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{mpsc, oneshot},
    time::{sleep_until, Instant},
};

use super::command::ClientCommand;
use super::config::ClientConfig;

pub(crate) struct ClientConnection {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    rx: mpsc::Receiver<ClientCommand>,
    /// FIFO of in-flight requests. Meta result codes are ambiguous (`HD` may
    /// answer a get, store, delete, or arithmetic), so each slot retains the
    /// reply expectation derived while encoding its request.
    pending: VecDeque<(MetaReplyExpectation, oneshot::Sender<Result<Reply>>)>,
    encoder: MetaRequestEncoder,
    decoder: MetaReplyDecoder,
    read_buf: BytesMut,
    write_buf: BytesMut,
    write_timeout: Option<Duration>,
    read_idle_timeout: Option<Duration>,
    read_deadline: Instant,
}

impl ClientConnection {
    pub(crate) fn new(
        stream: TcpStream,
        rx: mpsc::Receiver<ClientCommand>,
        cfg: &ClientConfig,
    ) -> Self {
        let (reader, writer) = stream.into_split();

        Self {
            reader,
            writer,
            rx,
            pending: VecDeque::new(),
            encoder: MetaRequestEncoder::new(),
            decoder: MetaReplyDecoder::new(),
            read_buf: BytesMut::with_capacity(cfg.read_buf_initial_capacity),
            write_buf: BytesMut::new(),
            write_timeout: cfg.write_timeout,
            read_idle_timeout: cfg.read_idle_timeout,
            read_deadline: Instant::now(),
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                maybe_cmd = self.rx.recv() => {
                    match maybe_cmd {
                        Some(cmd) => {
                            if let Err(err) = self.write_batch(cmd).await {
                                self.fail_all_pending(err);
                                return;
                            }
                        }
                        None => return,
                    }
                }
                res = self.reader.read_buf(&mut self.read_buf),
                    if !self.pending.is_empty() =>
                {
                    let n = match res {
                        Ok(n) => n,
                        Err(e) => {
                            self.fail_all_pending(NetError::Io(e));
                            return;
                        }
                    };
                    if n == 0 {
                        self.fail_all_pending(NetError::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "backend closed connection",
                        )));
                        return;
                    }
                    self.arm_read_deadline();
                    if let Err(err) = self.deliver_replies() {
                        self.fail_all_pending(err);
                        return;
                    }
                }
                _ = sleep_until(self.read_deadline),
                    if self.read_idle_timeout.is_some() && !self.pending.is_empty() =>
                {
                    self.fail_all_pending(NetError::Timeout {
                        phase: TimeoutPhase::Reply,
                    });
                    return;
                }
            }
        }
    }

    // Coalesce the triggering command plus any other commands already queued in
    // the channel into one buffer and a single write syscall, preserving order so
    // FIFO reply matching still holds. (Tier 2: vectored/zero-copy via IoSlice.)
    async fn write_batch(&mut self, first: ClientCommand) -> Result<()> {
        self.write_buf.clear();
        self.encode_command(first);

        // drain whatever else is already waiting, into the same write
        while let Ok(cmd) = self.rx.try_recv() {
            self.encode_command(cmd);
        }
        if self.write_buf.is_empty() {
            // every drained command failed to encode; nothing hit the wire
            return Ok(());
        }

        let write_timeout = self.write_timeout;
        let write = self.writer.write_all(&self.write_buf);
        match write_timeout {
            Some(dur) => {
                tokio::time::timeout(dur, write)
                    .await
                    .map_err(|_| NetError::Timeout {
                        phase: TimeoutPhase::Write,
                    })??
            }
            None => write.await?,
        }
        self.arm_read_deadline();
        Ok(())
    }

    /// An encode failure (for example an empty backend key) is a per-request
    /// error: the oneshot fails, the connection and its FIFO stay intact, and
    /// `write_buf` is untouched because the encoder truncates on error.
    fn encode_command(&mut self, cmd: ClientCommand) {
        match cmd {
            ClientCommand::Request { request, reply_tx } => {
                match self.encoder.encode(&request, &mut self.write_buf) {
                    Ok(expectation) => self.pending.push_back((expectation, reply_tx)),
                    Err(error) => {
                        let _ = reply_tx.send(Err(NetError::Encode(error)));
                    }
                }
            }
            ClientCommand::VersionProbe { reply_tx } => {
                let expectation = self.encoder.encode_version_probe(&mut self.write_buf);

                self.pending.push_back((expectation, reply_tx));
            }
        }
    }

    fn arm_read_deadline(&mut self) {
        if let Some(dur) = self.read_idle_timeout {
            self.read_deadline = Instant::now() + dur;
        }
    }

    fn deliver_replies(&mut self) -> Result<()> {
        loop {
            let Some((expectation, _)) = self.pending.front() else {
                if self.read_buf.is_empty() {
                    return Ok(());
                }
                return Err(NetError::Desync("reply bytes with no pending request"));
            };
            match self.decoder.decode(expectation, &mut self.read_buf)? {
                Some(reply) => {
                    let (_, tx) = self.pending.pop_front().expect("front checked above");
                    let _ = tx.send(Ok(reply));
                }
                None => return Ok(()),
            }
        }
    }

    fn fail_all_pending(&mut self, err: NetError) {
        for (_, tx) in self.pending.drain(..) {
            let _ = tx.send(Err(err.clone()));
        }
    }
}
