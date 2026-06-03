use std::{collections::BTreeMap, rc::Rc};

use bytes::{Bytes, BytesMut};
use rusty_mcrouter_core::DynRoute;
use rusty_mcrouter_net::NetError;
use rusty_mcrouter_protocol::{parse_request, Reply, Request};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::mpsc,
};

use crate::proxy::{config::ThreadMode, proxy_set::ProxySet};

const READ_BUF_INITIAL_CAPACITY: usize = 4096;
const COMPLETED_CHANNEL_CAPACITY: usize = 1024;

/// one client connection's lifecycle:
/// - parse pipelined requests
/// - dispatch each to a proxy (local inline or remote via the proxy queue)
/// - write replies back in request order
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
    pending: BTreeMap<usize, Reply>,
    next_seq: usize,
    next_write: usize,
    in_flight: usize,
    input_closed: bool,
    completed_tx: mpsc::Sender<(usize, Reply)>,
    completed_rx: mpsc::Receiver<(usize, Reply)>,
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
            pending: BTreeMap::new(),
            next_seq: 0,
            next_write: 0,
            in_flight: 0,
            input_closed: false,
            completed_tx,
            completed_rx,
        }
    }

    pub async fn run(mut self) -> Result<(), NetError> {
        loop {
            if !self.input_closed {
                self.drain_input()?;
            }

            self.flush_ready().await?;

            if self.input_closed && self.in_flight == 0 {
                return Ok(());
            }

            // select! touches reader/buf and completed_rx as disjoint fields
            // directly; the two arms can't be factored into &mut self methods.
            tokio::select! {
                read = self.reader.read_buf(&mut self.buf), if !self.input_closed => {
                    if read? == 0 {
                        self.input_closed = true;
                    }
                }
                maybe_completed = self.completed_rx.recv(), if self.in_flight > 0 => {
                    match maybe_completed {
                        Some((seq, reply)) => {
                            self.pending.insert(seq, reply);
                            while let Ok((seq, reply)) = self.completed_rx.try_recv() {
                                self.pending.insert(seq, reply);
                            }
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    /// parse every complete frame currently buffered and submit it without
    /// waiting for replies (pipelining).
    fn drain_input(&mut self) -> Result<(), NetError> {
        while let Some(req) = parse_request(&mut self.buf)? {
            let seq = self.next_seq;
            self.next_seq = self.next_seq.wrapping_add(1);
            self.in_flight = self.in_flight.saturating_add(1);
            self.submit(seq, req);
        }
        Ok(())
    }

    /// choose a proxy for `req` and spawn the routing task.
    /// same-thread requests route inline
    /// remote requests cross into the target proxy's queue.
    fn submit(&self, seq: usize, req: Request) {
        let handle = self.proxies.choose(self.mode, self.current_id, &req);
        let same_thread = handle.id() == self.current_id;
        let route = Rc::clone(&self.local_route);
        let completed_tx = self.completed_tx.clone();

        tokio::task::spawn_local(async move {
            let reply = if same_thread {
                route.route_dyn(req).await.unwrap_or_else(|_| {
                    Reply::ServerError(Bytes::from_static(b"backend unavailable"))
                })
            } else {
                handle.send_request(req).await
            };
            let _ = completed_tx.send((seq, reply)).await;
        });
    }

    /// flush replies that are ready in request order, advancing `next_write`.
    async fn flush_ready(&mut self) -> Result<(), NetError> {
        self.write_buf.clear();
        while let Some(reply) = self.pending.remove(&self.next_write) {
            reply.serialize_into(&mut self.write_buf);
            self.next_write = self.next_write.wrapping_add(1);
            self.in_flight = self.in_flight.saturating_sub(1);
        }
        if !self.write_buf.is_empty() {
            self.writer.write_all(&self.write_buf).await?;
        }
        Ok(())
    }
}
