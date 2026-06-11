use std::{collections::BTreeMap, rc::Rc};

use bytes::{Bytes, BytesMut};
use rusty_mcrouter_core::DynRoute;
use rusty_mcrouter_net::NetError;
use rusty_mcrouter_protocol::{parse_request, Parsed, Reply, Request};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::mpsc,
};

use crate::proxy::{config::ThreadMode, proxy_set::ProxySet, ProxyHandle};

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
        while let Some(parsed) = parse_request(&mut self.buf)? {
            let seq = self.next_seq;
            self.next_seq = self.next_seq.wrapping_add(1);
            self.in_flight = self.in_flight.saturating_add(1);
            match parsed {
                Parsed::One(req) => self.submit_single(seq, req),
                Parsed::MultiGet(keys) => self.submit_multiget(seq, keys),
            }
        }
        Ok(())
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

    fn submit_multiget(&self, seq: usize, keys: Vec<Bytes>) {
        let n = keys.len();

        let (sub_tx, mut sub_rx) = mpsc::channel::<(usize, Reply)>(n);

        for (i, key) in keys.into_iter().enumerate() {
            let req = Request::Get { key };
            let target = self.route_target(&req);
            let sub_tx = sub_tx.clone();
            tokio::task::spawn_local(async move {
                let reply = route_one(target, req).await;
                let _ = sub_tx.send((i, reply)).await;
            });
        }
        drop(sub_tx);

        let completed_tx = self.completed_tx.clone();
        tokio::task::spawn_local(async move {
            let mut slots: Vec<Option<Reply>> = (0..n).map(|_| None).collect();
            let mut first_error: Option<Reply> = None;
            while let Some((i, reply)) = sub_rx.recv().await {
                if let Reply::Get { .. } = &reply {
                    slots[i] = Some(reply);
                } else {
                    first_error.get_or_insert(reply);
                }
            }
            let merged = first_error.unwrap_or_else(|| merge_multiget(slots));
            let _ = completed_tx.send((seq, merged)).await;
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
        route
            .route_dyn(req)
            .await
            .unwrap_or_else(|_| Reply::ServerError(Bytes::from_static(b"backend unavialable")))
    } else {
        handle.send_request(req).await
    }
}

fn merge_multiget(slots: Vec<Option<Reply>>) -> Reply {
    let mut hits = Vec::new();

    for slot in slots {
        match slot {
            Some(Reply::Get { hits: h }) => hits.extend(h),
            Some(other) => return other,
            None => return Reply::ServerError(Bytes::from_static(b"multiget: lost subreply")),
        }
    }

    Reply::Get { hits }
}
