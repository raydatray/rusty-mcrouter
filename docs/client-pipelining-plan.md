# Backend client pipelining plan

Tracks how mcrouter's backend memcache client keeps multiple requests in flight
on one connection, and how rusty-mcrouter should adapt the same shape for its
Tokio `Client`.

Scope is strictly the client-to-backend connection: request admission, write
batching, pending/in-flight tracking, reply matching, timeouts, and connection
error behavior. Proxy/thread topology is covered in
[`threading-model-plan.md`](./threading-model-plan.md), and proxy message queue
backpressure is covered in
[`message-queue-backpressure.md`](./message-queue-backpressure.md).

References to mcrouter source assume the upstream repo is checked out elsewhere;
paths are noted as `mcrouter/...` without clickable links. Line numbers reflect
the state of the code at the time of writing and may drift.

Remote source was spot-checked against `facebook/mcrouter` commit
`0c2a7455073db017cbe6b2f6a2ab6e2631af599e`; no separate upstream markdown docs
for these internals were found, so source comments/code are the authoritative
references.

## TL;DR

| Area | mcrouter | rusty now | Planned rusty model |
|---|---|---|---|
| API shape | request context waits on baton | `Client::send(&mut self, ...)` | shareable `Client::send(&self, Request)` |
| Backend concurrency | many in-flight requests per socket | one request per socket RTT | one writer + one reader + FIFO waiters |
| Pending tracking | pending/write/pending-reply/replied queues | none | pending reply FIFO + command channel |
| Reply matching | ASCII FIFO, Caret by reqId | one outstanding reply | ASCII FIFO only for now |
| Write path | delayed event-loop write batching + `writev` | serialize + `write_all` per request | writer task serializes requests as they arrive |
| Backpressure | `maxPending` + `maxInflight` | external `Mutex<Client>` serializes all | bounded command queue + optional in-flight semaphore |
| Error handling | fail pending/sent queues on connect/read/write errors | current caller gets error | fail all pending reply waiters; close client |

The important local fix is to remove this whole-RTT mutex from
`DestinationRoute`:

```rust
pub struct DestinationRoute {
    client: Mutex<Client>,
}

async fn route(&self, req: Request) -> Result<Reply> {
    let mut client = self.client.lock().await;
    client.send(&req).await
}
```

The target shape is:

```text
route tasks
  │
  │ Client::send(req)
  ▼
bounded command queue
  │
  ▼
client actor owns backend socket
  ├── writer side serializes/writes requests
  ├── pending FIFO stores reply waiters
  └── reader side parses replies and completes oldest waiter
```

---

## Current rusty client

`rusty-mcrouter-net/src/client.rs` has one `TcpStream` and one read buffer:

```rust
pub struct Client {
    stream: TcpStream,
    buf: BytesMut,
}
```

`send` takes `&mut self`, writes the request, then reads until one reply is
complete:

```rust
pub async fn send(&mut self, req: &Request) -> Result<Reply> {
    let mut send_buf = BytesMut::new();
    req.serialize_into(&mut send_buf);
    self.stream.write_all(&send_buf).await?;

    loop {
        let n = self.stream.read_buf(&mut self.buf).await?;
        if n == 0 {
            return Err(NetError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "backend closed connection mid-reply",
            )));
        }
        if let Some(reply) = parse_reply(&mut self.buf)? {
            return Ok(reply);
        }
    }
}
```

That is correct for one request at a time, but it prevents pipelining. The
caller must hold a mutable borrow for the full round trip. `DestinationRoute`
therefore wraps the client in `tokio::sync::Mutex` and holds the lock for the
entire backend RTT.

Current behavior:

```text
route task 1 ─┐
route task 2 ─┼── Mutex<Client> ── write req1 ── wait reply1 ── write req2 ── wait reply2
route task 3 ─┘
```

Planned behavior:

```text
route task 1 ── send req1 ─┐
route task 2 ── send req2 ─┼── backend socket has req1, req2, req3 in flight
route task 3 ── send req3 ─┘
```

---

## mcrouter public client shape

mcrouter exposes `AsyncMcClient`, which is a thin public wrapper over
`AsyncMcClientImpl`.

Relevant source:

- `mcrouter/lib/network/AsyncMcClient.h:30-39` describes the public client and
  says outstanding requests keep the base implementation alive.
- `mcrouter/lib/network/AsyncMcClient.h:75-89` exposes `sendSync` from fiber
  context.
- `mcrouter/lib/network/AsyncMcClient.h:91-109` exposes `setThrottle` with
  `maxInflight` and `maxPending`.
- `mcrouter/lib/network/AsyncMcClient.h:111-115` exposes request queue stats.
- `mcrouter/lib/network/AsyncMcClient-inl.h:40-49` forwards `sendSync` and
  `setThrottle` to `AsyncMcClientImpl`.

Although the public method is called `sendSync`, it is sync only from the caller
fiber's perspective. Internally the client is asynchronous: the request is
queued, the event base writes it later, and the fiber waits on a baton.

```text
fiber calls sendSync(request)
  │
  ▼
create McClientRequestContext on fiber stack
  │
  ▼
sendCommon(ctx) queues the request
  │
  ▼
fiber waits on ctx.baton
  │
  ▼
read callback parses reply
  │
  ▼
queue_.reply(...) stores reply and posts baton
  │
  ▼
sendSync returns reply to caller
```

---

## Request context and queue states

mcrouter stores every request in a `McClientRequestContextBase` while it moves
through the client.

Relevant source:

- `mcrouter/lib/network/McClientRequestContext.h:30-40` defines per-request
  context storage with serialized request and id.
- `mcrouter/lib/network/McClientRequestContext.h:79-86` defines request states:
  `NONE`, `PENDING_QUEUE`, `WRITE_QUEUE`, `PENDING_REPLY_QUEUE`,
  `REPLIED_QUEUE`, `COMPLETE`.
- `mcrouter/lib/network/McClientRequestContext.h:115-119` has the waiting baton
  and timeout handler.
- `mcrouter/lib/network/McClientRequestContext.h:303-320` has the four queues:
  pending, write, pending reply, replied, plus timed-out parser initializers.

State diagram:

```text
NONE
  │ markAsPending
  ▼
PENDING_QUEUE
  │ markNextAsSending
  ▼
WRITE_QUEUE
  │ writeSuccess / markNextAsSent
  ▼
PENDING_REPLY_QUEUE
  │ reply parsed
  ▼
COMPLETE
```

There is one important special case:

```text
WRITE_QUEUE
  │ reply parsed before write callback
  ▼
REPLIED_QUEUE
  │ writeSuccess later
  ▼
COMPLETE
```

That can happen when the server replies before the client receives the socket
write callback. mcrouter handles it explicitly with `repliedQueue_`.

Queue operations:

- `McClientRequestContext.cpp:131-144` moves a request to `pendingQueue_`.
- `McClientRequestContext.cpp:150-156` moves the next pending request to
  `writeQueue_`.
- `McClientRequestContext.cpp:159-177` moves written requests to
  `pendingReplyQueue_`, or completes from `repliedQueue_`.
- `McClientRequestContext-inl.h:136-202` delivers replies either by reqId
  for out-of-order protocols or FIFO for in-order protocols.

---

## Admission and throttling

`AsyncMcClientImpl::sendSync` checks `maxPending` before accepting a new request:

```cpp
if (maxPending_ != 0 && queue_.getPendingRequestCount() >= maxPending_) {
  return createReply<Request>(ErrorReply, ...);
}
```

References:

- `mcrouter/lib/network/AsyncMcClientImpl-inl.h:24-33` checks pending capacity.
- `mcrouter/lib/network/AsyncMcClientImpl.cpp:174-177` stores throttle values.
- `mcrouter/lib/network/AsyncMcClientImpl.cpp:200-210` computes how many pending
  requests may be sent based on `maxInflight`.
- `mcrouter/lib/network/test/AsyncMcClientTestSync.cpp:223-239` tests that a
  full outstanding limit yields a local error.
- `mcrouter/lib/network/test/AsyncMcClientTestSync.cpp:590-607` tests that
  `maxInflight` gates sending while later requests remain pending.

For rusty-mcrouter, split this into two knobs:

```text
max_pending:
  bounded command queue length before a request is written

max_inflight:
  requests already written to backend and waiting for replies
```

Rust sketch:

```rust
pub struct ClientConfig {
    pub max_pending: usize,
    pub max_inflight: usize,
}

pub struct Client {
    tx: mpsc::Sender<ClientCommand>,
}

pub enum ClientCommand {
    Request {
        request: Request,
        reply_tx: oneshot::Sender<Result<Reply>>,
    },
}
```

Use bounded `mpsc` for `max_pending`, and optionally a `Semaphore` or explicit
counter inside the client actor for `max_inflight`.

---

## Write path and batching

mcrouter does not write every request immediately from the caller. It schedules
a writer loop on the event base.

Relevant source:

- `mcrouter/lib/network/AsyncMcClientImpl.cpp:81-92` delays writes until the end
  of the current event-loop turn to improve batching without adding meaningful
  latency.
- `mcrouter/lib/network/AsyncMcClientImpl.cpp:179-189` queues a request in
  `sendCommon`, schedules the writer, and starts connecting if down.
- `mcrouter/lib/network/AsyncMcClientImpl.cpp:213-223` schedules the writer loop
  only when the connection is up and there is something sendable.
- `mcrouter/lib/network/AsyncMcClientImpl.cpp:229-324` batches pending requests
  into stack `iovec`s and writes via `socket_->writev`.
- `mcrouter/lib/network/AsyncMcClientImpl.cpp:31-35` sets the batching constants:
  min/max read buffer, 128 stack iovecs, and a 24KB max batch size.

Simplified mcrouter write flow:

```text
sendCommon(ctx)
  │
  ├── queue_.markAsPending(ctx)
  ├── scheduleNextWriterLoop()
  └── attemptConnection() if down

writer loop
  │
  ├── compute numToSend = min(pending, maxInflight room)
  ├── peek next pending request
  ├── collect iovecs until stack limit or 24KB batch limit
  ├── mark each request as WRITE_QUEUE
  └── socket_->writev(..., CORK for non-final batch)

write callback
  │
  ├── mark each batch request as sent
  ├── move to PENDING_REPLY_QUEUE
  └── schedule request timeout
```

Rusty does not need `writev` in the first implementation. It should first get
the ownership and concurrency model right:

```rust
async fn client_actor(mut stream: TcpStream, mut rx: mpsc::Receiver<ClientCommand>) {
    let mut pending = VecDeque::<oneshot::Sender<Result<Reply>>>::new();
    let mut read_buf = BytesMut::with_capacity(READ_BUF_INITIAL_CAPACITY);

    loop {
        tokio::select! {
            Some(ClientCommand::Request { request, reply_tx }) = rx.recv() => {
                let mut out = BytesMut::new();
                request.serialize_into(&mut out);

                if let Err(err) = stream.write_all(&out).await {
                    let _ = reply_tx.send(Err(err.into()));
                    fail_all_pending(&mut pending, err);
                    return;
                }

                pending.push_back(reply_tx);
            }

            read = stream.read_buf(&mut read_buf), if !pending.is_empty() => {
                let n = match read {
                    Ok(n) => n,
                    Err(err) => {
                        fail_all_pending(&mut pending, err);
                        return;
                    }
                };

                if n == 0 {
                    fail_all_eof(&mut pending);
                    return;
                }

                while let Some(reply) = parse_reply(&mut read_buf)? {
                    if let Some(reply_tx) = pending.pop_front() {
                        let _ = reply_tx.send(Ok(reply));
                    }
                }
            }
        }
    }
}
```

Later, add writer-side batching by draining multiple pending commands before a
flush:

```rust
while batch.len() < MAX_WRITE_BATCH {
    match rx.try_recv() {
        Ok(cmd) => batch.push(cmd),
        Err(TryRecvError::Empty) => break,
        Err(TryRecvError::Disconnected) => break,
    }
}
```

---

## Reply matching

mcrouter supports both in-order and out-of-order protocols.

The client chooses queue mode from protocol:

- `mcrouter/lib/network/AsyncMcClientImpl.cpp:94-101` sets `outOfOrder_` to true
  for non-ASCII protocols.
- `mcrouter/lib/network/McClientRequestContext.cpp:76-83` constructs the
  request queue with that mode.
- `mcrouter/lib/network/McParser.cpp:232-247` determines protocol from the
  first byte and marks ASCII in-order, Caret out-of-order.
- `mcrouter/lib/network/McClientRequestContext.cpp:137-143` inserts requests
  into an id set only in out-of-order mode.
- `mcrouter/lib/network/McClientRequestContext-inl.h:140-169` uses request id
  lookup for out-of-order replies.
- `mcrouter/lib/network/McClientRequestContext-inl.h:170-200` uses FIFO for
  in-order replies.
- `mcrouter/lib/network/CaretHeader.h:27-43` shows Caret carries a `reqId` and
  reserves `0` for connection-control messages.
- `mcrouter/lib/network/AsyncMcClientImpl.cpp:800-802` increments client message
  ids by 2.

For rusty-mcrouter today, memcached ASCII is the only relevant protocol, so the
right first implementation is FIFO:

```text
pending_reply_fifo: VecDeque<oneshot::Sender<Result<Reply>>>

on parsed reply:
  waiter = pop_front()
  waiter.send(reply)
```

Do not add request IDs or hash maps until a protocol that actually carries
request IDs exists.

### ASCII parser initialization detail

mcrouter's ASCII parser needs to know which reply type is expected. For each
request context it stores a parser initializer:

- `mcrouter/lib/network/AsyncMcClientImpl-inl.h:35-42` constructs a request
  context with an initializer that calls `parser.expectNext<Request>()`.
- `mcrouter/lib/network/ClientMcParser-inl.h:63-77` initializes ASCII or Caret
  reply parsing for a request type.
- `mcrouter/lib/network/ClientMcParser-inl.h:190-205` asks the client for the
  next expected reply before parsing ASCII data.
- `mcrouter/lib/network/ClientMcParser-inl.h:87-101` forwards a parsed ASCII
  reply to `replyReady(..., reqId = 0)`.

rusty-mcrouter's `parse_reply` is currently type-erased into `Reply`, so it does
not need per-request parser initialization. FIFO matching is enough.

---

## Timeouts and cancellation

mcrouter distinguishes where a timeout happens:

- `McClientRequestContext-inl.h:75-113` waits on the request baton and inspects
  the current state.
- If still `PENDING_QUEUE`, it removes the request from pending and returns
  `Client queue timeout`.
- If `PENDING_REPLY_QUEUE`, it removes the request from pending replies and
  returns `Reply timeout`.
- If `REPLIED_QUEUE`, it already has the reply but still waits for the socket
  write callback before completing.
- `McClientRequestContext.cpp:218-228` stores parser initializers for timed-out
  in-order requests so future wire replies can still be parsed and discarded in
  the right shape.

For rusty-mcrouter's first pass, keep timeout scope simpler:

```text
client send timeout:
  optional caller-side timeout around send(req).await

on timeout before reply:
  remove waiter or mark waiter canceled
  keep parser FIFO alignment by retaining a tombstone until the wire reply arrives
```

That tombstone matters. In ASCII, if request 2 times out but its reply later
arrives, the parser must consume that reply before delivering request 3's reply.

Rust sketch:

```rust
enum PendingReply {
    Waiting(oneshot::Sender<Result<Reply>>),
    Canceled,
}

while let Some(reply) = parse_reply(&mut read_buf)? {
    match pending.pop_front() {
        Some(PendingReply::Waiting(tx)) => {
            let _ = tx.send(Ok(reply));
        }
        Some(PendingReply::Canceled) => {
            // Reply belonged to a timed-out request. Consume and drop it.
        }
        None => {
            // Unexpected backend data: close/fail the client.
        }
    }
}
```

If we avoid per-request timeouts initially, this becomes easier: no canceled
tombstones are needed, and EOF/error simply fails all outstanding waiters.

---

## Connection lifecycle and failures

mcrouter separates pending-not-yet-sent from sent/in-flight requests:

- `AsyncMcClientImpl.cpp:345-430` starts a connection when the first pending
  request arrives.
- `AsyncMcClientImpl.cpp:432-536` marks the connection up, schedules writes,
  constructs the parser, and starts reading.
- `AsyncMcClientImpl.cpp:538-585` handles connect failure and fails all pending
  requests if retries are exhausted.
- `AsyncMcClientImpl.cpp:587-638` handles shutdown/errors, fails sent requests,
  optionally fails pending requests, and reconnects if pending requests remain.
- `AsyncMcClientImpl.cpp:652-665` maps EOF/read errors into shutdown.
- `AsyncMcClientImpl.cpp:688-713` handles write errors by moving the write batch
  forward and shutting down.

Rusty first pass can be simpler:

```text
connect once at Client::connect
if read/write/protocol error occurs:
  fail current request if needed
  fail all pending waiters
  close command channel / mark client closed
  do not reconnect automatically yet
```

Automatic reconnect can come later. The first correctness requirement is that no
waiter hangs and ASCII reply ordering is not corrupted.

---

## Recommended rusty implementation

### Public API

Make `Client` cheap to clone and share:

```rust
#[derive(Clone)]
pub struct Client {
    tx: mpsc::Sender<ClientCommand>,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (tx, rx) = mpsc::channel(DEFAULT_PENDING_CAPACITY);

        tokio::spawn(client_actor(stream, rx));

        Ok(Self { tx })
    }

    pub async fn send(&self, req: Request) -> Result<Reply> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(ClientCommand::Request { request: req, reply_tx })
            .await
            .map_err(|_| NetError::closed("backend client closed"))?;

        reply_rx
            .await
            .map_err(|_| NetError::closed("backend client closed"))?
    }
}
```

If keeping the old call sites temporarily matters, add a compatibility method:

```rust
pub async fn send_ref(&self, req: &Request) -> Result<Reply> {
    self.send(req.clone()).await
}
```

Only do that if `Request` cloning is acceptable. Otherwise change callers to
pass owned requests.

### DestinationRoute

Remove the outer mutex:

```rust
pub struct DestinationRoute {
    client: Client,
}

impl Route for DestinationRoute {
    async fn route(&self, req: Request) -> Result<Reply> {
        self.client.send(req).await.map_err(RouteError::from)
    }
}
```

That is the critical throughput unlock.

### Actor internals

Use one actor that owns the backend socket:

```text
Client handle clones
  │
  │ ClientCommand::Request
  ▼
bounded command channel
  │
  ▼
client actor
  ├── writes serialized requests
  ├── pushes reply waiters into FIFO
  ├── reads backend replies
  └── pops FIFO waiter per parsed reply
```

Minimal actor sketch:

```rust
async fn client_actor(
    mut stream: TcpStream,
    mut rx: mpsc::Receiver<ClientCommand>,
) {
    let mut pending = VecDeque::<oneshot::Sender<Result<Reply>>>::new();
    let mut read_buf = BytesMut::with_capacity(READ_BUF_INITIAL_CAPACITY);

    loop {
        tokio::select! {
            Some(cmd) = rx.recv() => {
                if let Err(err) = write_command(&mut stream, cmd, &mut pending).await {
                    fail_all(&mut pending, err);
                    return;
                }
            }

            read = stream.read_buf(&mut read_buf), if !pending.is_empty() => {
                match read {
                    Ok(0) => {
                        fail_all_eof(&mut pending);
                        return;
                    }
                    Ok(_) => {
                        while let Some(reply) = parse_reply(&mut read_buf)? {
                            match pending.pop_front() {
                                Some(tx) => { let _ = tx.send(Ok(reply)); }
                                None => {
                                    fail_unexpected_reply();
                                    return;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        fail_all(&mut pending, err);
                        return;
                    }
                }
            }
        }
    }
}
```

For an initial implementation, do not attempt all mcrouter details at once:

```text
include now:
  bounded pending queue
  FIFO reply matching
  fail all waiters on EOF/read/write/protocol error
  remove DestinationRoute Mutex<Client>

defer:
  reconnect
  per-request timeout tombstones
  writev/scatter-gather
  CORK-like batching
  out-of-order request IDs
```

---

## Tests to add first

These tests should drive the implementation.

### 1. Concurrent sends are written before first reply

Backend test server should read two requests before writing any reply.

```text
client sends req1 and req2 concurrently
server observes both wire requests
server then replies reply1, reply2
both client futures complete
```

This proves we removed the RTT serialization.

### 2. Replies match FIFO

```text
send get k1, get k2
backend replies VALUE k1..., then VALUE k2...
future 1 gets k1
future 2 gets k2
```

### 3. EOF fails all pending waiters

```text
send req1, req2
backend closes before replies
both futures return UnexpectedEof / NetError
```

### 4. Protocol error fails the client

```text
send req1, req2
backend sends malformed reply
req1 fails with protocol error
req2 does not hang
new sends fail because actor closed
```

### 5. DestinationRoute routes concurrently

```text
two route.route(req) futures against same DestinationRoute
backend sees both requests before first reply
```

That last test is the real integration proof.

---

## Implementation order

1. **Change `Client` into a handle + actor.**
   Keep one backend TCP connection. The actor owns the stream and read buffer.

2. **Add FIFO pending reply queue.**
   Push a reply waiter after a successful request write. Pop one waiter per
   parsed reply.

3. **Fail all waiters on terminal socket/parser errors.**
   Do not let any `send()` future hang.

4. **Update `DestinationRoute` to remove `Mutex<Client>`.**
   The client owns synchronization internally.

5. **Add concurrency tests.**
   Specifically prove multiple requests can be written before the first reply.

6. **Only then add advanced fidelity.**
   Reconnect, explicit `maxInflight`, timeout tombstones, and batched writes can
   be layered in after the core pipeline is correct.

The target is not to clone all of `AsyncMcClientImpl` immediately. The target is
to preserve the core mcrouter invariant:

```text
one backend connection can have many outstanding requests,
and ASCII replies complete callers in wire order.
```
