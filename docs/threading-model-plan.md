# Threading model plan: proxy queues and request routing

Tracks the next threading-model changes needed to make rusty-mcrouter closer to
Meta's mcrouter. Scope is strictly the proxy/server threading model: accepted
socket ownership, proxy message queues, request-to-thread selection, and
fiber-like per-request routing concurrency.

Backend memcache connection pipelining is intentionally out of scope here. That
belongs with the `Client` / destination route work already tracked in
[`mcrouter-comparison.md`](./mcrouter-comparison.md).

References to mcrouter source assume the upstream repo is checked out elsewhere;
paths are noted as `mcrouter/...` without clickable links. Line numbers reflect
the state of the code at the time of writing and may drift.

## TL;DR

| Area | rusty now | mcrouter shape | Planned rusty shape |
|---|---|---|---|
| Proxy entry point | Connection task calls route directly | Requests enter `Proxy::messageQueue_` | Connection task submits `ProxyMessage::Request` |
| Routing thread choice | Socket assigned round-robin at accept time | Same-thread, fixed remote, or affinitized remote | `ThreadMode` chooses target proxy per request |
| Route execution | Session awaits each route inline | Proxy schedules route work on `FiberManager` | Proxy queue drains and `spawn_local`s route tasks |
| Client connection ownership | Connection task also owns routing decision | Client I/O and proxy routing can be different threads | Connection thread writes replies; target proxy routes |

Current TODO markers live at:

- [`main.rs:81`](../rusty-mcrouter/src/main.rs) — socket queues are not proxy
  message queues yet.
- [`proxy_thread.rs:37`](../rusty-mcrouter/src/proxy_thread.rs) — `LocalSet`
  is the closest analogue to mcrouter's `FiberManager`.
- [`proxy_thread.rs:79`](../rusty-mcrouter/src/proxy_thread.rs) — direct route
  closure should become proxy message handling.
- [`proxy_thread.rs:91`](../rusty-mcrouter/src/proxy_thread.rs) — `serve_worker`
  should choose a thread mode before enqueueing requests.
- [`server.rs:70`](../rusty-mcrouter-net/src/server.rs) — accepted sockets are
  round-robin today; per-request affinity belongs later.
- [`server.rs:112`](../rusty-mcrouter-net/src/server.rs) — connection tasks own
  routing directly today.
- [`server.rs:130`](../rusty-mcrouter-net/src/server.rs) — pipelined requests are
  routed inline instead of as independent route tasks.

---

## Current rusty model

rusty-mcrouter already has the right outer topology: `N` proxy OS threads, each
with a Tokio current-thread runtime and `LocalSet`.

```text
                         ┌──────────────────────────────┐
                         │            main              │
                         │                              │
                         │  parse config / CLI          │
                         │  create N proxy threads      │
                         │  create N socket queues      │
                         └──────────────┬───────────────┘
                                        │
          ┌─────────────────────────────┼─────────────────────────────┐
          │                             │                             │
          ▼                             ▼                             ▼
┌─────────────────────┐       ┌─────────────────────┐       ┌─────────────────────┐
│ proxy thread 0      │       │ proxy thread 1      │       │ proxy thread N-1    │
│                     │       │                     │       │                     │
│ Tokio current_thread│       │ Tokio current_thread│       │ Tokio current_thread│
│ LocalSet            │       │ LocalSet            │       │ LocalSet            │
│                     │       │                     │       │                     │
│ route graph 0       │       │ route graph 1       │       │ route graph N-1     │
│ socket queue 0      │       │ socket queue 1      │       │ socket queue N-1    │
└─────────────────────┘       └─────────────────────┘       └─────────────────────┘
```

Relevant local source:

- `rusty-mcrouter/src/main.rs:82-84` creates one socket handoff channel per
  proxy thread.
- `rusty-mcrouter/src/main.rs:104-118` spawns named OS threads.
- `rusty-mcrouter/src/proxy_thread.rs:32-39` creates one current-thread Tokio
  runtime and `LocalSet` per proxy thread.
- `rusty-mcrouter/src/proxy_thread.rs:67-79` builds one route graph per proxy
  thread and shares it locally with `Rc`.

### Accept and socket dispatch

Only the first `M = num_listening_sockets` proxy threads also listen. Accepted
sockets are dispatched round-robin to worker socket queues.

```text
Clients
  │
  │ TCP connect
  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ kernel listen sockets                                                │
│                                                                     │
│ if M = 1: one listener                                               │
│ if M > 1: SO_REUSEPORT listeners on proxy 0..M-1                    │
└───────────────┬───────────────────────┬─────────────────────────────┘
                │                       │
                ▼                       ▼
       ┌────────────────┐      ┌────────────────┐
       │ proxy thread 0 │      │ proxy thread 1 │
       │ listener role  │      │ listener role  │
       └───────┬────────┘      └───────┬────────┘
               │                       │
               │ accepted TcpStream    │ accepted TcpStream
               │                       │
               └───────────┬───────────┘
                           ▼
              round-robin socket dispatch
                           │
       ┌───────────────────┼───────────────────┐
       ▼                   ▼                   ▼
socket queue 0       socket queue 1       socket queue N-1
```

Local source:

- `rusty-mcrouter-net/src/server.rs:52-77` accepts sockets and round-robins them
  across worker queues.
- `rusty-mcrouter-net/src/server.rs:68` converts a Tokio stream to
  `std::net::TcpStream` before handoff.
- `rusty-mcrouter-net/src/server.rs:101` re-registers the socket on the worker
  runtime.

### Direct route call today

After a socket lands on a worker, the connection task routes directly:

```text
socket queue i
    │
    ▼
serve_worker on proxy thread i
    │
    ▼
spawn_local connection task
    │
    ▼
serve_session
    │
    ▼
parse request
    │
    ▼
route graph i directly
    │
    ▼
write reply
```

Current code shape:

```rust
let handler = move |req| {
    let route = Rc::clone(&route);
    async move {
        route.route_dyn(req).await.unwrap_or_else(|_| {
            Reply::ServerError(Bytes::from_static(b"backend unavailable"))
        })
    }
};
```

Then `serve_session` awaits that handler inline:

```rust
while let Some(req) = parse_request(&mut buf)? {
    let reply = (*handler)(req).await;
    let mut out = BytesMut::new();
    reply.serialize_into(&mut out);
    stream.write_all(&out).await?;
}
```

This is simple, but it means the connection task is also the routing entry
point. mcrouter does not model it that way.

---

## mcrouter reference model

mcrouter's equivalent proxy path is:

```text
CarbonRouterClient
  │
  │ ProxyMessage::REQUEST
  ▼
Proxy::messageQueue_
  │
  │ drained on proxy EventBase
  ▼
Proxy::messageReady()
  │
  ▼
ProxyRequestContext::startProcessing()
  │
  ▼
Proxy::processRequest()
  │
  ▼
FiberManager::addTaskFinally()
  │
  ▼
route handle tree
```

Relevant mcrouter source:

- `mcrouter/Proxy-inl.h:200-225` constructs `MessageQueue<ProxyMessage>`.
- `mcrouter/Proxy-inl.h:236-241` attaches the message queue and fiber manager
  to the proxy `VirtualEventBase`.
- `mcrouter/Proxy-inl.h:299-304` sends remote-thread requests into the message
  queue.
- `mcrouter/Proxy-inl.h:313-318` handles `ProxyMessage::REQUEST` by calling
  `ProxyRequestContext::startProcessing()`.
- `mcrouter/Proxy-inl.h:95-114` schedules route work with
  `fiberManager().addTaskFinally(...)`.
- `mcrouter/CarbonRouterClient.h:65-76` defines `SameThread`,
  `FixedRemoteThread`, and `AffinitizedRemoteThread`.
- `mcrouter/CarbonRouterClient-inl.h:291-312` distinguishes remote-thread queue
  send from same-thread direct dispatch.
- `mcrouter/CarbonRouterClient-inl.h:370-410` chooses an affinitized proxy per
  request.
- `mcrouter/lib/MessageQueue.h:181-221` attaches the queue to the event base and
  drains it from the event loop.

The important difference is not naming. In mcrouter, **the proxy is an actor**:
all request routing enters through the proxy's event-loop-owned queue.

---

## Planned rusty model

Add a proxy message queue beside the existing socket queue.

```text
┌──────────────────────────────────────────┐
│ proxy thread i                           │
│                                          │
│  socket queue i                          │
│       │                                  │
│       ▼                                  │
│  serve_worker                            │
│       │                                  │
│       ▼                                  │
│  connection task                         │
│       │                                  │
│       │ parse request                    │
│       ▼                                  │
│  choose target proxy                     │
│       │                                  │
└───────┼──────────────────────────────────┘
        │
        │ ProxyMessage::Request
        ▼
┌──────────────────────────────────────────┐
│ target proxy thread j                    │
│                                          │
│  proxy message queue j                   │
│       │                                  │
│       ▼                                  │
│  spawn_local route task                  │
│       │                                  │
│       ▼                                  │
│  route graph j                           │
│       │                                  │
│       ▼                                  │
│  reply oneshot back to connection task   │
└──────────────────────────────────────────┘
        │
        ▼
connection task on proxy thread i writes reply
```

The new rule becomes:

```text
connection I/O thread != necessarily routing proxy thread
```

That is what unlocks mcrouter-style thread modes.

---

## Proxy message queue sketch

Start with a small actor interface:

```rust
pub enum ProxyMessage {
    Request(ProxyRequest),
    Shutdown,
}

pub struct ProxyRequest {
    pub request: Request,
    pub reply_tx: tokio::sync::oneshot::Sender<Reply>,
}

#[derive(Clone)]
pub struct ProxyHandle {
    id: usize,
    tx: tokio::sync::mpsc::Sender<ProxyMessage>,
}

impl ProxyHandle {
    pub async fn send_request(&self, request: Request) -> Reply {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        let msg = ProxyMessage::Request(ProxyRequest {
            request,
            reply_tx,
        });

        if self.tx.send(msg).await.is_err() {
            return Reply::ServerError(Bytes::from_static(b"proxy unavailable"));
        }

        reply_rx
            .await
            .unwrap_or_else(|_| Reply::ServerError(Bytes::from_static(b"proxy dropped request")))
    }
}
```

Each proxy thread owns the receiver and schedules route work locally:

```rust
async fn run_proxy_queue(
    mut rx: tokio::sync::mpsc::Receiver<ProxyMessage>,
    route: Rc<dyn DynRoute>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            ProxyMessage::Request(req) => {
                let route = Rc::clone(&route);

                tokio::task::spawn_local(async move {
                    let reply = route
                        .route_dyn(req.request)
                        .await
                        .unwrap_or_else(|_| {
                            Reply::ServerError(Bytes::from_static(b"backend unavailable"))
                        });

                    let _ = req.reply_tx.send(reply);
                });
            }

            ProxyMessage::Shutdown => break,
        }
    }
}
```

This intentionally mirrors `Proxy::messageReady()` + `FiberManager::addTaskFinally()`:

```text
mcrouter                         rusty equivalent
──────────────────────────────   ──────────────────────────────
MessageQueue<ProxyMessage>       mpsc<ProxyMessage>
ProxyRequestContext              ProxyRequest + oneshot reply channel
FiberManager::addTaskFinally     spawn_local route task
VirtualEventBase/EventBase       Tokio current_thread runtime + LocalSet
```

---

## Thread mode sketch

Add explicit target-selection modes:

```rust
#[derive(Clone, Copy)]
pub enum ThreadMode {
    SameThread,
    FixedRemote { proxy_id: usize },
    AffinitizedRemote,
}
```

Choose a proxy for each request:

```rust
pub struct ProxySet {
    proxies: Vec<ProxyHandle>,
}

impl ProxySet {
    pub fn choose_proxy(
        &self,
        mode: ThreadMode,
        current_proxy_id: usize,
        req: &Request,
    ) -> ProxyHandle {
        let idx = match mode {
            ThreadMode::SameThread => current_proxy_id,

            ThreadMode::FixedRemote { proxy_id } => proxy_id % self.proxies.len(),

            ThreadMode::AffinitizedRemote => hash_request(req) % self.proxies.len(),
        };

        self.proxies[idx].clone()
    }
}
```

Use the first request key as the initial affinity input:

```rust
fn hash_request(req: &Request) -> usize {
    use std::hash::{Hash, Hasher};

    let mut h = std::collections::hash_map::DefaultHasher::new();

    match req {
        Request::Get { keys } => {
            if let Some(key) = keys.first() {
                key.hash(&mut h);
            }
        }
        Request::Set { key, .. } => key.hash(&mut h),
        _ => std::mem::discriminant(req).hash(&mut h),
    }

    h.finish() as usize
}
```

Thread-mode diagrams:

### SameThread

```text
connection on proxy i
    │
    ▼
choose proxy i
    │
    ▼
proxy message queue i
    │
    ▼
route graph i
```

### FixedRemoteThread

```text
connection on proxy i
    │
    ▼
fixed target proxy k
    │
    ▼
proxy message queue k
    │
    ▼
route graph k
```

### AffinitizedRemoteThread

```text
connection on proxy i
    │
    ├── get foo ── hash(foo) % N = 2 ──► proxy queue 2
    │
    ├── get bar ── hash(bar) % N = 0 ──► proxy queue 0
    │
    └── get baz ── hash(baz) % N = 2 ──► proxy queue 2
```

This is the main behavior difference from today's socket-only round-robin:
accepted connection ownership stays round-robin, but routing ownership can be
chosen per request.

---

## Fiber-like route concurrency sketch

Today `serve_session` drains complete requests but awaits each route before
starting the next route. To behave more like mcrouter fibers, parse and enqueue
requests independently, then preserve write ordering at the connection boundary.

```rust
while let Some(req) = parse_request(&mut buf)? {
    let seq = next_seq;
    next_seq += 1;

    let target = proxies.choose_proxy(thread_mode, current_proxy_id, &req);
    let completed = completed_tx.clone();

    tokio::task::spawn_local(async move {
        let reply = target.send_request(req).await;
        let _ = completed.send((seq, reply));
    });
}
```

Then write replies in request order:

```rust
let mut next_write_seq = 0usize;
let mut pending = BTreeMap::<usize, Reply>::new();

while let Some((seq, reply)) = completed_rx.recv().await {
    pending.insert(seq, reply);

    while let Some(reply) = pending.remove(&next_write_seq) {
        let mut out = BytesMut::new();
        reply.serialize_into(&mut out);
        stream.write_all(&out).await?;
        next_write_seq += 1;
    }
}
```

This creates the same conceptual split as mcrouter:

```text
client connection task
  ├── parses requests
  ├── submits each request to a proxy actor
  ├── receives replies asynchronously
  └── serializes writes back to the socket

target proxy task
  ├── drains proxy messages
  ├── schedules route tasks on the proxy LocalSet
  └── completes reply channels
```

---

## Full planned request lifecycle

```text
client
  │
  │ TCP request
  ▼
listener proxy thread
  │
  │ accept()
  ▼
round-robin socket dispatch
  │
  ▼
connection-owning proxy thread i
  │
  │ parse request
  ▼
choose routing proxy using ThreadMode
  │
  ├── SameThread:            i
  ├── FixedRemoteThread:     fixed k
  └── AffinitizedRemote:     hash(req) % N
  │
  ▼
proxy message queue target
  │
  │ drain on target proxy event loop
  ▼
spawn route task on target proxy LocalSet
  │
  ▼
target proxy route graph
  │
  ▼
reply oneshot
  │
  ▼
connection task on proxy i
  │
  ▼
write response to client
```

---

## Suggested implementation order

1. **Add `ProxyMessage`, `ProxyRequest`, and `ProxyHandle`.**
   Keep accepted-socket distribution as-is. The first behavior-preserving step
   is to route all requests through the same proxy's message queue.

2. **Make `serve_session` target a `ProxyHandle` instead of a route closure.**
   Start with `ThreadMode::SameThread` only. This gets the actor boundary in
   place without changing request placement.

3. **Add `ThreadMode`.**
   Implement `SameThread`, then `FixedRemote`, then `AffinitizedRemote`.

4. **Make session routing concurrent but ordered.**
   Spawn per-request route submissions and preserve client write order with
   sequence numbers.

5. **Then fix backend client pipelining.**
   The proxy model above exposes routing concurrency. Backend throughput still
   depends on replacing the current exclusive `Client::send(&mut self, ...)`
   shape, tracked separately.

The key architectural shift is:

```text
current: connection task routes directly
planned: connection task submits to proxy actor; proxy actor schedules routing
```
