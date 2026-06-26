# rusty-mcrouter request timeouts (design)

> Status: **Proposed (2026-06-26)**
> Mirrors: mcrouter's `AsyncMcClient` request + connect timeouts. **Full source-verified reference (cited by symbol): [`../mcrouter/timeouts.md`](../mcrouter/timeouts.md)** — read that first. Source pinned to `facebook/mcrouter @ 42aa391189c7`.
> Implemented in: TBD — `rusty-mcrouter-net/src/client/{handle,connection,config}.rs`, `rusty-mcrouter-net/src/lib.rs` (`NetError`).
> Related: [`./write-batching.md`](./write-batching.md) (sibling client-layer change), [`./threading-model.md`](./threading-model.md) (the per-request dispatch this rides on), [`../architecture/backend-client.md`](../architecture/backend-client.md) (as-built client). **Prerequisite for `./failover.md`** (timeout is the dominant real-world failover trigger) and for TKO (soft TKO is timeout-driven).

A backend `send` currently waits **forever**. This adds a bounded deadline to the
two places a request can hang — establishing the connection and awaiting a reply —
designed for our pipelined actor client rather than transcribed from mcrouter's
fiber model. This is the foundational primitive under failover and TKO.

---

## tl;dr

- Today `Client::send` does `reply_rx.await` with no deadline, and `Client::connect`
  does `TcpStream::connect` with no deadline. A slow or black-hole backend hangs the
  caller indefinitely. There is no `NetError::Timeout`.
- **Request timeout is a caller-side deadline.** Wrap the reply `oneshot` await in
  `tokio::time::timeout`. On elapse, return `NetError::Timeout` and drop the receiver.
  **The connection actor (`run()`) is not touched at all** — zero changes to the hot loop.
- **The load-bearing invariant:** because the client is **FIFO-pipelined**, a timed-out
  request's slot **must stay in the `pending` demux queue**; its late reply is matched
  and **discarded on arrival**. Removing the slot early would shift every subsequent
  reply onto the wrong caller. mcrouter reaches the same end *differently* — it **removes**
  the request from `pendingReplyQueue_` and stores a **typed parser-initializer** in
  `timedOutInitializers_` to parse-and-drop the late reply in order. Ours is simpler
  (leave the slot, drop on pop) because our ASCII reply parser self-frames — see below.
- **Connect timeout** wraps `TcpStream::connect` → `NetError::ConnectTimeout`.
- Two error variants, not one: `Timeout` (awaiting a reply) and `ConnectTimeout`
  (establishing the connection) — so TKO can later map them to **soft** vs **hard**
  knockout the way mcrouter does (`isSoftTkoErrorResult` = `TIMEOUT`,
  `isHardTkoErrorResult` ⊇ `CONNECT_TIMEOUT`).
- **v1 sources both deadlines from `ClientConfig`** (one default per client). Per-route /
  topology-aware timeouts (mcrouter's `cross_region`/`cross_cluster`/`within_cluster`
  precedence) are deferred — they need a timeout argument threaded through `Backend::send`.

---

## the problem (what hangs today)

Two unbounded waits, both in `rusty-mcrouter-net/src/client/`:

```rust
// handle.rs — Client::send (BEFORE)
self.tx.send(ClientCommand { request, reply_tx }).await...?;
match reply_rx.await {                 // <-- no deadline: hangs until the actor replies or dies
    Ok(result) => result,
    Err(_) => Err(NetError::ClientClosed),
}
```
```rust
// handle.rs — Client::connect_with_config (BEFORE)
let stream = TcpStream::connect(addr).await?;   // <-- no deadline: a black-hole host hangs
                                                //     until the OS TCP timeout (~minutes)
```

`NetError` has `Io`, `Protocol`, `NoAddresses`, `WorkerClosed`, `ClientClosed` — **no timeout
variant** — so even if we detected a stall there is no way to report it. And the route layer
(`RouteError::Backend(NetError)`) has nothing for failover to classify as "try the next child."

This is the gap that makes failover toothless: the most common production failure is a
*slow* backend, not a *dead* one, and we currently can't bound it.

---

## what mcrouter does (reference)

The full, source-verified write-up — the timeout taxonomy, config-time value
resolution + region/cluster precedence, the connect/retry path, the
`McClientRequestContextQueue` state machine, the `timedOutInitializers_` discard
handshake, the fiber-baton timeout, and how timeouts feed TKO — lives in
[`../mcrouter/timeouts.md`](../mcrouter/timeouts.md). **Read that first.** This
section records only the two facts our design leans on:

1. **A timeout fails the caller, not the connection.** mcrouter resumes the
   request's fiber with `carbon::Result::TIMEOUT` and leaves the connection up; a
   single timeout never tears it down (knocking a destination out after N is TKO's
   separate job).
2. **The pipelined reply stream stays demux-correct.** mcrouter *removes* the
   timed-out request from `pendingReplyQueue_` and stashes a typed parser-initializer
   in `timedOutInitializers_` so the late reply is parsed-and-discarded in order.

Our design reproduces both invariants with a deliberately simpler mechanism
(caller-side deadline + leave-the-slot-in-`pending` + drop-on-pop), justified below.

---

## our design

### Request timeout = a caller-side deadline on the reply oneshot

The asynchrony in our client is the `oneshot` between `Client::send` and the actor. The deadline
belongs exactly there:

```rust
// handle.rs — Client::send (AFTER). `request_timeout: Duration` is carried on Client,
// set from ClientConfig at connect time.
self.tx.send(ClientCommand { request, reply_tx }).await
    .map_err(|_| NetError::ClientClosed)?;

match tokio::time::timeout(self.request_timeout, reply_rx).await {
    Ok(Ok(reply))   => reply,                  // reply arrived in time
    Ok(Err(_))      => Err(NetError::ClientClosed), // actor dropped the sender (conn died)
    Err(_elapsed)   => Err(NetError::Timeout),      // deadline hit; reply_rx is dropped here
}
```

`Client` gains one field (`request_timeout: Duration`); `Client` stays `Clone` (`Duration: Copy`).
**`ClientConnection::run()` does not change** — the actor never learns about the deadline. That is
the point: timeouts add nothing to the hot select-loop, no per-entry timers, no extra branches on
the read path.

### The FIFO-discard invariant (the part to get right)

Our actor matches replies to callers **in wire order** via `pending: VecDeque<oneshot::Sender>`
(`connection.rs`: `write_batch` pushes back, `deliver_replies` pops front). When a caller times out,
it drops its receiver — but **its slot must remain in `pending`**, because the wire still owes a
reply in that position and the actor demuxes positionally.

Walk it through — three pipelined requests `A, B, C`, `pending = [a, b, c]`:

1. `A` times out caller-side → caller gets `NetError::Timeout`, drops `a`'s receiver.
   `pending` is still `[a, b, c]` (the actor is unaware).
2. Reply `A` arrives first (ordered wire). `deliver_replies` pops `a`,
   `let _ = a.send(Ok(reply_a))` → receiver gone → **silently discarded**. `pending = [b, c]`.
3. Reply `B` arrives → pops `b` → delivered to `B`'s caller. Correct.

If instead we eagerly removed `a` from `pending` on timeout, reply `A` would pop `b` and **`B` would
receive `A`'s reply** — silent cross-talk. So the rule is: *a timeout never mutates `pending`; the
stale reply is dropped when it surfaces.* `deliver_replies` already tolerates a dropped receiver
(`let _ = tx.send(...)`), so **no actor change is required** to make this safe — it falls out of the
existing code.

This reaches the same invariant as mcrouter's `removePendingReply` + `timedOutInitializers_` (late
reply parsed-and-dropped in order), but is **simpler**: mcrouter must store a *typed parser-initializer*
to consume the timed-out reply off the wire (its machinery also serves Caret's out-of-order reqId
demux), whereas our `parse_reply` is **self-framing** for ASCII (`VALUE…END` / single-line replies
delimit themselves), so the dropped `oneshot` left in `pending` needs no stored type. Minor bonus:
because we never mutate `pending`, requests may time out in **any order** and demux still stays
aligned — mcrouter's in-order path asserts only the *front* request can time out.

### Connect timeout

Bound establishment in `connect_with_config`:

```rust
let stream = match tokio::time::timeout(cfg.connect_timeout, TcpStream::connect(addr)).await {
    Ok(Ok(s))     => s,
    Ok(Err(e))    => return Err(NetError::Io(e)),
    Err(_elapsed) => return Err(NetError::ConnectTimeout),
};
```

mcrouter additionally retries connect (`connect_timeout_retries`); we do **not** retry in v1 — one
attempt, then surface `ConnectTimeout` (the builder's `ConnectFailed` path already handles a failed
connect).

### Error model

Add to `NetError`:

```rust
Timeout,         // request: no reply within request_timeout
ConnectTimeout,  // connect: no connection within connect_timeout
```

Two variants rather than one because the future TKO classifier needs the distinction (soft vs hard).
Both are field-less, so the hand-written `NetError: Clone` impl extends trivially (no `io::Error`
clone problem). They propagate as `RouteError::Backend(NetError::Timeout | ConnectTimeout)`.

### Where the deadline values come from

`ClientConfig` grows two fields with sane defaults; that is the **whole** v1 configuration surface:

```rust
pub struct ClientConfig {
    pub max_pending: usize,                 // existing
    pub read_buf_initial_capacity: usize,   // existing
    pub request_timeout: Duration,          // NEW (e.g. 1000 ms)
    pub connect_timeout: Duration,          // NEW (e.g. 250 ms)
}
```

One default timeout per client is enough to unblock **in-order failover**: each failover child wraps
its own `Client`, so each already times out independently and failover just observes the resulting
`Timeout`. Per-route override (different timeouts for different routes) and mcrouter's
region/cluster precedence are a separate, larger change — see below.

---

## scope

**v1 (this design):**
- `request_timeout` + `connect_timeout` on `ClientConfig`.
- Caller-side request deadline in `Client::send`; connect deadline in `connect_with_config`.
- `NetError::Timeout` + `NetError::ConnectTimeout`; FIFO-discard behavior (free from the existing actor).
- No changes to `ClientConnection::run()` or the `Backend` trait.

**Deferred (named so failover/TKO can build on a known shape):**
- **Per-route / topology-aware timeouts** (mcrouter's `cross_region`/`cross_cluster`/`within_cluster`
  precedence) — requires threading a timeout into `Backend::send(req, timeout)` and through the route
  graph. Touches the `Backend` trait, so it is its own diff.
- **`waiting_request_timeout`** — bounding time spent *queued* before the write. Our `write_batch`
  drains immediately so there is no real queue wait yet; revisit if we add `maxInflight` throttling.
- **Deadline propagation** (`DEADLINE_EXCEEDED`) — a per-request budget carried across hops.
- **Connect retries** (`connect_timeout_retries`).
- **Connection liveness teardown** — see the boundary note below; belongs to TKO/connection-health.

---

## the hung-but-open backend boundary (an honest limitation)

The caller-side deadline guarantees the **caller's** latency SLA: every `send` returns within
`request_timeout` regardless of the backend. It does **not**, by itself, clean up a backend that
accepts the connection and then goes permanently silent. In that case each caller times out promptly
(good), but their slots accumulate in `pending` as dropped-receiver senders that only drain when
*some* reply eventually arrives — and if none ever does, they sit until the connection errors or is
torn down.

That is deliberately **out of scope here**: detecting a persistently silent-but-open connection and
reconnecting/knocking it out is a *connection-health* concern, which is exactly what TKO owns
(`timeouts-until-tko`, probing). v1 request timeout is the per-request latency bound; TKO is the
per-destination liveness bound. Keeping them separate matches mcrouter (a single timeout fails the
caller; N timeouts trigger TKO).

---

## how this feeds failover & TKO

- **Failover** ([`./failover.md`](./failover.md)): `Timeout` and `ConnectTimeout` are
  **failover-worthy** — the route tries the next child. This is the trigger that makes failover
  useful in practice (slow backend, not just dead).
- **TKO** (later): `Timeout` → **soft** TKO (mcrouter `isSoftTkoErrorResult`); `ConnectTimeout` →
  **hard** TKO (`isHardTkoErrorResult`). The two-variant error model exists now so TKO needs no
  error-type rework later.

---

## test plan

Built on the existing `scripted_backend` / `MockBackend` harness ([`./testing.md`](./testing.md)):

- **Request timeout fires:** script a backend that reads the request and never writes a reply;
  assert `Client::send` returns `Err(NetError::Timeout)` within a small multiple of `request_timeout`.
- **FIFO-discard correctness (the invariant):** pipeline `A` then `B` on one client with a tiny
  `request_timeout`; have the backend reply to `A` *late* and then to `B`. Assert `A`'s caller got
  `Timeout`, and crucially that **`B`'s caller receives `B`'s reply, not `A`'s** — proving the stale
  reply was discarded and demux stayed aligned.
- **Connect timeout fires:** connect to a blackholed/unroutable address with a short
  `connect_timeout`; assert `Err(NetError::ConnectTimeout)` promptly (not an OS-length hang).
- **No regression with a fast backend:** existing client/destination tests pass unchanged with a
  generous default timeout (the deadline never trips).
- (For route-level tests) optionally add a `MockBackend::delaying(dur)` so destination/failover tests
  can exercise the timeout path without a socket.

---

## open questions / decisions

- **Default values.** Proposed `request_timeout = 1000 ms`, `connect_timeout = 250 ms` as placeholders;
  mcrouter's web flavor uses ~1 s server timeouts. Confirm before wiring into the proxy.
- **Per-route timeout now or later?** Decided: **later.** A single per-client default unblocks failover;
  changing `Backend::send` to carry a timeout is a separate diff so this one stays minimal and the
  `Backend` trait stays stable while the route catalog grows.
- **Is caller-side vs actor-side the right call?** Decided: **caller-side.** It keeps the actor's
  select-loop untouched and gets the FIFO-discard behavior for free. Actor-side per-entry deadline
  timers would buy proactive slot cleanup, but that overlaps with TKO/liveness and adds hot-loop
  complexity we don't need yet.

---

## done when

- `NetError::Timeout` and `NetError::ConnectTimeout` exist and are `Clone`.
- `ClientConfig` carries `request_timeout` + `connect_timeout`; `Client::send` enforces the former,
  `connect_with_config` the latter.
- `ClientConnection::run()` is unchanged; a timed-out request's late reply is discarded without
  disturbing FIFO demux (covered by the discard test).
- The proxy's `ClientFactory` passes a configured `ClientConfig` (no longer `ClientConfig::default()`
  implicitly) so production runs with real deadlines.
- `lsp_diagnostics` / `clippy` clean; the four tests above pass; failover can classify the two new
  results as failover-worthy.
