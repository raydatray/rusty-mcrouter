# Message queue backpressure plan

Tracks how to make rusty-mcrouter's future proxy message queue closer to
mcrouter's `MessageQueue`, without immediately reimplementing Folly's full
`eventfd` + `EventBase` notifier stack.

Scope is strictly proxy-thread request admission, queueing, notification,
batching, and overload behavior. Thread topology is covered in
[`threading-model-plan.md`](./threading-model-plan.md). Backend memcache client
pipelining is tracked separately in [`mcrouter-comparison.md`](./mcrouter-comparison.md).

References to mcrouter source assume the upstream repo is checked out elsewhere;
paths are noted as `mcrouter/...` without clickable links. Line numbers reflect
the state of the code at the time of writing and may drift.

## TL;DR

| Concern | mcrouter | Naive Tokio queue | Planned rusty model |
|---|---|---|---|
| Capacity | Fixed-capacity `folly::MPMCQueue` | Often unbounded or implicit | Bounded `mpsc::channel(client_queue_size)` |
| Backpressure | Full queue blocks producer | `send().await` waits, `try_send` fails | Prefer explicit fail-fast or reserved-capacity policy |
| Same-thread path | Bypasses queue | Often still queues | Bypass queue when target is current proxy |
| Notification | Deduplicated, relaxed, rate-aware | Tokio wakes receiver normally | Start with Tokio wakeups + batch drain |
| Batching | `blockingWriteNoNotify` + later notify | One message per wake if naive | Drain up to `MAX_BATCH` with `try_recv` |
| In-flight limit | CarbonRouterClient outstanding limit | Not automatic | Separate `Semaphore`, held until reply completion |

The practical target is:

```text
target == current proxy:
  route locally / spawn_local directly

target != current proxy:
  acquire admission capacity
  enqueue into bounded proxy queue
  target proxy drains queue in batches
```

---

## What mcrouter's queue does

mcrouter's proxy queue is not just a channel. It is a bounded queue with explicit
notification control and event-loop integration.

Relevant mcrouter source:

- `mcrouter/lib/MessageQueue.h:134-176` constructs a fixed-capacity
  `folly::MPMCQueue` and a `Notifier`.
- `mcrouter/lib/MessageQueue.h:181-221` attaches the queue to a
  `folly::VirtualEventBase` using an `eventfd`, persistent event handler, and a
  `runBeforeLoop` drain callback.
- `mcrouter/lib/MessageQueue.h:232-234` drains through `Notifier::drainWhileNonEmpty`.
- `mcrouter/lib/MessageQueue.h:247-288` documents and implements blocking writes
  when the queue is full.
- `mcrouter/lib/MessageQueue.h:290-299` exposes relaxed notification.
- `mcrouter/lib/MessageQueue.h:345-350` drains all currently available messages.
- `mcrouter/Proxy-inl.h:200-225` creates the proxy `MessageQueue` with queue
  size, no-notify rate, wait threshold, notification stats, and post-drain hints.
- `mcrouter/Proxy-inl.h:236-241` attaches the queue and fiber manager to the
  proxy event base.
- `mcrouter/CarbonRouterClient-inl.h:292-305` sends remote-thread requests via
  `blockingWriteNoNotify(...)` and then optionally `notifyRelaxed()`.
- `mcrouter/CarbonRouterClient-inl.h:307-312` bypasses the queue for same-thread
  requests.
- `mcrouter/CarbonRouterClient-inl.h:364-367` delays notifications for batched
  multi-request sends.

The key behavior:

```text
producer thread
  │
  │ blockingWriteNoNotify / blockingWriteRelaxed
  ▼
bounded MPMC queue
  │
  │ maybe notify eventfd
  ▼
proxy EventBase wakes
  │
  │ runBeforeLoop drain callback
  ▼
drain all pending messages
  │
  ▼
Proxy::messageReady()
  │
  ▼
FiberManager schedules request work
```

Important detail: mcrouter's same-thread path does **not** pay the queue cost.
It calls `messageReady(...)` directly.

---

## Backpressure semantics

mcrouter has two different controls that should stay separate in rusty-mcrouter.

### 1. Queue capacity

`client_queue_size` bounds how many messages can sit in the proxy queue. When it
is full, mcrouter's `blockingWriteNoNotify` forces a notification and then
blocks until the reader catches up.

Rust equivalent options:

```rust
let (tx, rx) = tokio::sync::mpsc::channel::<ProxyMessage>(client_queue_size);
```

Then choose an overload policy:

```rust
pub enum QueuePolicy {
    Wait,
    FailFast,
}
```

`Wait` applies cooperative backpressure:

```rust
tx.send(msg).await.map_err(|_| QueueClosed)?;
```

`FailFast` rejects immediately:

```rust
tx.try_send(msg).map_err(|_| QueueFull)?;
```

For a network-facing router, `FailFast` is often easier to reason about because
queue saturation becomes an explicit `SERVER_ERROR` / busy response instead of
hidden latency on the client connection.

### 2. In-flight request limit

Queue capacity limits backlog. It does not limit total work already accepted and
waiting on replies. Use a separate semaphore for that.

```rust
pub struct ProxyHandle {
    tx: mpsc::Sender<ProxyMessage>,
    inflight: Arc<Semaphore>,
    policy: QueuePolicy,
}
```

Acquire an owned permit before accepting the request:

```rust
let permit = self
    .inflight
    .clone()
    .try_acquire_owned()
    .map_err(|_| Busy)?;

let msg = ProxyMessage::Request(ProxyRequest {
    request,
    reply_tx,
    _inflight: permit,
});

self.tx.try_send(msg).map_err(|_| QueueFull)?;
```

The permit should live in the request context and be dropped only when the
request completes, is canceled, times out, or is failed.

```text
queue capacity:
  how many accepted messages can wait for the proxy loop

in-flight limit:
  how many requests can be active or waiting for replies
```

---

## Capacity-first admission with permits

If constructing the request context has side effects or allocations we want to
avoid under overload, reserve queue capacity first.

Fail-fast path:

```rust
let permit = tx.try_reserve().map_err(|_| QueueFull)?;

let msg = ProxyMessage::Request(ProxyRequest {
    request,
    reply_tx,
});

permit.send(msg);
```

Cooperative path:

```rust
let permit = tx.reserve().await.map_err(|_| QueueClosed)?;

let msg = ProxyMessage::Request(ProxyRequest {
    request,
    reply_tx,
});

permit.send(msg);
```

Do not hold route references, locks, or scarce resources across
`reserve().await`. Reserve capacity, build the message, send immediately.

---

## Batch draining on the proxy thread

A naive receiver processes one message per wake:

```rust
while let Some(msg) = rx.recv().await {
    handle(msg);
}
```

That misses one of the important mcrouter properties: one notification can drain
many queued messages.

Rusty should batch-drain:

```rust
const MAX_BATCH: usize = 64;

async fn run_proxy_queue(
    mut rx: mpsc::Receiver<ProxyMessage>,
    route: Rc<dyn DynRoute>,
) {
    let mut batch = Vec::with_capacity(MAX_BATCH);

    while let Some(first) = rx.recv().await {
        batch.clear();
        batch.push(first);

        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(msg) => batch.push(msg),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        for msg in batch.drain(..) {
            handle_proxy_message(msg, Rc::clone(&route));
        }

        tokio::task::yield_now().await;
    }
}
```

`yield_now()` is a fairness hint after bounded work, not a correctness or
backpressure mechanism.

---

## Same-thread bypass

The same-thread path should not enqueue. This matches mcrouter's
`sendSameThread()` path.

```rust
enum TargetProxy {
    Local,
    Remote(ProxyHandle),
}

match target {
    TargetProxy::Local => {
        tokio::task::spawn_local(async move {
            let reply = route.route_dyn(req).await;
            let _ = reply_tx.send(reply);
        });
    }

    TargetProxy::Remote(handle) => {
        handle.try_send_request(req, reply_tx)?;
    }
}
```

This keeps the common local case fast while still supporting `FixedRemoteThread`
and `AffinitizedRemoteThread`.

---

## Notification coalescing

Tokio `mpsc` already wakes the receiver when messages arrive. That is enough for
the first implementation.

mcrouter's custom `Notifier` adds more control:

```text
EMPTY / NOTIFIED / READING state
notify only when needed
relaxed notification at high rates
force notification after wait threshold
periodic drain every 2ms when relaxed notifications are enabled
```

Do not implement this first. It is easy to get wrong, and batching on a bounded
Tokio channel gives most of the value.

If benchmarks later show wakeups are a bottleneck, a closer custom queue would
look like:

```rust
struct ProxyQueue {
    queue: Mutex<VecDeque<ProxyMessage>>,
    notify: Notify,
    state: AtomicU8,
    capacity: usize,
}
```

With states:

```rust
const EMPTY: u8 = 0;
const NOTIFIED: u8 = 1;
const DRAINING: u8 = 2;
```

Producer sketch:

```rust
fn try_push(&self, msg: ProxyMessage) -> Result<(), QueueFull> {
    {
        let mut q = self.queue.lock().unwrap();
        if q.len() == self.capacity {
            return Err(QueueFull);
        }
        q.push_back(msg);
    }

    if self
        .state
        .compare_exchange(EMPTY, NOTIFIED, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        self.notify.notify_one();
    }

    Ok(())
}
```

Consumer sketch:

```rust
async fn recv_batch(&self, max: usize, out: &mut Vec<ProxyMessage>) {
    loop {
        self.notify.notified().await;
        self.state.store(DRAINING, Ordering::Release);

        {
            let mut q = self.queue.lock().unwrap();
            while out.len() < max {
                match q.pop_front() {
                    Some(msg) => out.push(msg),
                    None => break,
                }
            }

            if q.is_empty() {
                self.state.store(EMPTY, Ordering::Release);
                return;
            }
        }

        self.state.store(NOTIFIED, Ordering::Release);
        return;
    }
}
```

This should be a later optimization only after measurements justify it.

---

## Recommended implementation order

1. **Bounded proxy queues**
   Add `client_queue_size` and create one bounded `mpsc::Receiver<ProxyMessage>`
   per proxy thread.

2. **Explicit overload policy**
   Start with `try_send` / fail-fast. Map full queue to a clear busy/server-error
   response instead of silently adding latency.

3. **Same-thread bypass**
   If target proxy is the current proxy, schedule route work locally and skip the
   queue.

4. **Batch-drain remote queues**
   Use `recv().await` for the first message, then `try_recv()` up to `MAX_BATCH`.

5. **Separate in-flight semaphore**
   Add a max outstanding request limit independent of queue size.

6. **Benchmark wake behavior**
   Only then decide whether to build a custom `Notify` + atomic-state queue.

The fidelity target is not "use Folly's exact notifier." The target is:

```text
bounded admission
explicit overload behavior
same-thread fast path
batched remote queue draining
separate in-flight limits
```
