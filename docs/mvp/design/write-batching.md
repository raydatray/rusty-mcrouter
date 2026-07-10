# rusty-mcrouter write batching (design + history)

> Status: **Tier 1 implemented (2026-06-03); Tier 2 proposed**
> Mirrors: [`../mcrouter/backend-client.md`](../mcrouter/backend-client.md) — mcrouter's `AsyncMcClientImpl` writer batching (`kMaxBatchSize`)
> Implemented in: `rusty-mcrouter/src/proxy/connection.rs` (client reply path) and `rusty-mcrouter-net/src/client/connection.rs` (backend request path)
> Related: [`./threading-model.md`](./threading-model.md) (the per-request dispatch this sits on top of), [`../architecture/threading-model.md`](../architecture/threading-model.md) (as-built)

How we removed the per-message `write()` syscall bottleneck on both write paths,
why it was the dominant cost, what it bought (≈6–7×), and what's left to match
mcrouter. This doc is both a design record and a performance changelog.

---

## tl;dr

- Both write paths used to issue **one `write()` syscall per message** — one per
  reply on the client side (`flush_ready`), one per request on the backend side
  (`write_one`). At pipeline depth 64 that's 64 syscalls where 1 would do.
- That syscall — not CPU — was the throughput ceiling. We had mis-attributed the
  ~590k/299k plateau to "CPU-bound per-thread cost"; it was syscall-bound.
- **Tier 1 (done):** coalesce all ready messages into one reused buffer and issue
  a **single `write_all`** per batch. ≈**6.4× NullRoute** (590k → ~3.8M rps) and
  ≈**7.3× memcached** (299k → ~2.2M rps), with no protocol changes.
- **Tier 2 (proposed):** replace the coalesce-by-copy with **vectored, zero-copy
  writes** (`IoSlice`/`writev`) so large value payloads aren't memcpy'd into an
  intermediate buffer. This is mcrouter's `pushMessages` model.
- mcrouter's "24 KB" = `kMaxBatchSize = 24576`, the **byte cap** on a backend
  `writev` batch. We currently have **no cap** (we drain the whole queue) — a
  guardrail to add.

---

## the problem (what was slow, and why)

Per the per-request dispatch design ([`./threading-model.md`](./threading-model.md)),
a `Connection` parses pipelined requests, routes each as its own task, and writes
replies back in request order. The write step, before this change, looked like:

```rust
// proxy/connection.rs — BEFORE
async fn flush_ready(&mut self) -> Result<(), NetError> {
    while let Some(reply) = self.pending.remove(&self.next_write) {
        let mut out = BytesMut::new();        // a heap alloc per reply
        reply.serialize_into(&mut out);
        self.writer.write_all(&out).await?;   // a write() syscall per reply
        self.next_write = self.next_write.wrapping_add(1);
        self.in_flight = self.in_flight.saturating_sub(1);
    }
    Ok(())
}
```

The backend client mirrored it — `write_one` did `serialize_into` + `write_all`
per command (`net/src/client/connection.rs`, flagged with a `// todo - writev`).

So a connection pipelining `depth` requests paid `depth` write syscalls per batch
in **both** directions. A `write()` is ~1–3 µs; at depth 64 that's the entire
per-request budget. The stress sweeps plateaued at ~590k (NullRoute) / ~299k
(memcached) regardless of added concurrency — the signature we *misread* as CPU
saturation. The control that exposed the truth: **depth=1 throughput was
identical before and after** this change (one reply per batch ⇒ nothing to
coalesce ⇒ no win), proving the entire gain is syscall reduction.

---

## measured impact

Single 12-core box, all-localhost, `--num-proxies 4`, GET-miss workload, 3 s/run.
Absolute numbers are localhost-inflated and partly bounded by the load generator
at the top end; the **relative** before/after on identical setup is the signal.

**NullRoute (router-only) — connections @ depth=64**

| conns | before | after | speedup |
|---|---|---|---|
| 8 | 584k | 3,481k | 6.0× |
| 32 | 588k | 3,755k | 6.4× |
| 128 | 585k | 3,772k | 6.4× |
| 512 | 587k | 3,692k | 6.3× |

**NullRoute — depth @ conns=32**

| depth | before | after | speedup |
|---|---|---|---|
| 1 | 65.5k | 64.7k | **1.0× (control)** |
| 16 | 463k | 1,006k | 2.2× |
| 64 | 595k | 3,825k | 6.4× |
| 256 | 598k | **5,388k** | 9.0× |

**memcached backend — connections @ depth=64**

| conns | before | after | speedup |
|---|---|---|---|
| 8 | 282k | 886k | 3.1× |
| 32 | 299k | 1,553k | 5.2× |
| 128 | 294k | 2,171k | 7.4× |
| 512 | 292k | 2,124k | 7.3× |

**memcached — depth @ conns=32**: depth 1 → 33k (control, unchanged), 64 →
1,585k (5.4×), 256 → **2,315k** (7.9×).

Headline: **NullRoute ~590k → ~3.8M (6.4×), memcached ~299k → ~2.2M (7.3×).**
Latency improved in step (p50 at conns=8/depth=64: 876 µs → 143 µs NullRoute;
1.81 ms → 559 µs memcached). The memcached saturation knee moved out from ~16 to
~128 connections — the router got ~7× faster, so it now takes far more
concurrency to saturate, and the residual ceiling is the backend round-trip plus
the single backend connection per thread.

---

## Tier 1 — write coalescing (implemented)

One reused write buffer per connection; serialize every ready message into it;
one `write_all`. No allocation per message, one syscall per batch.

### client reply path (`proxy/connection.rs`)

`Connection` gains a reused `write_buf: BytesMut`. `flush_ready` becomes:

```rust
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
```

This alone is **not enough**: the run loop receives completions one at a time
(`completed_rx.recv()` yields one per iteration) and calls `flush_ready` after
each, so for in-order completion it would still write one reply per batch. The
completion arm therefore **drains** every immediately-available completion before
the next flush, so multiple replies are actually ready to coalesce:

```rust
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
```

### backend request path (`net/src/client/connection.rs`)

`write_one` → `write_batch`: serialize the triggering command plus everything
else already queued in the channel into one buffer, one write. Order is preserved
so FIFO reply matching still holds.

```rust
async fn write_batch(&mut self, first: ClientCommand) -> Result<()> {
    self.write_buf.clear();
    first.request.serialize_into(&mut self.write_buf);
    self.pending.push_back(first.reply_tx);
    while let Ok(cmd) = self.rx.try_recv() {
        cmd.request.serialize_into(&mut self.write_buf);
        self.pending.push_back(cmd.reply_tx);
    }
    self.writer.write_all(&self.write_buf).await?;
    Ok(())
}
```

### what Tier 1 does NOT do

- **Still copies.** Every key/value byte is `serialize_into`'d (memcpy) into the
  shared buffer before the write. For large values this copy is pure overhead —
  Tier 2 removes it.
- **No batch cap.** Both paths drain the *entire* ready set / queue into one
  buffer. The channel bounds it (≤1024 commands), but with large values a full
  queue could build a very large transient buffer. mcrouter caps at 24 KB
  (below). This is the main guardrail to add.

---

## Tier 2 — vectored, zero-copy writes (proposed)

Instead of copying every message into one buffer, gather the pieces as
`IoSlice`s and issue one `writev`, leaving big payloads in place. This is exactly
mcrouter's `pushMessages` (gather iovecs → one `writev`).

`Reply` is structured (framing is regenerated, not stored), but its big payloads
(`Value.key`, `Value.data`, error messages) are already zero-copy `Bytes` views
of the read buffer. So the emit step is: **`&'static` framing + payload `Bytes`
(no copy) + small decimals rendered into a reused scratch**, then one vectored
write.

```rust
// codec: append wire form as zero-copy segments
impl AsciiReplyEncoder {
    pub fn encode_segments(
        &self,
        context: &BasicTextEncodeContext,
        reply: &Reply,
        scratch: &mut BytesMut,
        out: &mut Vec<Bytes>,
    ) {
        // Get hit: b"VALUE ", key.clone(), b" ", <flags>, b" ", <len>, b"\r\n",
        //          data.clone() /* <-- the value payload, zero-copy */, b"\r\n"
        // ... End/Stored/Numeric/ServerError similarly
    }
}
```

Flush via a tiny `Buf` over the segment list so tokio handles vectoring +
partial-write resumption:

```rust
// SegList: Vec<Bytes> implementing bytes::Buf, exposing each Bytes as one IoSlice
// (the hyper BufList pattern). Then:
self.writer.write_all_buf(&mut seglist).await?;   // vectored, capped at 64 iovecs
```

### constraints (codebase-specific)

- **`unsafe_code = "forbid"`** (workspace lint) → safe-only. `IoSlice::new`,
  `write_vectored`, `write_all_buf`, a custom `Buf` impl are all safe. Fine.
- **MSRV 1.75** → `IoSlice::advance_slices` (needed for a hand-rolled
  `write_vectored` partial-write loop) is **Rust 1.81+**. Use `write_all_buf`
  (handles partials + vectoring internally, available on 1.75), or bump MSRV.
- **`OwnedWriteHalf::is_write_vectored() == true`** → tokio issues a real
  `writev`, not a silent copy. `write_all_buf` caps at **64 iovecs**.
- Net effect at depth=64 with 1 KB values: instead of ~64 KB memcpy + 1 write,
  the value bytes are referenced in place and only the framing (~25 B/value)
  touches memory.

---

## how this maps to mcrouter

The "24 KB" is mcrouter's **backend write-batch byte cap**,
`kMaxBatchSize = 24576` in `AsyncMcClientImpl.cpp`. `pushMessages()` gathers
pending requests into an iovec array, sums their lengths, and flushes a `writev`
when the batch would exceed 24 KB — with a separate iovec-count cap
`kStackIovecs = 128`, and `WriteFlags::CORK` (→ `MSG_MORE`) between partial
flushes. Request *count* is throttled separately by `getNumToSend()`/`maxInflight`.
(`fibers-stack-size = 24*1024` is an unrelated coincidence.)

| dimension | mcrouter (`AsyncMcClientImpl`) | rusty Tier 1 | rusty Tier 2 |
|---|---|---|---|
| coalesce N msgs → 1 syscall | ✅ | ✅ | ✅ |
| mechanism | `writev` (gather, no copy) | copy into one buffer + `write_all` | `writev` / `IoSlice` (no copy) |
| batch size cap | **24 KB** (`kMaxBatchSize`) + 128 iovecs | **none** (drains queue) | 24 KB + 64 iovecs (planned) |
| small-packet control | `CORK` / `MSG_MORE` between flushes | none | optional |
| in-flight throttle | `maxInflight` (count) | none | none |

Citations: `facebook/mcrouter` `mcrouter/lib/network/AsyncMcClientImpl.cpp`
(`kMaxBatchSize`/`kStackIovecs` ~L31–34, `pushMessages` ~L229–315,
`getNumToSend` ~L200–223). folly `AsyncSocket` itself has **no** 24 KB threshold
(passes buffers through; `kSmallIoVecSize = 64` stack array); `IOBufQueue`'s
`kMaxPackCopy = 4096` governs chain packing, not socket batching.

Takeaway: Tier 1 already captured the **dominant** win mcrouter gets from
batching (our measured 6–7× is precisely the per-message syscall cost mcrouter
avoids). What's left to fully match it is the **24 KB cap** and the **`writev`
gather** (Tier 2).

---

## follow-ups

1. **Add a ~24 KB batch cap** to `write_batch` and `flush_ready` (stop draining
   once `write_buf.len() >= 24 * 1024`, leave the rest for the next iteration).
   Matches `kMaxBatchSize`, bounds the currently-unbounded drain. Low-risk; do
   this regardless of Tier 2.
2. **Tier 2 `writev`/`IoSlice`** — removes the residual serialize-copy for large
   payloads. Marginal on small values (our GET-miss benchmark won't show it);
   significant for large-value workloads.
3. **`maxInflight`** — a per-connection in-flight cap (count). Separate from this
   work but the same family; the request-path in-flight is currently unbounded
   (see the connection backpressure notes).
4. **`CORK`/`MSG_MORE`** between partial flushes if/when we cap batches — avoids
   dribbling small segments. Minor.

---

## benchmark methodology (for reproducibility)

- Harness: `rusty-mcrouter/examples/load.rs` — raw memcached-protocol load
  generator. Opens `--conns` TCP connections, keeps `--depth` pipelined
  `get <missing-key>\r\n` in flight per connection, counts `END\r\n` terminators.
  GET-miss ⇒ every reply is exactly `END\r\n` (clean framing, exercises the full
  router path). Reports throughput + batch-RTT percentiles.
- Router: `--num-proxies 4`; configs are a `NullRoute` and a `PoolRoute|local`
  pointing at a local `memcached:1.6` container.
- Caveats: localhost contention (load gen + router + memcached share 12 cores),
  GET-miss only (cheapest op), single backend server, `p50` is batch-RTT/depth
  (derived, not true per-request). Treat the **relative** speedups as the result,
  not the absolute rps.

---

## history

- **2026-06-03** — Tier 1 landed: reply-path coalescing (`flush_ready` +
  completion drain) and backend-path coalescing (`write_batch`). Measured
  ≈6.4× (NullRoute) / ≈7.3× (memcached) on the localhost stress sweep. Revealed
  the prior ~590k/299k plateau was syscall-bound, not CPU-bound. Tier 2 (vectored
  zero-copy) and the 24 KB cap deferred.
