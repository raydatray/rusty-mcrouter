# Performance comparison: rusty-mcrouter vs mcrouter

Tracks performance and architectural gaps between this codebase and Meta's
[mcrouter](https://github.com/facebook/mcrouter) for the same `set`/`get` ASCII
protocol code paths. Scope is wire-level CPU and memory efficiency on the hot
path. Features rusty-mcrouter doesn't attempt (compression, TLS, the binary
Caret protocol, service discovery) are noted at the end as informational only.

References to mcrouter source assume the upstream repo is checked out elsewhere;
paths are noted as `mcrouter/lib/network/...` without clickable links.

Line numbers reflect the state of the code at the time of writing and may drift.

## TL;DR

| Gap | Cost per request | Fix priority |
|---|---|---|
| No request pipelining per backend | Throughput capped at `1 / RTT` per backend | **high** |
| No `writev`/scatter-gather on send | 2× value-size memcpy per round-trip | medium |
| No buffer pooling | 1 alloc per request + 1 per reply | low |

Plus two minor items: a key copy on `set` parsing (≤250 bytes/req) and a read
buffer that never shrinks (matters only for long-lived connections).

For small values + low concurrency the absolute cost of all these is single-digit
microseconds and noise in the allocator. They become real once values get large
or concurrency-per-backend gets high.

---

## Gap 1: no request pipelining per backend

This is the largest throughput limit and the hardest to fix.

mcrouter's `AsyncMcClientImpl` (see `mcrouter/lib/network/AsyncMcClientImpl.h`)
keeps a single TCP connection with N requests in flight. Replies are matched to
requests by order (ASCII protocol) or by request ID (Caret). Throughput is
bounded by the backend's actual processing capacity, not by RTT.

rusty-mcrouter ([`DestinationRoute`](../rusty-mcrouter-core/src/lib.rs#L18-L35)):

```rust
pub struct DestinationRoute {
    client: Mutex<Client>,
}

impl Route for DestinationRoute {
    async fn route(&self, req: Request) -> Result<Reply, RouteError> {
        let mut client = self.client.lock().await;
        Ok(client.send(&req).await?)   // mutex held for the entire RTT
    }
}
```

The `Mutex<Client>` serializes everyone. With 1ms backend latency, **one
DestinationRoute caps that backend at ~1000 req/sec** regardless of how many
client connections are fanning in. mcrouter saturates the backend.

### Fix path

Two viable architectures:

1. **In-flight queue inside `Client`**: the client holds a `VecDeque<oneshot::Sender<Reply>>`,
   spawns a separate read loop that pops senders FIFO as replies arrive, and the
   `send()` API returns a future that awaits its own sender. Single connection,
   ASCII-style ordered matching. Smallest change; correct for memcached ASCII.
2. **Connection pool inside `DestinationRoute`**: hold N independent `Client`s,
   round-robin or pick-by-availability. Easier to reason about; uses more
   sockets per backend.

Both are nontrivial — the current `Client::send(&mut self, ...)` shape forces
exclusive borrow for the whole call, which is fundamentally what blocks
pipelining. Refactoring to `&self` + interior mutability (or splitting read/write
halves with `tokio::io::split`) is the prerequisite for either fix.

---

## Gap 2: no scatter-gather (`writev`) on the send path

mcrouter (`mcrouter/lib/network/AsciiSerialized.cpp`, `keyValueRequestCommon`):

```cpp
auto value = coalesceAndGetRange(request.value_ref());
auto len = snprintf(printBuffer_, kMaxBufferLength,
                    " %lu %d %zd\r\n",
                    *request.flags_ref(), *request.exptime_ref(), value.size());
addStrings(prefix, request.key_ref()->fullKey(),
           folly::StringPiece(printBuffer_, len),
           value, "\r\n");
```

`addString` stores `(iov_base, iov_len)` into a stack `iovec[8]`. The numeric
header goes into a stack `printBuffer_[80]`. The **value bytes are never
touched** — `iov_base` points directly into the user's `IOBuf`, the `IOBuf` is
held alive by `WriteBuffer::iobuf_` until `writev()` finishes. For a 1MB value,
zero bytes are memcpy'd during serialization.

rusty-mcrouter ([`Request::Set::serialize_into`](../rusty-mcrouter-protocol/src/request.rs#L31-L46)):

```rust
out.put_slice(b"set ");
out.put_slice(key);
// ... write decimals ...
out.put_slice(b"\r\n");
out.put_slice(data);   // memcpy of the entire value into a fresh BytesMut
out.put_slice(b"\r\n");
```

Same problem on the reply side ([`Reply::Get::serialize_into`](../rusty-mcrouter-protocol/src/reply.rs)):
`out.put_slice(&v.data)` for each `VALUE` block.

### Round-trip cost

For `set foo 0 0 N` then `get foo` through the router:

| Stage | rusty-mcrouter copies of value | mcrouter copies |
|---|---|---|
| Server reads request bytes | 1 (kernel→userspace, unavoidable) | 1 (same) |
| Server parses `set` body | 0 (`frozen.slice(...)`) | 0 (`cloneOneInto`) |
| Route forwards → `Client::send` serializes | **1** (`put_slice(data)`) | 0 (`writev`) |
| Backend → router (read STORED reply) | 0 | 0 |
| For `get foo` later: server parses | 0 | 0 |
| Backend returns `VALUE foo ...` | 1 (kernel→userspace, unavoidable) | 1 (same) |
| `Client::send` returns parsed Reply | 0 (slice) | 0 (slice) |
| Server serializes Reply::Get | **1** (`put_slice(&v.data)`) | 0 (`writev`) |

**Two extra value memcpys per round-trip** beyond what's physically necessary.

### Fix path

Tokio supports scatter-gather via [`AsyncWriteExt::write_vectored`](https://docs.rs/tokio/latest/tokio/io/trait.AsyncWriteExt.html#method.write_vectored)
which takes `&[IoSlice<'_>]` (the same primitive as `writev(2)`). The refactor:

1. Change `serialize_into` to populate a `Vec<IoSlice<'_>>` (or fixed-size
   `[IoSlice; N]`) instead of writing into a `BytesMut`.
2. Numeric portions still need a small backing buffer (`[u8; 80]`-ish) that the
   `IoSlice`s borrow from. A `SmallVec` or stack array works.
3. Lifetime constraints: `IoSlice<'_>` borrows the source. Caller must keep the
   `Request`/`Reply` alive across the async write. Awkward but tractable.

This eliminates both gap-2 copies AND removes the per-request `BytesMut`
allocation from gap 3. Two birds.

---

## Gap 3: no buffer / object pooling

mcrouter's `WriteBufferQueue` (`mcrouter/lib/network/WriteBuffer.h`, lines
153-233) keeps a thread-local free stack of up to 50 `WriteBuffer` objects, each
with a pre-allocated `iovec[16]` and `printBuffer_[100]`. After use, `clear()`
lets it be reused without freeing.

rusty-mcrouter allocates a fresh `BytesMut::new()` per request
([client.rs:25](../rusty-mcrouter-net/src/client.rs#L25)) and per reply
([server.rs:58](../rusty-mcrouter-net/src/server.rs#L58)). At 1M req/sec that's
2M allocations/sec — Rust's allocator handles it fine (jemalloc-grade is fast),
but it's pure waste.

### Fix path

Two options, in increasing order of complexity:

1. Per-task buffer reuse: hold a `BytesMut` in `serve_session` and `Client::send`,
   `clear()` it instead of allocating fresh each time. Fixes most of the cost
   for free; doesn't help when `Bytes` is split off (which transfers ownership).
2. Thread-local `Vec<BytesMut>` free stack à la `WriteBufferQueue`. More plumbing
   but matches mcrouter's pattern.

If gap 2 (writev) is fixed first, this gap mostly disappears for the send path
because the `BytesMut` allocation goes away entirely. Only the small numeric
backing buffer remains, and that's stack-allocatable.

---

## Smaller gaps

### Read buffer never shrinks

mcrouter's `McParser` (`mcrouter/lib/network/McParser.h`, lines 112-142) tracks
`minBufferSize_=256` / `maxBufferSize_=4096` and shrinks between requests via a
`lastShrinkCycles_` counter for amortized shrinkage.

rusty-mcrouter's read buffer ([server.rs](../rusty-mcrouter-net/src/server.rs#L11),
[client.rs](../rusty-mcrouter-net/src/client.rs#L8)) starts at `BytesMut::with_capacity(4096)`
and grows naturally via `read_buf`. It never shrinks. A connection that handled
one 10MB value will hold that 10MB allocation for the rest of its lifetime.

Matters only for long-lived connections that see occasional large requests.

### Key copy on set parsing

[`parse_set_header`](../rusty-mcrouter-protocol/src/parser.rs#L148) ends with:

```rust
Ok((Bytes::copy_from_slice(key), flags, exptime, bytes_count))
```

mcrouter (`mcrouter/lib/network/McAsciiParser.rl`, line 89) uses
`appendKeyPiece(buffer, currentKey_, ...)` + `cloneOneInto` to keep the key
zero-copy even across fragment boundaries.

Cost per `set` request: copy of ≤250 bytes. The deliberate trade-off was
simplicity — zero-copy slicing requires manual offset tracking through the
`split` iterator instead of letting it produce `&[u8]` slices. Doable but adds
complexity. See the `parse_set_header` body for the current shape.

### Decimal writing

mcrouter uses `snprintf` into a stack buffer. rusty-mcrouter uses manual
`n.ilog10()`-based digit emission ([`wire.rs`](../rusty-mcrouter-protocol/src/wire.rs)).

Both avoid heap allocations. rusty-mcrouter's is probably marginally faster (no
format string parsing) but it's microbenchmark noise. Not a gap; just a
difference.

### Key validation

mcrouter calls `folly::hasSpaceOrCntrlSymbols(key)` which is SIMD-vectorized on
x86_64. rusty-mcrouter's [`validate_key`](../rusty-mcrouter-protocol/src/parser.rs#L268-L286)
is a plain `iter().any()`. For ≤250-byte keys: ~10ns vs ~50ns. Almost certainly
invisible.

---

## What rusty-mcrouter gets right

- **Read-side framing of value bytes**: identical zero-copy story to mcrouter.
  `frozen.slice(data_start..data_end)` and `cloneOneInto` + `trimStart`/`trimEnd`
  do the same thing — share refcounted backing storage, no memcpy. This is the
  single most important hot path for a router.
- **Type safety**: `Reply::Stored` / `Reply::ClientError(Bytes)` is exhaustively
  checked at compile time. mcrouter uses `carbon::Result` enum codes that can be
  typo'd or fail to be handled (lots of `default: handleUnexpected(...)`
  branches in `AsciiSerialized.cpp`).
- **Memory safety**: buffer accounting can't read past end. Rust slices
  bounds-check. mcrouter relies on Ragel-generated state machines being correct
  (and they are — but that's a much harder property to verify).
- **Backend errors propagate as Reply variants** rather than dropping the
  connection. See [`Reply::ServerError`](../rusty-mcrouter-protocol/src/reply.rs#L8-L14)
  and the rationale comment at that location.

---

## Out of scope (mcrouter features rusty-mcrouter doesn't attempt)

Listed for completeness; not gaps in the comparison sense.

- Compression (Lz4, Lz4Immutable, Zstd codecs)
- TLS via Fizz (TLS 1.3 implementation)
- TCP `MSG_ZEROCOPY` for replies above a size threshold (Linux-only)
- The binary Caret protocol (more efficient than ASCII)
- `folly::IOBuf` chains for gather-write across non-contiguous backing buffers
- Multi-connection pools per backend (independent of gap 1)
- ServiceRouter / SR integration for service discovery
- `JemallocNodumpAllocator` to exclude read buffers from core dumps
- Out-of-order reply matching by request ID (ASCII protocol can't do this anyway;
  Caret can)

---

## Recommended fix order

1. **Pipelining (gap 1)** — biggest throughput multiplier. Needs `Client` to grow
   a request/reply matcher with split read/write halves.
2. **`write_vectored` (gap 2)** — eliminates both per-round-trip value copies
   AND obviates most of gap 3. Refactor `serialize_into` to emit `IoSlice`s.
3. **Buffer pooling (gap 3)** — partly subsumed by #2. If pursued separately,
   start with per-task `BytesMut` reuse before moving to a thread-local free
   stack.
4. **Read buffer shrinking** — minor; only matters for long-lived connections.
   Add a periodic shrink-to-min step in `serve_session` / `Client::send`.

Gaps 1 and 2 are the only ones that materially matter for typical workloads.
The rest is polish.
