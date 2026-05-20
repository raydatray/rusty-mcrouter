# Code review follow-ups

Internal cleanup backlog from a code-quality audit. Scope is **code clarity,
architecture, and consistency** — performance gaps live in
[`mcrouter-comparison.md`](./mcrouter-comparison.md), missing features and
existing `// todo` markers are intentional and excluded.

- **Audited at**: commit `38e9b5d179a4b63e9b920fd5bd71d814d367779b`
  (`[7/7][main] wire up route parsing to main`)
- **Date**: 2026-05-20
- **Method**: oracle deep review + cross-crate consistency audit, results
  synthesized and spot-verified against the working tree

References to source files use repo-relative paths. Line numbers reflect the
audit commit and may drift.

## TL;DR

| Tier | Count | Theme |
|---|---|---|
| 1 — correctness | 3 | Real bugs worth fixing soon |
| 2 — architectural | 5 | Larger cleanups; do when in the area |
| 3 — idiomatic | 6 | Small Rust style fixes |
| 4 — test quality | 4 | Test robustness improvements |
| 5 — quick wins | 7 | Typos and drive-by edits |

Recommended order at the bottom.

---

## Tier 1 — Correctness

### 1. Unchecked `usize` arithmetic on wire-derived sizes

[`parser.rs:114`](../rusty-mcrouter-protocol/src/parser.rs) and
[`parser.rs:287`](../rusty-mcrouter-protocol/src/parser.rs):

```rust
let data_end = data_start + bytes_count;
```

`bytes_count` is parsed directly off the wire via `parse_usize(...)`. A client
or backend sending `set foo 0 0 18446744073709551615\r\n...` panics in debug
and silently wraps in release before the framing check runs. Same shape on
both the request `set` path and the reply `VALUE` path.

```rust
let data_end = data_start
    .checked_add(bytes_count)
    .ok_or(ProtocolError::Malformed("body length overflow"))?;
```

Fix both sites identically.

### 2. Server silently swallows session errors

[`server.rs:36-38`](../rusty-mcrouter-net/src/server.rs):

```rust
tokio::spawn(async move {
    let _ = serve_session(stream, handler).await;
});
```

Protocol errors, malformed clients, and I/O failures all become invisible. At
minimum:

```rust
if let Err(err) = serve_session(stream, handler).await {
    eprintln!("session ended: {err}");
}
```

Same pattern in [`main.rs:47`](../rusty-mcrouter/src/main.rs) — every
`RouteError` is collapsed into one opaque
`Reply::ServerError("backend unavailable")` with no logging. Keep the
client-facing reply generic; log the underlying error before mapping.

### 3. Shorthand routes silently accept malformed config

[`route_builder.rs:89-93`](../rusty-mcrouter-core/src/route_builder.rs):

```rust
"NullRoute" => Ok(NullRoute.into_dyn()),                                // args ignored
"ErrorRoute" => Ok(ErrorRoute::new(args.first().cloned()).into_dyn()), // extras dropped
"PoolRoute" => { if args.len() != 1 { return Err(...) } ... }          // only this validates
```

`"NullRoute|typo"` parses as a bare `NullRoute`. `"ErrorRoute|msg|garbage"`
parses as `ErrorRoute("msg")` with the extra silently discarded. Config typos
become invisible behavior changes. Apply the same arity check `PoolRoute`
already has to the other two shorthand forms.

---

## Tier 2 — Architectural cleanup

### 4. Module visibility / re-exports — pick one pattern

Three patterns currently in use across the workspace:

| Pattern | Crates | Effect on callers |
|---|---|---|
| `pub mod foo;` (no re-exports) | `protocol`, `core` | `rusty_mcrouter_protocol::reply::Reply` |
| `mod foo;` + `pub use crate::foo::Foo;` | `config` | `rusty_mcrouter_config::ConfigDocument` |
| `pub mod foo;` + `pub use foo::Foo;` | `net` | either form works |

The visible symptom in [`core/src/route.rs:3-4`](../rusty-mcrouter-core/src/route.rs):

```rust
use rusty_mcrouter_net::NetError;                              // root (re-exported)
use rusty_mcrouter_protocol::{reply::Reply, request::Request}; // submodule (no re-export)
```

The mixed style is purely an accident of which crate the type lives in. Pick
`mod foo;` + `pub use crate::foo::Foo;` everywhere, then call sites import
from the crate root uniformly.

### 5. PoolRoute concrete/dyn type tension

[`pool_route.rs:11-23`](../rusty-mcrouter-core/src/pool_route.rs):

```rust
pub struct PoolRoute {
    // todo - clients, not destination routes
    children: Vec<Arc<DestinationRoute>>,
}
pub fn new(children: Vec<Arc<DestinationRoute>>) -> Option<Self> { ... }
```

Two issues compounded:

- `children` reads like a generic route tree but is hard-coded to one concrete
  leaf type. Either rename to `destinations` and own the coupling (a pool *is*
  a set of backends), or take `Vec<Arc<dyn DynRoute>>` and become a real
  fan-out combinator.
- `Option<Self>` for "empty children" doesn't tell the caller why. Rename to
  `try_new(...) -> Result<Self, EmptyPool>` and let the caller drop the
  explicit empty-pool error variant from `BuildError`.

Probably leave the rename until a second use site appears.

### 6. Zero doc comments anywhere

Across all five crates: count of `///` is zero. These are libraries; public
API contracts aren't obvious from names. Priority items:

- [`Route` and `DynRoute`](../rusty-mcrouter-core/src/route.rs) — the
  difference between them is genuinely non-obvious
- [`parse_request`/`parse_reply`](../rusty-mcrouter-protocol/src/parser.rs) —
  particularly the buffer-consumption semantics on `Err` (see next item)
- [`Client::send`](../rusty-mcrouter-net/src/client.rs) and
  [`Server::serve`](../rusty-mcrouter-net/src/server.rs)
- Public config types `ConfigDocument`, `RouteHandleConfig` variants
- `BuildError` variant semantics

### 7. Parser buffer semantics on `Err` are inconsistent

Request parser consumes the bad line on error
([`parser.rs:61`](../rusty-mcrouter-protocol/src/parser.rs)):

```rust
let _ = buf.split_to(total);
Err(ProtocolError::Malformed("unknown command"))
```

Reply parser does **not** consume on error
([`parser.rs:258`](../rusty-mcrouter-protocol/src/parser.rs)) — returns `Err`
with the buffer untouched.

Both are defensible; the inconsistency isn't. Callers can't write generic
recovery without knowing which side they're on. Pick one and document it on
the function (`/// On Err, the buffer is consumed up to ...`). "Consumed on
error" is probably the right answer for a stream-framing parser.

### 8. Test helper duplication and inconsistent naming

`req_get` appears in three modules
([`destination_route.rs:36`](../rusty-mcrouter-core/src/destination_route.rs),
[`pool_route.rs:71`](../rusty-mcrouter-core/src/pool_route.rs),
[`route_builder.rs:157`](../rusty-mcrouter-core/src/route_builder.rs)) and
once as `req` ([`client.rs:54`](../rusty-mcrouter-net/src/client.rs)). All
have the same body. Promote to a cfg-test `mod testing` at each crate root
with a single canonical name.

`mock_backend` lives in [`testing.rs`](../rusty-mcrouter-net/src/testing.rs)
(shared) but `mock_backend_chunked` is a private helper duplicated in
`client.rs`. The chunked variant is the more useful primitive — promote it
to the shared module.

---

## Tier 3 — Idiomatic Rust

### 9. `Ok(expr?)` smell

[`destination_route.rs:23`](../rusty-mcrouter-core/src/destination_route.rs):

```rust
Ok(client.send(&req).await?)
```

This is "decode the error, then re-encode it". Cleaner:

```rust
client.send(&req).await.map_err(RouteError::from)
// or, with From in scope:
client.send(&req).await.map_err(Into::into)
```

### 10. Redundant `&self` in the blanket impl

[`route.rs:39`](../rusty-mcrouter-core/src/route.rs):

```rust
Box::pin(<R as Route>::route(&self, req))
//                          ^^ self is already &R; auto-deref handles the &&R → &R
```

One-character fix: drop the `&`.

### 11. `for_each` for side effects

[`request.rs:23`](../rusty-mcrouter-protocol/src/request.rs) and
[`reply.rs:31`](../rusty-mcrouter-protocol/src/reply.rs):

```rust
keys.iter().for_each(|k| { out.put_slice(b" "); out.put_slice(k); });
```

Iterator adapters are for transformations; side-effect loops should look like
loops. Use plain `for`.

### 12. `connect` and `bind` use different generic styles

```rust
// client.rs:16
pub async fn connect(addr: impl ToSocketAddrs) -> ...
// server.rs:18
pub async fn bind<A: ToSocketAddrs>(addr: A) -> ...
```

Semantically identical; visibly different. Pick `impl Trait` for both.

### 13. Mixed imports style

[`route_builder.rs:1`](../rusty-mcrouter-core/src/route_builder.rs) uses
nested:

```rust
use std::{collections::BTreeMap, sync::Arc};
```

[`client.rs:1-4`](../rusty-mcrouter-net/src/client.rs) uses single-line:

```rust
use bytes::BytesMut;
use rusty_mcrouter_protocol::{...};
use tokio::io::{...};
```

Add `imports_granularity = "Crate"` to `rustfmt.toml` and run `cargo fmt`.

### 14. `pub` field that should be `pub(crate)`

[`pool.rs:10`](../rusty-mcrouter-config/src/pool.rs):

```rust
pub extra: Map<String, Value>,
```

If nothing outside the crate reads it, `pub(crate)`. Public fields are an API
commitment.

---

## Tier 4 — Test quality

### 15. Port 1 in connect-failure test

[`route_builder.rs:287`](../rusty-mcrouter-core/src/route_builder.rs) hardcodes
`127.0.0.1:1` as a "guaranteed closed" port. Usually true, not always.
Robust version:

```rust
let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
let dead_addr = listener.local_addr().unwrap();
drop(listener);  // port is closed; subsequent connects refuse
```

There's a tiny race window between drop and the test's connect — has been
~zero in practice for the entire history of the Rust ecosystem doing this.

### 16. `round_trip` integration helper assumes one read = one reply

[`integration.rs:108-115`](../rusty-mcrouter/tests/integration.rs):

```rust
let mut buf = vec![0u8; 256];
let n = conn.read(&mut buf).await.unwrap();
buf.truncate(n);
```

TCP doesn't preserve message boundaries. For small replies on loopback this
almost always reads the whole thing in one syscall — "almost always" produces
flakes you can't reproduce. Read until `END\r\n` or a status terminator.

### 17. Bare `unwrap()` in shared testing helper

[`testing.rs:11-14`](../rusty-mcrouter-net/src/testing.rs):

```rust
let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
let addr = listener.local_addr().unwrap();
```

This is `pub` and used from multiple crates. `expect("bind mock backend")`
gives a self-describing failure instead of
`called \`unwrap()\` on an \`Err\` value: ...`.

### 18. Dead `RUSTY_MCROUTER_BACKEND` env var

[`integration.rs:65`](../rusty-mcrouter/tests/integration.rs) sets it.
[`main.rs`](../rusty-mcrouter/src/main.rs) doesn't read it. Either remove from
the test or wire up the binary if intended.

---

## Tier 5 — Quick wins

| Where | Fix |
|---|---|
| [`config/src/route.rs:36`](../rusty-mcrouter-config/src/route.rs) | typo: `obejct` → `object` |
| [`config/src/pool.rs:8`](../rusty-mcrouter-config/src/pool.rs), [`config/src/route.rs:19`](../rusty-mcrouter-config/src/route.rs) | typo: `dont` → `don't` |
| [`route_builder.rs:38`](../rusty-mcrouter-core/src/route_builder.rs) | trailing space inside `#[error("…is not implemented ")]` |
| [`rusty-mcrouter-core/Cargo.toml`](../rusty-mcrouter-core/Cargo.toml) | `bytes` listed in both `[dependencies]` and `[dev-dependencies]` — drop the dev entry |
| [`integration.rs:37`](../rusty-mcrouter/tests/integration.rs) | `format!("127.0.0.1:{}", port).parse().unwrap()` → `SocketAddr::from(([127, 0, 0, 1], port))` |
| [`parse_fixtures.rs:45`](../rusty-mcrouter-config/tests/parse_fixtures.rs) | `&vec!["foo".to_string()]` → `["foo"]` slice literal |
| [`config/src/document.rs:65, 121, 126`](../rusty-mcrouter-config/src/document.rs) | `std::result::Result<...>` → `Result<...>` to match the rest of the codebase |

---

## Recommended order

1. **Tier 1 entirely** — three small surgical fixes, real bugs, roughly 15
   minutes total.
2. **Module visibility unification (#4)** — pick the `mod` + `pub use` pattern
   and apply across, no behavior change. Roughly 30 minutes.
3. **Shared test helpers (#8)** — promote `mock_backend_chunked`, dedup
   `req_get`/`req` across the core crate. Roughly 20 minutes.
4. **Doc comments on public API (#6)** — start with `Route`/`DynRoute` and
   the parser entry points. Stops the codebase from feeling WIP.
5. The rest as drive-by edits when you're already in the file for other
   reasons.

Don't bother with:

- `PoolRoute` rename (#5) — wait until you have a second use site.
- Parser error-buffer refactor (#7) — pick one semantic and document, don't
  rewrite the parsers.

---

## Explicitly out of scope

The following were considered and intentionally **not** included as
follow-ups:

- Anything from [`mcrouter-comparison.md`](./mcrouter-comparison.md) (request
  pipelining, `write_vectored`, buffer pooling, read buffer shrinking, key
  copy on set parsing).
- Missing features (`named_handles` resolution, prefix routing, additional
  route types). Tracked via `BuildError::*NotImplemented` variants instead.
- Existing `// todo` comments. Author leaves them as intentional reminders.
- Two ergonomic tweaks reviewers suggested that we decided against:
  - Moving `into_dyn`/`arc_into_dyn` off the `Route` trait into free
    functions. Trait is the right home; the conversion is fundamental to the
    trait's role.
  - Relaxing `'static` on `Route`. Every storage site (`Server`,
    `RouteBuilder`, spawned handlers) needs `'static` anyway — moving the
    bound downstream just adds clutter at every use site.
