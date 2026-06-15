# rusty-mcrouter warm-up route (design)

> Status: **Proposed (2026-06-12)**
> Mirrors: [`../mcrouter/warm-up-route.md`](../mcrouter/warm-up-route.md) — how mcrouter does it
> Implemented in: `../architecture/warm-up-route.md` (once built; **nothing exists yet** — see [`../architecture/overview.md`](../architecture/overview.md))
> Related: [`../mcrouter/backend-client.md`](../mcrouter/backend-client.md) + [`./hash-routing.md`](./hash-routing.md) (the route tree + pool selection a warm-up route composes), [`./threading-model.md`](./threading-model.md) (the per-proxy `LocalSet` the async set-back rides), [`./async-delete-log.md`](./async-delete-log.md) (the other "route triggers a side-effect write" feature)

How we port mcrouter's `WarmUpRoute` so a new/small **cold** pool can be filled
from a large/existing **warm** pool by live read traffic — letting you resize or
replace a pool without a miss-rate spike on the backing store. Read the
[mcrouter reference](../mcrouter/warm-up-route.md) first; this doc assumes it and
only describes our side.

---

## the two questions up front

**"How much of mcrouter's WarmUpRoute can we even build today?"** The *read-through
fill* core — yes. But mcrouter's `WarmUpRoute` special-cases six request types
(`get`, `gets`, `lease-get`, `metaget`, `gat`, `gats`), and **rusty's protocol
only has `get`** of those: the parser recognizes exactly 10 verbs and **lease-get,
gets/CAS, metaget, gat/gats do not exist** (`rusty-mcrouter-protocol/src/parser/mod.rs:35-50`).
So the portable feature is precisely the `get` path: *cold get → on miss, warm
get → on warm hit, `add` into cold with a reduced TTL.* The other five overloads
are **N/A until those ops exist** — and that's most of mcrouter's file. Good news:
rusty already has `Request::Add` (`request.rs:29-34`), so the faithful
**add-not-set** write-back is available.

**"What's genuinely new here?"** Three things rusty has never done, all small but
real (confirmed greenfield — a `warm|cold|migrate` search is empty):

1. **A route with two *distinct* child subtrees.** Every composite route today is
   homogeneous: `SelectionRoute` holds `Vec<Rc<dyn DynRoute>>` and picks *one*
   (`selection_route.rs:9-37`); `PoolRoute` wraps one `SelectionRoute`
   (`pool_route.rs:11-14`). A `warm` child + a `cold` child as separate fields is
   a first.
2. **A route that issues a *conditional second* downstream call.** Every route
   today makes at most one downstream call; the only multi-call code
   (`submit_multiget`, `proxy/connection.rs:148-179`) is *parallel + independent*
   and lives in the connection layer, not a route. "Call cold, inspect the reply,
   then maybe call warm, then maybe call cold again" is net-new control flow —
   though mechanically trivial since `route()` is a plain `async fn`.
3. **A fire-and-forget set-back** — and this one has a real dependency cost (§3).

---

## goal

Implement a `WarmUpRoute` (config `routeName` analogue `"warm-up"`) that, for a
single-key `get`:

1. routes to **cold** first; on a cold hit, returns it;
2. on a cold miss, routes to **warm**; returns the warm reply (hit *or* miss,
   like mcrouter);
3. on a warm **hit**, populates cold with an **`add`** (not `set`) carrying the
   warm value's `flags`/`data` and a **reduced TTL from config**, ideally
   **without blocking the client reply**;

…composing two arbitrary child route subtrees from config, and leaving all
non-`get` ops as **cold-only** pass-through.

## scope / non-goals

In scope:

- the `WarmUpRoute` two-child route + the `get` warm-fill algorithm;
- the **`add`** write-back with a config TTL;
- the config: a typed `WarmUpRoute { cold, warm, exptime }` variant + a recursive
  builder arm (rusty's first route that builds child *subtrees* from nested
  config);
- the **async-vs-inline set-back decision** (§3) — the one real fork.

Out of scope / deferred (with reason):

- **`gets`/`lease-get`/`metaget`/`gat`/`gats` warm-up paths** — those request
  types don't exist in rusty yet (`parser/mod.rs`). When they land, port the
  matching mcrouter overload (the lease hot-miss short-circuit, the gets/gat/gats
  *sync-add-then-re-read-cold-for-CAS* dance, metaget read-through). Tracked as
  follow-ups, not built now.
- **metaget-derived TTL.** mcrouter falls back to fetching warm's remaining TTL
  via `metaget` when `exptime` is unset. No metaget in rusty → **config `exptime`
  is the only TTL source** (default `0` = no expiry, matching mcrouter's OSS
  default). Revisit if/when metaget exists.
- **`SlowWarmUpRoute`** (per-destination hit-rate failover) and **`StagingRoute`**
  (warm-authoritative mirror) — separate routes, separate designs; both also
  depend on machinery rusty lacks (per-destination hit-rate stats; lease/metaget).
- **stats counters.** rusty has no metrics subsystem (see
  [`./observability.md`](./observability.md)); like mcrouter's WarmUpRoute we add
  none of our own and observe via the warm/cold pool destinations. A `tracing`
  event on each fill is the cheap interim.
- **`MigrateRoute`** (the timed-migration route WarmUpRoute often nests in) — its
  own feature.

---

## starting point (current rusty)

Greenfield for warm-up, but the substrate is ready and the gaps are precise (full
as-built detail belongs in `../architecture/warm-up-route.md`; summarized here to
frame the change):

**Ready to build on:**

- **The trait seam.** `Route::route(&self, req) -> impl Future<Output =
  Result<Reply>>` and object-safe `DynRoute` (`routes/mod.rs:29-57`); children are
  `Rc<dyn DynRoute>`. A `WarmUpRoute` is "just another `Route`" whose body awaits
  its children — sequential/conditional calls need **no trait change**.
- **The ops + fields warm-up needs already exist.** `Request::Get { key }` and
  `Request::Add { key, flags: u32, exptime: i32, data }` (`request.rs:17-34`);
  `Reply::Get { hits: Vec<Value> }` with **miss = empty `hits`**
  (`reply.rs:14`, test `reply.rs:91-94`); `Value { key, flags: u32, data }`
  (`reply.rs:5-10`). So the warm hit gives us `flags`+`data`, and `add` carries
  them plus the config `exptime` (`i32`, same type the wire serializer wants).
- **Detached-task idiom.** `Proxy::spawn_request` (`proxy/proxy.rs:28-36`) is the
  exact pattern an async set-back copies: `Rc::clone` a child into `spawn_local`
  on the per-thread `LocalSet` (`proxy/thread.rs:15-23`); `spawn_local` needs
  `'static` but **not** `Send`, so `!Send` `Rc` children are fine.
- **Config tolerates the JSON already.** An unknown `{"type":"WarmUpRoute",...}`
  round-trips into `RouteHandleConfig::Unknown` with nested children intact
  (`config/src/route.rs`, test at `:316-329`); `build_handle` is an
  `async fn(&mut self, ...)` so it's already recursion-capable.

**The four gaps (explicit build items):**

- **No two-distinct-child route.** `WarmUpRoute` is the first to hold a `warm` and
  a `cold` field. (Trivial: two `Rc<dyn DynRoute>` fields.)
- **No conditional second call.** New control flow inside `route()` — see §2.
- **The set-back can't spawn from `core` today.** `rusty-mcrouter-core`'s tokio
  enables only `features = ["sync"]` (`rusty-mcrouter-core/Cargo.toml:17`);
  `spawn_local`/`LocalSet` need the **`rt`** feature, which core lacks (all
  `spawn_local` calls today live in the binary crate). This is the key fork (§3).
- **No builder arm recurses into child route configs.** `build_handle` *can*
  recurse but no arm does — `PoolRoute` only resolves a pool *name* against
  `config.pools` (`route_builder.rs:78-81`). WarmUpRoute is the first to build
  child *subtrees* (`build_handle(warm)` + `build_handle(cold)`).

One simplifier: the connection layer already splits a multi-key `get` into
single-key requests (`Parsed::MultiGet` fan-out, `connection.rs:148-179`), so
**`WarmUpRoute` only ever sees a single-key `Request::Get`** — no multi-key
warm-up complexity.

---

## target design

A `WarmUpRoute` holding two child handles + a TTL, mapped straight onto the
[reference](../mcrouter/warm-up-route.md):

```rust
pub struct WarmUpRoute {
    warm: Rc<dyn DynRoute>,
    cold: Rc<dyn DynRoute>,
    exptime: i32,           // write-back TTL secs; 0 = no expiry (mcrouter OSS default)
}

#[inline]
fn is_get_hit(r: &Result<Reply>) -> bool {
    matches!(r, Ok(Reply::Get { hits }) if !hits.is_empty())
}
```

### 1. the `get` algorithm (the whole portable feature)

```rust
impl Route for WarmUpRoute {
    async fn route(&self, req: Request) -> Result<Reply> {
        // only `get` warms; everything else is cold-only (client owns warm consistency)
        let Request::Get { key } = req else {
            return self.cold.route_dyn(req).await;
        };

        // 1. cold first
        let cold = self.cold.route_dyn(Request::Get { key: key.clone() }).await;
        if is_get_hit(&cold) {
            return cold;                                   // cold hit: done
        }

        // 2. cold miss/err -> warm (plain get)
        let warm = self.warm.route_dyn(Request::Get { key: key.clone() }).await?;

        // 3. warm hit -> populate cold with `add`, reduced TTL from config
        if let Reply::Get { hits } = &warm {
            if let Some(v) = hits.first() {
                let add = Request::Add {
                    key,                                   // moved; warm path is done with it
                    flags: v.flags,
                    exptime: self.exptime,
                    data: v.data.clone(),
                };
                self.write_back(add);                      // async or inline — see §3
            }
        }
        Ok(warm)                                           // return warm reply (hit or miss)
    }
}
```

This is faithful to mcrouter's `get`: cold-first, **add** (not set) on warm hit,
return the warm reply on a cold miss regardless of warm hit/miss. The `is_get_hit`
predicate is our `isHitResult` analogue; a cold `Err(RouteError::Backend)` (cold
backend down) is *not* a hit, so it correctly falls through to warm.

### 2. non-`get` ops: cold-only pass-through

The `let Request::Get { key } = req else { return self.cold.route_dyn(req).await }`
line is the whole story — `set`/`delete`/`add`/`incr`/`decr`/`replace`/`append`/
`prepend`/`touch` all go to cold only, warm untouched, exactly as mcrouter's
generic template overload does ("client is responsible for warm consistency").

### 3. the set-back fork (the one real decision)

mcrouter does the cold write-back **fire-and-forget** (`folly::fibers::addTask`)
and returns the warm value immediately. Two ways to honor that in rusty:

**Option A — async, faithful (recommended).** `spawn_local` a detached task that
owns a clone of the cold child:

```rust
fn write_back(&self, add: Request) {
    let cold = Rc::clone(&self.cold);
    tokio::task::spawn_local(async move {
        let _ = cold.route_dyn(add).await;   // best-effort; a failed fill just re-warms next get
    });
}
```

- **Cost:** add the `rt` feature to `rusty-mcrouter-core`'s tokio dep
  (`Cargo.toml:17`: `features = ["sync", "rt"]`). It's the first non-test
  `spawn_local` in `core`, but runtime-safe: a `WarmUpRoute` is always invoked
  from a task already on a proxy's `LocalSet` (`submit_single`/`spawn_request`),
  so the spawn context exists.
- **Why recommended:** matches mcrouter's latency profile (client isn't blocked on
  the cold `add`) and the conformance tests (`test_warmup.py` asserts the
  write-back is async).

**Option B — inline, no dep change.** `self.cold.route_dyn(add).await` before
returning `Ok(warm)`. Simplest, zero Cargo change, but it **blocks the client
reply on the cold `add`** — extra round-trip of latency and a divergence from
mcrouter's async behavior. Acceptable as a first-cut MVP; not the end state.

**Recommendation:** ship Option A. If we want a no-dep MVP first, land B behind
the same `write_back` seam and flip to A when we add `rt` (the call sites don't
change).

### 4. config + builder wiring

Add a typed variant (children are full route handles, parsed recursively by the
existing custom `Deserialize` on `RouteHandleConfig`):

```rust
// rusty-mcrouter-config/src/route.rs — RouteHandleConfig
WarmUpRoute {
    cold: Box<RouteHandleConfig>,
    warm: Box<RouteHandleConfig>,
    exptime: i32,                 // optional in JSON; default 0
},
```

```jsonc
{ "type": "WarmUpRoute", "cold": "PoolRoute|A-cold", "warm": "PoolRoute|A-warm", "exptime": 3600 }
```

Parse it in `parse_object_form` (`route.rs:74-112`); build it in `build_handle`
(`route_builder.rs:70-108`) — the first arm to recurse into child configs:

```rust
RouteHandleConfig::WarmUpRoute { cold, warm, exptime } => {
    let cold = self.build_handle(cold).await?;
    let warm = self.build_handle(warm).await?;
    Ok(Rc::new(WarmUpRoute { warm, cold, exptime: *exptime }).into_dyn())
}
```

`exptime` is optional in JSON and defaults to `0` (no expiry) — matching
mcrouter's OSS behavior when `exptime` is omitted and metaget is unavailable
(which is exactly rusty's situation: no metaget).

---

## how this maps to mcrouter

| mcrouter | rusty |
|---|---|
| `WarmUpRoute<RouteHandleIf>`, `routeName "warm-up"` | `WarmUpRoute: Route` holding `warm`/`cold` `Rc<dyn DynRoute>` |
| `warm_` / `cold_` children + `folly::Optional<uint32_t> exptime_` | `warm` / `cold` + `exptime: i32` |
| `get`: cold → (miss) warm → **async** `add`, return warm | §1 + Option A `write_back` |
| write-back is **`add`** (not set) | `Request::Add { flags, exptime, data }` |
| `isHitResult` (cold hit short-circuit) | `is_get_hit` = `Reply::Get { hits }` non-empty |
| returns warm reply on cold miss (hit or miss) | `Ok(warm)` |
| set/delete/incr/... → cold only | `let Request::Get .. else { cold.route_dyn(req) }` |
| `folly::fibers::addTask` (fire-and-forget) | `spawn_local` on the proxy `LocalSet` (needs core `rt` feature) |
| children built recursively by `factory.create` | `build_handle(cold/warm)` recursion (new arm) |
| `cold`/`warm` required, `exptime` optional, OSS default 0 | same; `exptime: i32` default 0 |
| `gets` sync-add + re-read cold for CAS | **deferred** — no `gets`/CAS op in rusty |
| `lease-get` hot-miss short-circuit + async `lease-set` | **deferred** — no lease ops |
| `metaget` read-through; metaget-derived TTL | **deferred** — no metaget; TTL is config-only |
| `gat`/`gats` (TTL from request) | **deferred** — no gat/gats |
| no stats | no stats (optional `tracing` fill event) |
| `SlowWarmUpRoute` / `StagingRoute` | out of scope (separate designs) |

---

## the warm-up `get` lifecycle (target, Option A)

```mermaid
sequenceDiagram
  participant C as client
  participant W as WarmUpRoute
  participant CO as cold child
  participant WA as warm child
  participant BG as spawn_local task

  C->>W: route(Get key)
  W->>CO: route_dyn(Get key)
  alt cold hit (hits non-empty)
    CO-->>W: Reply::Get hit
    W-->>C: cold reply (done)
  else cold miss or error
    CO-->>W: miss / Err
    W->>WA: route_dyn(Get key)
    WA-->>W: warm reply
    opt warm hit
      W->>BG: spawn_local(add to cold, exptime=config)
      Note over BG,CO: detached: cold.route_dyn(Add); client not blocked
      BG->>CO: route_dyn(Add key flags data exptime)
    end
    W-->>C: warm reply (hit or miss)
  end
```

---

## implementation order

1. **Config variant + recursive build arm.** Add `RouteHandleConfig::WarmUpRoute`
   (parse `cold`/`warm`/optional `exptime`) and the `build_handle` arm that builds
   both children. Unit-test parse (including nested handles) + build.
2. **`WarmUpRoute` with inline set-back (Option B).** Implement §1/§2 with an
   awaited `add` (no Cargo change). Test: cold-miss + warm-hit returns the warm
   value *and* the cold side observed an `add` with the config `exptime`
   (port mcrouter's `{"get","add"}` / exptime invariant from `WarmUpRouteTest.cpp`);
   cold-hit short-circuits warm; non-`get` ops hit cold only.
3. **Flip to async set-back (Option A).** Add `rt` to `rusty-mcrouter-core`'s
   tokio; move the `add` into `spawn_local` behind the `write_back` seam. Test the
   client reply returns before the cold `add` completes (a slow-cold mock + timing,
   the `test_warmup.py` analogue).
4. **`tracing` fill event** (optional, cheap): `debug!(target: "warmup", ...)` on
   each warm-fill, pending the observability layer.
5. **Later / dependent.** Port the `gets`/`lease-get`/`metaget`/`gat`/`gats`
   overloads *as those request types are added to the protocol*; revisit
   metaget-derived TTL then.

---

## open questions / decisions

- **Async vs inline set-back? (leaning A)** Option A (spawn_local + core `rt`
  feature) is faithful and keeps the client off the cold-`add` latency; Option B
  is a zero-dependency MVP that blocks the reply. Decide before step 3 — but the
  `write_back` seam means either is a one-line swap.
- **TTL source.** With no metaget, config `exptime` is the *only* TTL. Confirm
  default `0` (= no expiry, mcrouter-OSS-faithful) is what we want, vs. requiring
  an explicit `exptime`. (Recommend default 0 for parity.)
- **`add` vs `set` write-back.** mcrouter uses `add` so a concurrent `set` into
  cold isn't clobbered. We have `Request::Add` — keep `add`. (Only reconsider if
  we ever lack `add`.)
- **Warm error handling.** mcrouter returns the warm reply even on a warm miss; on
  a warm *error* our `?` propagates `Err` (which collapses to `ServerError`
  upstream). Acceptable, or should a warm error fall back to returning the cold
  miss? (Recommend: match mcrouter — return warm verbatim; a cold-miss+warm-down
  is a genuine error.)
- **Should non-`get` reads with no rusty equivalent be rejected or pass through?**
  They pass through to cold today (correct). Just confirm we don't silently warm
  something we shouldn't when `gets`/lease land — wire each explicitly.
- **Eager-connect interaction.** `build_handle` eagerly connects pool backends
  (`route_builder.rs:135-136`, a known `// todo` to make lazy). A WarmUpRoute
  doubles the pools built at config time (warm + cold). Confirm that's acceptable
  until lazy-connect lands.

---

## done when

- A `{"type":"WarmUpRoute","cold":...,"warm":...,"exptime":N}` config parses
  (children recursively built) and `build_route` produces a working two-child
  route — with tests for parse + build (the first recursive-child-build arm).
- A single-key `get` that misses cold and hits warm returns the **warm** value and
  writes the value into **cold via `add`** with the configured `exptime` — a test
  ports `WarmUpRouteTest.cpp`'s observed `{"get","add"}` + exptime assertion.
- A cold **hit** never touches warm; a warm **miss** returns the warm reply; every
  non-`get` op routes to **cold only** (warm mock sees nothing).
- The set-back does not corrupt the client reply: with Option A, a slow cold `add`
  does **not** delay the client's warm reply (timing test); the detached task is
  `spawn_local` on the proxy `LocalSet` and its failure is swallowed (best-effort).
- `lsp_diagnostics` / `clippy` clean; the async-vs-inline decision and the
  deferred lease/gets/metaget/gat overloads are recorded in
  `../architecture/warm-up-route.md` once built.
