# rusty-mcrouter selection routing (design)

> Status: **Implemented** (2026-06-10)
> Mirrors: [`../mcrouter/hash-routing.md`](../mcrouter/hash-routing.md) — how mcrouter does it (`SelectionRoute<HashSelector<Func>>`, `Ch3`/`furc_hash`)
> Implemented in: [`../architecture/hash-routing.md`](../architecture/hash-routing.md) — as-built (incl. where it diverged)
> Related: [`./multiget.md`](./multiget.md) — **independent** of this (neither blocks the other): multigets are split into single-key gets at the request layer *before* routing, so a selection route normally hashes one key; until that lands, routing hashes a get's first key as an interim. And [`./threading-model.md`](./threading-model.md) — thread *affinity* hashing is a **different** hash (which proxy thread) and is orthogonal to this (which backend). Don't conflate them.

Replace `PoolRoute`'s random backend selection with a **pluggable selection
framework**: a `SelectionRoute` handle that asks a `Selector` for a child index,
with the default selector being a deterministic **consistent hash** (`Ch3`) so a
given key always lands on the same backend. The framework is designed up front to
host *several* selection strategies — we know more are coming (Rendezvous,
ConstShard, weighted variants) plus the load-aware route strategies
(Latest, LoadBalancer) — so the abstraction, not just the first hash, is the
deliverable. Read the [mcrouter reference](../mcrouter/hash-routing.md) first —
this doc assumes it and only describes our side.

---

## tl;dr

- Today `PoolRoute::route` picks a backend with `random_range(..)`
  (`rusty-mcrouter-core/src/pool_route.rs`). That makes the pool **not a cache** —
  a key has only a `1/N` chance of hitting the server holding its value, and
  writes scatter. This is the one routing bug that makes the rest of the router
  pointless as a cache layer.
- Build the **mcrouter layering, in idiomatic Rust**: a generic `SelectionRoute`
  handle holding `children + Box<dyn Selector>`, where a `Selector` maps a routing
  key to a child index. This is `SelectionRoute<HashSelector<Func>>` collapsed to
  one handle + one trait object (we don't need C++'s template nesting; runtime
  dispatch is free against a network round-trip).
- **Two tiers, decided up front** (this is the load-bearing design choice):
  - **Stateless index selectors** — pure functions of `(routing key, static pool
    config)`: `Ch3` (default), `Crc32`, and later `WeightedCh3`, `Rendezvous`,
    `ConstShard`. These implement `Selector` and share `SelectionRoute`.
  - **Stateful route strategies** — `Latest`, `LoadBalancer`: they pick by *live
    backend state* and may retry, so they are **their own `Route` handles**, not
    selectors. They occupy the same `hash_func` config slot but dispatch to a
    different builder. (mcrouter draws exactly this line — they're "route
    behaviors, not hash algorithms.")
- First two selectors ship now: **`Ch3`** (a safe-Rust port of mcrouter's
  `furc_hash`, the default) and **`Crc32`** (fast, non-consistent, for parity).
  Both are pure functions of `(key, pool_size)` and need **no new dependencies**.
  **`salt`** is a `Salted` decorator that wraps any byte-hashing selector.
- Make it **config-driven** via a `hash` field on `PoolRoute`
  (`{"type":"PoolRoute","pool":"A","hash":"Ch3"}` or
  `{"hash":{"hash_func":"Ch3","salt":"..."}}`), defaulting to `Ch3` when omitted —
  matching mcrouter. The dispatch is two-tier: selector-backed funcs build a
  `SelectionRoute`; strategy-backed funcs build their own handle.
- **Multi-key `get` is handled upstream, not here** — and **independently**. A
  `get k1 k2 k3` is split into single-key gets at the request layer *before*
  routing (see [`./multiget.md`](./multiget.md)); once that lands a selection
  route only ever sees a one-key get. The two features don't block each other:
  until the split lands, routing hashes a get's first key as an interim and never
  fans out. No fan-out lives in any route handle.
- Verify byte-for-byte against real mcrouter by generating `furc_hash` vectors
  from the pinned source and asserting our port reproduces them — the same way
  the protocol layer tests against mcrouter wire fixtures.

---

## goal

A key deterministically selects one backend in a pool, via a **selection strategy
chosen per-route from config**. The default strategy, `Ch3`, is consistent:
adding or removing one server in a pool of `N` re-homes only ~`1/N` of keys
(mcrouter's guarantee), instead of reshuffling everything, and is wire-compatible
with mcrouter's `Ch3`/`furc_hash` so a rusty pool and a real-mcrouter pool over
the same server list agree on placement. Crucially, swapping in a *different*
strategy later (a different consistency profile, weights, or a load-aware
behavior) must be **additive** — a new `Selector` impl or a new `Route` handle,
not a rewrite of the routing path.

## scope / non-goals

In scope:

- a `Selector` trait + `SelectionRoute` handle (the generic framework)
- `Ch3` (furc) and `Crc32` selectors
- `salt` support as a `Salted` decorator
- parsing `hash` / `hash_func` on `PoolRoute` (string and object forms), default `Ch3`
- routing-key extraction from a (post-split, single-key) `Request`
- two-tier builder dispatch wired into `route_builder`, with the **seam** for
  future selector-backed funcs *and* strategy-backed handles explicitly in place
- fixing the pool cache so two routes can reference one pool with different selectors

Out of scope here (tracked elsewhere or deferred) — but the framework leaves a
**named seam** for each:

- **multi-key `get` splitting** — done at the request/connection layer *before*
  routing; see [`./multiget.md`](./multiget.md). A selection route relies on the
  post-split invariant (one key per get) and never fans out.
- **thread-affinity hashing** (`AffinitizedRemote` in
  [`./threading-model.md`](./threading-model.md)) — that hashes to pick a *proxy
  thread*, not a *backend*. Orthogonal; different call site; do not share code
  prematurely.
- **`WeightedCh3`** weights — a future `Selector` variant carrying per-server
  weights from config; the trait and builder already accommodate it (see
  [the future seam](#11-the-future-seam-how-new-strategies-slot-in)).
- **`Rendezvous` / `WeightedRendezvous`** — future `Selector`s; note they are
  *order-independent* (see [server order](#server-order-is-the-index-space-and-when-it-isnt)).
- **`ConstShard`** — a future `Selector` that parses an explicit shard id from
  the key and maps it to an index.
- **`Latest` / `LoadBalancer`** — the **stateful tier**: future *`Route`
  handles*, not selectors, because they pick by live backend state and retry.
  Deferred, but the two-tier split is built now so they have a home.
- **routing-prefix stripping** (`/region/cluster/`) — rusty has no prefix routing
  yet (`build_route` returns `PrefixRoutingNotImplemented`), so there's no prefix
  to strip. The `|#|` hash-stop is cheap and is included; prefix handling waits
  for prefix routing.

---

## starting point (current rusty)

`PoolRoute` holds its destinations and picks one at random
(`rusty-mcrouter-core/src/pool_route.rs`):

```rust
pub struct PoolRoute {
    // todo - clients, not destination routes
    children: Vec<Rc<DestinationRoute>>,
}

impl PoolRoute {
    pub fn new(children: Vec<Rc<DestinationRoute>>) -> Option<Self> {
        if children.is_empty() { return None; }
        Some(Self { children })
    }
}

impl Route for PoolRoute {
    async fn route(&self, req: Request) -> Result<Reply> {
        // todo - hash, this is a random func
        self.children[random_range(0..self.children.len())].route(req).await
    }
}
```

Relevant surrounding facts:

- The `Route` trait (`rusty-mcrouter-core/src/route.rs`) is
  `fn route(&self, req: Request) -> impl Future<Output = Result<Reply>>` — note
  **`&self`** (shared, not `&mut`). A handle that wants to mutate state per
  request (a load-aware strategy) must use interior mutability. This is precisely
  why the stateful tier lives at the `Route` level, not inside a pure `Selector`.
- `route_builder.rs` (`get_or_build_pool`) builds `Vec<Rc<DestinationRoute>>` in
  **pool-server order** and calls `PoolRoute::new(destinations)`. That order is
  the hash index space — see [server order matters](#server-order-is-the-index-space-and-when-it-isnt).
- **The pool cache caches the whole handle by name.**
  `pool_cache: BTreeMap<String, Rc<PoolRoute>>` and the test
  `pool_referenced_twice_is_built_once_and_shared` asserts `Rc::ptr_eq`. So if one
  pool is referenced by two routes with *different* selectors, today's structure
  would force them to share one handle — a real collision we must fix (see
  [wiring](#9-wiring-route_builder--selectionroute)).
- Config: `RouteHandleConfig::PoolRoute { pool: String }`
  (`rusty-mcrouter-config/src/route.rs`) carries **only** the pool name. The
  object-form parser currently **drops** any sibling fields (the test
  `object_form_pool_route_silently_drops_extras` shows `asynclog` discarded) — so
  a `hash` key is silently ignored today. The shorthand `"PoolRoute|P"` has no
  place for a hash.
- `PoolConfig { servers: Vec<String>, extra: Map }`
  (`rusty-mcrouter-config/src/pool.rs`) — **`servers` is the only source of
  server identity.** `DestinationRoute::new(client)` holds a connected `Client`,
  not a hostname, so identity-based selectors (Rendezvous) must be fed
  `servers`, not the destinations. mcrouter reads `hash` from the **route**, not
  the pool, so we follow that and ignore a pool-level `hash` in `extra`.
- `Request` keys (`rusty-mcrouter-protocol/src/request.rs`): `Get { keys: Vec<Bytes> }`
  (multi-key!), and single-key `Set/Add/Replace/Append/Prepend/Delete/Incr/Decr/Touch { key, .. }`.
- Deps: `rusty-mcrouter-core` pulls `rand = "0.10"` solely for the random pick
  (`Cargo.toml`). Once selection lands, **`rand` can be dropped** — a nice
  side-cleanup. `unsafe_code = "forbid"` and MSRV `1.75` constrain us to safe
  Rust (fine — see below).

---

## target design

### 1. two tiers: stateless selectors vs stateful route strategies

The single most important decision, made before any hash math: **not every
"selection strategy" is the same shape.** mcrouter lumps them into one `hash_func`
config slot but implements them in two fundamentally different ways, and rusty
must too.

| Tier | Examples | Input it needs | Shape in rusty |
|---|---|---|---|
| **Stateless index selector** | `Ch3`, `Crc32`, `WeightedCh3`, `Rendezvous`, `ConstShard` | routing key + static pool config (size, weights, server names, shard table) bound at build time | a `Selector` impl, run inside `SelectionRoute` |
| **Stateful route strategy** | `Latest`, `LoadBalancer` | *live* backend state (health, load, last-good index), and the ability to **retry** other children | its **own `Route` handle**, holding state via interior mutability |

Why the split is non-negotiable:

- A `Selector` is a **pure function**: `fn select(&self, key) -> usize`, `&self`,
  same key → same index, forever. That's what makes `Ch3` testable against golden
  vectors and what makes "consistency" a property you can assert.
- `Latest`/`LoadBalancer` are *not* pure. `Latest` remembers the last backend that
  worked and sticks to it until it fails; `LoadBalancer` weights by observed load.
  Both mutate per request and both may try a *different* child when the first is
  bad. Cramming that into `Selector` would force `&mut`/interior mutability into a
  trait that 90% of implementations don't want, and would still not give them the
  retry/fallback control they need — that control lives at the `Route` level
  (which already owns `req` and the children and can `.await` multiple).

So: **selectors return one index and never see a reply; strategies are route
handles that own the whole request lifecycle.** Everything below (§2–§10) builds
the selector tier and its `SelectionRoute`. §11 shows where the strategy tier
plugs in.

### 2. the `Selector` abstraction

A selector's parameters are fixed at build time (size, salt, weights, server
identities), exactly like mcrouter's `Ch3HashFunc(n)`:

```rust
// rusty-mcrouter-core/src/select/mod.rs

/// Map an already-extracted routing key to a **single primary** child index in
/// `[0, n)`, where `n` — and any per-server data (weights, owned server
/// identities, shard tables, salt) — is bound at construction.
///
/// A `Selector` is a *pure, stateless* function of the routing key: same key →
/// same index, no interior mutability, no awareness of backend health or load.
/// It returns one index, not a ranked candidate list — deterministic failover
/// ordering is a *separate* future trait (§11), not a widening of this one.
/// Strategies that need live state (Latest, LoadBalancer) are NOT selectors —
/// see §1.
pub trait Selector: 'static {
    fn select(&self, routing_key: &[u8]) -> usize;
}
```

Implementations shipping now:

- `Ch3 { n: u32 }` → `furc_hash(key, n) as usize` (the default; §4).
- `Crc32 { n: u32 }` → `(crc32(key) % n) as usize` (parity; non-consistent; §5).
- `Salted { inner: Box<dyn Selector>, salt: Bytes }` → mixes `salt` into `key`,
  then delegates to `inner.select(..)`; wraps any **byte-hashing** selector (§6).

Binding all parameters at construction matches mcrouter's constructor invariant
(`1 <= n <= furc_maximum_pool_size()`, which `Ch3::new` enforces) and keeps the
**unsalted** hot path allocation-free; salt is the one exception (it allocates a
temporary salted key per request — §6). Identity-based selectors (Rendezvous)
**own** their precomputed server hashes, built from `servers` at construction —
they don't borrow it. The trait deliberately takes the **already-extracted
routing key** (`&[u8]`), not `&Request`: routing-key extraction is shared across
all selectors and belongs to `SelectionRoute` (§7). (If a future selector needs a
non-key request field, that's a richer trait or a `&Request` overload — flagged in
[open questions](#open-questions--decisions); none of the near-term selectors need it.)

### 3. `SelectionRoute` (the mechanism) and `PoolRoute` (the named pool case)

`SelectionRoute` is the generic mechanism that turns "a request" into "forward to
`children[idx]`", delegating the *which index* decision to its `Selector`.
`PoolRoute` is the thin, named handle for the common case — a `SelectionRoute`
over one pool's backends — so the friendly name keeps a real anchor in the code
and maps 1:1 to `RouteHandleConfig::PoolRoute`. The mechanism first:

```rust
// rusty-mcrouter-core/src/select/route.rs
pub struct SelectionRoute {
    // arbitrary child routes, not just pool destinations — so the same handle
    // serves a future `HashRoute` (explicit children) and selecting among
    // sub-routes, exactly like mcrouter's `SelectionRoute`.
    children: Vec<Rc<dyn DynRoute>>,
    selector: Box<dyn Selector>,
}

impl SelectionRoute {
    pub fn new(children: Vec<Rc<dyn DynRoute>>, selector: Box<dyn Selector>) -> Option<Self> {
        if children.is_empty() { return None; }
        Some(Self { children, selector })
    }
}

impl Route for SelectionRoute {
    async fn route(&self, req: Request) -> Result<Reply> {
        let idx = self.selector.select(routing_key(&req)?);
        // Defensive bounds check — NOT a debug_assert. Ch3/Crc32 are bound to `n`
        // and cannot exceed it, but the trait-object seam can't prove that, so a
        // buggy future selector must surface a route error, not panic the task.
        // (mcrouter checks `idx >= children_.size()` too.)
        let child = self
            .children
            .get(idx)
            .ok_or(RouteError::SelectorOutOfRange { idx, n: self.children.len() })?;
        child.route_dyn(req).await
    }
}
```

This needs two new `RouteError` variants (`route.rs` currently has only
`Backend`): `SelectorOutOfRange { idx, n }` and `EmptyGet` (see §7).

`PoolRoute` is a thin, legible wrapper over `SelectionRoute`. It exists so the
common-case name has a real anchor in the code, and it earns its keep by carrying
the **pool name** for diagnostics — something `SelectionRoute` (a generic
mechanism) has no business knowing:

```rust
// rusty-mcrouter-core/src/select/pool_route.rs
/// Hash-select among a *pool's* backends. The named common case: a
/// `SelectionRoute` over the pool's destinations (in `servers` order), plus the
/// pool name for logs/metrics/errors. `RouteHandleConfig::PoolRoute { pool, hash }`
/// builds exactly this.
pub struct PoolRoute {
    pool: String,              // for diagnostics; SelectionRoute can't/shouldn't know it
    inner: SelectionRoute,
}

impl PoolRoute {
    pub fn new(
        pool: impl Into<String>,
        destinations: Vec<Rc<DestinationRoute>>,
        selector: Box<dyn Selector>,
    ) -> Option<Self> {
        // coerce the pool's shared destinations into generic dyn children
        let children = destinations.into_iter().map(|d| d as Rc<dyn DynRoute>).collect();
        Some(Self { pool: pool.into(), inner: SelectionRoute::new(children, selector)? })
    }
}

impl Route for PoolRoute {
    async fn route(&self, req: Request) -> Result<Reply> {
        self.inner.route(req).await   // forwards; one inlined, non-virtual hop
    }
}
```

There is **no duplication**: `SelectionRoute` stays the single place selection
happens; `PoolRoute` only names it and supplies the pool's backends as children.
The names read top-to-bottom: `Selector` (the policy) → `SelectionRoute` (the
mechanism) → `PoolRoute` (the named pool case). This mirrors mcrouter, where
*"PoolRoute provides the same functionality as HashRoute."*

**`HashRoute` is a future sibling, not built now.** It is *basically a `PoolRoute`
except its children aren't leaf destinations*: the children are given **explicitly**
in config and can be arbitrary sub-routes (other pools, failover groups, …) rather
than one pool's backends. Because `SelectionRoute.children` is already
`Vec<Rc<dyn DynRoute>>`, `HashRoute` is just a second builder front-end over the
same `SelectionRoute` — no new runtime machinery. Deferred until something needs
to hash across non-leaf children; `PoolRoute` covers the overwhelming common case.

**Why `Box<dyn Selector>` and not `SelectionRoute<S: Selector>`:** the selector is
chosen from JSON at build time, so the choice is inherently runtime-polymorphic;
a generic would just push a `Box<dyn>` somewhere else. And the per-request cost of
one virtual call is noise against an async backend round-trip. Trait object is the
right call.

### 4. `Ch3` = a safe-Rust `furc_hash` port

This is the heart of the *first* selector. Port `mcrouter/lib/fbi/hash.c`
faithfully — see the
[reference](../mcrouter/hash-routing.md#furc_hash-the-consistent-hash). All of it
is safe wrapping integer arithmetic; **no `unsafe`, no new crate.**

```rust
// rusty-mcrouter-core/src/select/furc.rs
const FURC_SHIFT: u32 = 23;             // max pool = 1 << 23 = 8_388_608
const MAX_TRIES: u32 = 32;              // CONFIRM exact value in hash.c
const MURMUR_SEED: u64 = /* CONFIRM from hash.c (the SEED arg to murmur_hash_64A) */;

/// MurmurHash64A — standard; CONFIRM constants match mcrouter's copy.
fn murmur_hash_64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let mut h = seed ^ (key.len() as u64).wrapping_mul(M);
    let mut chunks = key.chunks_exact(8);
    for c in &mut chunks {
        let mut k = u64::from_le_bytes(c.try_into().unwrap());
        k = k.wrapping_mul(M); k ^= k >> R; k = k.wrapping_mul(M);
        h ^= k; h = h.wrapping_mul(M);
    }
    // tail bytes + final mix … (CONFIRM mcrouter's tail handling matches)
    h
}

fn murmur_rehash_64a(h: u64) -> u64 { /* mcrouter's rehash of a 64-bit word */ }

/// Lazily-extended bitstream: bit `idx` lives in word `idx >> 6`, bit `idx & 63`.
/// This is mcrouter's `furc_get_bit` + its per-call `hash[]` word cache: the
/// binary-tree descent consumes a key-derived pseudo-random bit stream of
/// unpredictable length (positions are strided `FURC_SHIFT` apart and the
/// MAX_TRIES loop reads deeper on each retry), so words are generated on demand
/// (word 0 = murmur, each later word = rehash of the previous) and memoized.
struct Bits<'a> { key: &'a [u8], words: Vec<u64>, /* cache */ }
impl Bits<'_> {
    fn get(&mut self, idx: u32) -> u64 { /* fill words on demand, return one bit */ }
}

pub fn furc_hash(key: &[u8], m: u32) -> u32 {
    if m <= 1 { return 0; }
    // port the binary-tree descent + MAX_TRIES retry loop verbatim
    // from hash.c (see the reference doc's quoted loop).
    todo!("faithful port")
}
```

`Ch3` is then a trivial `Selector`:

```rust
pub struct Ch3 { n: u32 }
impl Ch3 {
    pub fn new(n: usize) -> Result<Self, SelectorBuildError> {
        const MAX: usize = 1 << FURC_SHIFT; // 8_388_608
        if !(1..=MAX).contains(&n) {
            return Err(SelectorBuildError::Ch3PoolSizeOutOfRange { n });
        }
        Ok(Self { n: n as u32 }) // safe: 1 <= n <= 2^23
    }
}
impl Selector for Ch3 {
    fn select(&self, key: &[u8]) -> usize { furc_hash(key, self.n) as usize }
}
```

**Constants to confirm from `hash.c` before/while implementing** (the librarian
pass didn't capture their literal values):

- `MURMUR_SEED` — the `SEED` passed to `murmur_hash_64A`.
- `murmur_rehash_64A`'s exact body.
- `MAX_TRIES` (reasoned to be 32 — verify).
- MurmurHash64A **tail-byte** handling (the switch on `len & 7`).

These four are the entire risk surface for byte-compatibility; everything else
is mechanical. The [testing](#testing-prove-it-matches-mcrouter) section pins
them down with vectors.

### 5. `Crc32` (parity, secondary)

A table-driven CRC32 is ~20 lines of safe Rust, or pull the tiny well-audited
`crc32fast` crate. Recommendation: **implement it inline** to stay dependency-
minimal (consistent with the workspace's lean `Cargo.toml`), and because we only
need correctness, not SIMD throughput, on a cache key. It's a `Selector` like any
other (`Crc32 { n }`, `select = (crc32(key) % n) as usize`). It's non-consistent
by nature — document that and keep `Ch3` the default.

**Wire-compat caveat:** "CRC32" is a family — variants differ in polynomial,
init, input/output reflection, and final XOR. If `Crc32` must place keys
identically to mcrouter's `Crc32HashFunc` (a mixed rusty/mcrouter pool on
`hash:"Crc32"`), the implementation must match mcrouter's exact variant, pinned by
golden vectors the same way `Ch3` is. Until those vectors exist, mark rusty
`Crc32` **non-wire-compatible** (fine for a single-implementation pool; not for a
mixed one).

### 6. `Salted` (salt as a decorator)

mcrouter mixes `salt` into the key before hashing (`hashWithSalt`). Because salt
is "mutate the key bytes before the selector sees them," it composes over any
**byte-hashing** selector uniformly as a decorator. Store the inner selector as a
`Box<dyn Selector>` (not a generic `S`) so `build_selector` (§9) can wrap an
already-boxed `base` without an `impl Selector for Box<dyn Selector>` dance:

```rust
pub struct Salted { inner: Box<dyn Selector>, salt: Bytes }
impl Selector for Salted {
    fn select(&self, key: &[u8]) -> usize {
        // mirror mcrouter's hashWithSalt: hash(salt ++ key) (CONFIRM order/format).
        // NOTE: allocates a temporary salted key per request. Only salted pools
        // pay this; the unsalted path is allocation-free. A no-alloc variant
        // (feed salt then key into the hasher incrementally) is a later option.
        self.inner.select(&salted_bytes(&self.salt, key))
    }
}
```

Keeping salt a decorator (rather than a field on `SelectionRoute`) means it works
identically for `Ch3`, `Crc32`, and any future byte-hashing selector with zero
per-selector code. Whether `salt` is even meaningful for a non-hashing selector
like `ConstShard` follows mcrouter — **CONFIRM** before promising it there. Two
pools with the same servers but different salts distribute the same keys
differently.

### 7. routing-key extraction

`HashSelector` in mcrouter hashes `routingKey()`. In rusty this is a free function
owned by `SelectionRoute` (shared by all selectors), extracting the bytes to hash
from a (post-split, single-key) `Request`:

```rust
// The routing key (bytes to hash) for a single-key op. A free fn owned by the
// selection layer and reusable by future strategy handles (§11).
fn routing_key(req: &Request) -> Result<&[u8], RouteError> {
    let key: &[u8] = match req {
        Request::Set { key, .. } | Request::Delete { key } | Request::Add { key, .. }
        | Request::Replace { key, .. } | Request::Append { key, .. }
        | Request::Prepend { key, .. } | Request::Incr { key, .. }
        | Request::Decr { key, .. } | Request::Touch { key, .. } => key,
        // Today `Request::Get` is still `{ keys: Vec<Bytes> }`. hash-routing and
        // multiget are *independent* — neither blocks the other (./multiget.md,
        // "why … hash-routing"). Until the routed Get becomes `{ key }`, hash the
        // first key (the sanctioned interim: correct for the common single-key
        // get) and return an error for an empty get rather than panicking.
        Request::Get { keys } => keys.first().map(Bytes::as_ref).ok_or(RouteError::EmptyGet)?,
    };
    Ok(hash_stop_cut(key)) // cut at b"|#|"; prefix stripping deferred (non-goals)
}
```

The `|#|` hash-stop (`hash_stop_cut`) is a cheap, well-defined slice and worth
doing now; `/region/cluster/` prefix stripping is deferred until prefix routing
exists (noted in non-goals). Note `routing_key` is **fallible**: an empty `Get`
yields `RouteError::EmptyGet` (never a panic), and a genuine multi-key `Get`
arriving before the split lands hashes its first key — matching `multiget.md`'s
interim, not a silent surprise unique to this doc.

### 8. config parsing

Extend `RouteHandleConfig::PoolRoute` to carry an optional hash spec, parsed from
both the object form and (default) the shorthand:

```rust
// rusty-mcrouter-config/src/route.rs
pub enum RouteHandleConfig {
    PoolRoute { pool: String, hash: HashConfig },
    // …
}

#[derive(Clone, Debug, PartialEq)]
pub struct HashConfig {
    pub func: HashFunc,        // default Ch3
    pub salt: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub enum HashFunc {
    #[default] Ch3,
    Crc32,
    // future selector-backed: WeightedCh3 { weights: Vec<f64> }, Rendezvous, ConstShard
    // future strategy-backed (NOT selectors): Latest { .. }, LoadBalancer { .. }
}
```

`HashFunc` is an enum precisely so the two tiers can coexist in one config slot
(matching mcrouter's `hash_func`) while the *builder* (§9) routes each variant to
the right tier. Variants carry their own data (weights, strategy knobs) as they're
added — additive, no churn to existing variants.

Parsing rules (match mcrouter) — each is a test case:

- `"hash"` absent → `HashConfig { func: Ch3, salt: None }`.
- `"hash": "Ch3"` (bare string) → that function, no salt.
- `"hash": { "hash_func": "Crc32", "salt": "x" }` (object) → func + salt.
- `"hash": { "salt": "x" }` (object, `hash_func` **omitted**) → `Ch3` + salt
  (mirrors mcrouter's "omitted `hash_func` defaults to `Ch3`").
- Unknown `hash_func` string → a config error (don't silently fall back).
- Non-string `hash`, or non-string `hash_func` / `salt` inside the object → a
  config error (don't coerce).
- `"PoolRoute|P"` shorthand → pool `P`, default `Ch3` (no hash slot in shorthand).
- A pool-level `hash` (landing in `PoolConfig.extra`) stays **ignored** — mcrouter
  reads `hash` from the route, not the pool.

This also means the object-form parser must **stop dropping** the `hash` sibling
(today `object_form_pool_route_silently_drops_extras` shows everything but `pool`
is discarded).

### 9. wiring: `route_builder` → `SelectionRoute`

`get_or_build_pool` already builds the destinations vector (and has the pool's
`servers` list from `PoolConfig`). Two changes:

**(a) Fix the cache collision.** Cache the **destinations**, not the handle, keyed
by pool name; build the per-route handle (a `PoolRoute`, or a strategy
handle) on each reference. Destinations are the expensive, shareable artifact
(they hold live connections); the selector is cheap and route-specific.

```rust
// pool_cache: BTreeMap<String, Vec<Rc<DestinationRoute>>>   // was Rc<PoolRoute>
let destinations = self.get_or_build_destinations(pool_name).await?; // clone of cached Vec
let handle = build_pool_handle(pool_name, &hash_config, &pool_config.servers, destinations)?;
```

`get_or_build_destinations` returns a **clone of the cached `Vec`** — cheap `Rc`
clones; the underlying `Client` connections stay shared across every route that
references the pool.

> This changes the existing `pool_referenced_twice_is_built_once_and_shared`
> test: it should now assert the **destinations** are shared (`Rc::ptr_eq` on a
> `DestinationRoute`), while two references with different `hash` get *distinct*
> `PoolRoute`s over the same shared destinations.

**(b) Two-tier dispatch.** `build_pool_handle` is where the enum splits into the
two tiers — selector-backed funcs wrap destinations in a `PoolRoute` (over a
`SelectionRoute`); strategy-backed funcs build their own handle:

```rust
fn build_pool_handle(
    pool: &str,
    cfg: &HashConfig,
    servers: &[String],
    destinations: Vec<Rc<DestinationRoute>>,
) -> Result<Rc<dyn DynRoute>, BuildError> {
    match cfg.func {
        // ── stateless tier: a PoolRoute (thin named wrapper over SelectionRoute) ──
        HashFunc::Ch3 | HashFunc::Crc32 /* | WeightedCh3 | Rendezvous | ConstShard */ => {
            let selector = build_selector(cfg, servers)?;       // size/weights/identity bound here
            // PoolRoute::new coerces the shared destinations into dyn children
            let route = PoolRoute::new(pool, destinations, selector)
                .ok_or_else(|| BuildError::EmptyPool { /* .. */ })?;
            Ok(route.into_dyn())
        }
        // ── stateful tier (future): a dedicated Route handle, NOT a selector ──
        // HashFunc::Latest       => Ok(LatestRoute::new(destinations, cfg).into_dyn()),
        // HashFunc::LoadBalancer => Ok(LoadBalancerRoute::new(destinations, cfg).into_dyn()),
    }
}

fn build_selector(cfg: &HashConfig, servers: &[String]) -> Result<Box<dyn Selector>, BuildError> {
    let n = servers.len();
    let base: Box<dyn Selector> = match &cfg.func {
        HashFunc::Ch3   => Box::new(Ch3::new(n)?),    // enforces 1 <= n <= 2^23
        HashFunc::Crc32 => Box::new(Crc32::new(n)?),
        // Rendezvous needs `servers` (identities); WeightedCh3 needs weights from cfg.
        other => return Err(BuildError::NotASelector { func: other.name() }),
    };
    Ok(match &cfg.salt {
        Some(s) => Box::new(Salted::new(base, Bytes::from(s.clone()))),
        None => base,
    })
}
```

Note `build_selector` takes **`servers: &[String]`**, not just the count, because
identity-based selectors (Rendezvous) hash server names and weighted ones align
weights positionally to servers — `DestinationRoute` can't supply that.

Errors layer cleanly: selector constructors return `SelectorBuildError` (e.g.
`Ch3PoolSizeOutOfRange`), which `BuildError` wraps via `#[from]` so `Ch3::new(n)?`
just works; an unknown `hash_func` is already rejected at config-parse time (§8),
so it never reaches here; `NotASelector` is a defensive `BuildError` guard for a
*strategy* func (Latest/LoadBalancer) mistakenly routed to the selector builder
instead of its own handle.

### 10. `SelectionRoute::route`

```mermaid
flowchart LR
  REQ["Request (single-key)"] --> RK["routing_key(req) (+ |#| cut)"]
  RK --> SEL["selector.select(key) → idx"]
  SEL --> CH["children[idx].route(req)"]
```

The handle is dumb on purpose: extract key, ask the selector, forward. All the
variation — which hash, salt, weights, identities — lives in the `Box<dyn
Selector>` built once at config time.

### 11. the future seam: how new strategies slot in

The whole point of this redesign. Adding a strategy is **additive**, and lands in
exactly one of the two tiers:

**Add a stateless selector** (e.g. `Rendezvous`, `WeightedCh3`, `ConstShard`):

1. Add a `HashFunc` variant (with its data, e.g. `WeightedCh3 { weights }`).
2. Implement `Selector` for it (`fn select(&self, key) -> usize`).
3. Add one arm to `build_selector` (and the `Ch3 | Crc32 | …` list in
   `build_pool_handle`). It's bound to `servers`/`weights` there.
4. No change to `SelectionRoute`, `routing_key`, salt, the `Route` trait, or any
   call site. Golden-vector/consistency tests follow the `Ch3` template.

**Add a stateful strategy** (e.g. `Latest`, `LoadBalancer`):

1. Add a `HashFunc` variant (with its knobs, e.g. `failover_count`, `load_ttl_ms`).
2. Write a new `Route` handle (e.g. `LatestRoute`) holding the destinations **and
   its mutable state** (via `RefCell`/atomics, since `Route::route` is `&self`),
   implementing the retry/fallback logic it needs.
3. Add one arm to `build_pool_handle`'s strategy tier returning that handle.
4. It does **not** touch `Selector` or `SelectionRoute` — that's the entire reason
   for the split. The pure-selector path stays pure.

**A third shape, anticipated but not built: ranked selection.** A future
[`FailoverRoute`](./failover.md) may want "hash to a primary, then try the *other* children in a
deterministic, key-derived order." That needs **more than one index**, so it is
*not* `Selector` (single primary index) and *not* the stateful tier (it's still a
pure function of the key). When it arrives, add a separate
`RankedSelector { fn rank(&self, key: &[u8]) -> impl Iterator<Item = usize> }` (or
a candidate-iterator) used by the failover route — do **not** widen `Selector`'s
return type now. `Ch3`/`Rendezvous` could later offer both; keeping the traits
separate means single-primary callers never pay for ranking.

This is why we don't collapse to a single `PoolRoute { children, Box<dyn Selector> }`
even though it's tempting for the two hashes shipping today: it has no home for
the stateful tier we know is coming, and would force the next person to either
leak state into `Selector` or special-case `PoolRoute`.

---

## multi-key `get`: handled upstream

`Request::Get` carries `Vec<Bytes>`, and a client's `get k1 k2 k3` can name keys
that hash to **different** backends — so it cannot be routed as one unit. That
split is **not** a selection route's job: it happens once, at the
request/connection layer, *above all routing*, exactly as mcrouter splits a
multiget in its ASCII parser. See [`./multiget.md`](./multiget.md).

The consequence for this doc: once the split lands, by the time a `Get` reaches
`SelectionRoute` (or any route handle) it has **exactly one key** — the handle
hashes that key and forwards (no bucketing, no fan-out, no reply merging here).
The two designs are **independent** (`./multiget.md`): until the split lands,
`routing_key` (§7) hashes a get's *first* key as a sanctioned interim and returns
`RouteError::EmptyGet` for an empty get — it never panics or fans out. So
hash-routing can land before, after, or with multiget.

> An earlier draft put the split *inside* `PoolRoute` (bucket keys by destination,
> fan out, merge hits). That was the wrong layer: it only fires when a multiget
> hits a selection route directly, and it would have to be re-implemented in every
> handle that forwards a `Get` (`NullRoute`, a future `FailoverRoute`, …).
> [`./multiget.md`](./multiget.md) supersedes it — split once, above routing.

---

## server order is the index space (and when it isn't)

Index-based selectors (`Ch3`, `Crc32`) return an **index into the destination
list**, and `route_builder` builds that list in pool-`servers` order. Consequences
to document for operators (mirrors mcrouter):

- **Appending** a server to a pool grows `N` and `Ch3` re-homes ~`1/N` of keys
  (consistent — good).
- **Reordering** servers, or **removing** a non-last server, shifts indices and
  re-homes far more than `1/N` (with `Ch3`, removal of the k-th server is *not*
  the clean inverse of appending — only growing/shrinking at the tail is cheap).
  With `Crc32`, any change reshuffles everything.
- So for the index-based selectors: treat pool order as append-only where
  possible. This is inherent to index-based consistent hashing, not a rusty quirk.

**This is selector-dependent, which is another reason the strategy is pluggable.**
A future `Rendezvous` (HRW) selector scores each server independently from
`hash(key, server_id)` and picks the max — so it is **order-independent**:
reordering `servers` changes nothing, and removing a server re-homes only *that
server's* keys regardless of position. When operators need that property
(frequent mid-list churn), they pick `Rendezvous`; when they need mcrouter
wire-compatibility, they pick `Ch3`. The framework makes that a config choice, not
a fork.

---

## testing: prove it matches mcrouter

The protocol crate already asserts against mcrouter wire fixtures
(`reply.rs::single_hit_matches_mcrouter_fixture`). Do the same here, at two levels.

**Selector level (per `Selector`):**

1. **In-range.** For any selector built with size `n`, `select(key) < n` for all
   keys (fuzz with random/edge keys). Guards the `SelectionRoute` bounds check.
2. **Determinism.** Same key → same index across calls and across freshly built
   selectors.
3. **`Ch3` golden vectors.** From the pinned mcrouter source (or a built binary),
   generate `(key, m) → furc_hash(key, m)` for a spread of keys and pool sizes —
   include the boundaries: `m ∈ {1, 2, 3, 5, 8, 100, 1024, 2^23}` (the max), keys
   including empty, short, long, binary. Commit them as a fixture; assert our port
   reproduces every value. This is what nails down the four "CONFIRM" constants —
   if a vector mismatches, a constant is wrong. Separately assert `Ch3::new`
   **rejects** `m = 0` and `m > 2^23` (the constructor bound). Require these
   vectors **green before** wiring `Ch3` into routing (step 3 of implementation).
   If `Crc32` is to be wire-compatible, commit mcrouter `Crc32HashFunc` vectors
   the same way; otherwise mark it non-compatible (see §5).
4. **Distribution.** Over many random keys, each of `N` buckets gets roughly
   `1/N` (within tolerance) — guards against a broken descent collapsing to one
   bucket.
5. **`Ch3` consistency.** Hash a large key set at `N` and at `N+1`; assert only
   ~`1/(N+1)` of keys change index (the mcrouter guarantee). This is the property
   `Crc32` will *fail* — a nice contrast test (assert it *does* reshuffle).
6. **Salt.** Same keys, different salt → different distribution (over `Salted<Ch3>`).

**Route level (`SelectionRoute`):**

7. **Single-key routing.** Each single-key op (including a post-split get) lands
   on the selected backend. The `counting_mock_backend` helper already in
   `pool_route.rs` tests, plus `mock_backend`/`pipelining_mock_backend`
   (`rusty-mcrouter-net/src/testing.rs`), cover this.
8. **Selector swap.** Same pool, two routes with different `hash` → distinct
   `SelectionRoute`s with (potentially) different placement; shared destinations
   (the cache-fix assertion).

End-to-end multiget-spanning-backends tests live with
[`./multiget.md`](./multiget.md), since the split is upstream.

---

## how this maps to mcrouter

| mcrouter | rusty |
|---|---|
| `SelectionRoute<RouterInfo, HashSelector<Func>>` | `SelectionRoute` holding `Box<dyn Selector>` |
| `SelectionRoute::select` → `children[idx]` | `SelectionRoute::route` indexes `children` |
| `HashSelector::select` → `routingKey()` | `routing_key(&Request)` (+ `|#|` cut), owned by `SelectionRoute` |
| `Ch3HashFunc(n)` / `furc_hash` | `Ch3 { n }: Selector` / `furc_hash` (safe port) |
| `Crc32HashFunc` | `Crc32 { n }: Selector` |
| `hashWithSalt(key, salt)` | `Salted: Selector` decorator (wraps `Box<dyn Selector>`) |
| `WeightedCh3HashFunc` | future `WeightedCh3: Selector` (enum variant carries weights) |
| `Rendezvous` / `ConstShard` | future `Selector`s (Rendezvous is order-independent) |
| `Latest` / `LoadBalancer` (route *strategies*) | **separate `Route` handles**, NOT `Selector`s — the stateful tier |
| `createHashRoute` dispatch on `hash_func` | `build_pool_handle` (two-tier) + `build_selector` |
| `makePoolRoute` reads route `hash` | `route_builder::get_or_build_pool` |
| `PoolRoute` ≡ `HashRoute` (config sugar) | `PoolRoute` = thin named wrapper over `SelectionRoute` (one pool's backends, carries pool name); `HashRoute` = future sibling, same wrapper but children are explicit sub-routes, not leaf destinations |
| multiget split (in the ASCII parser) | request-layer split — see [`./multiget.md`](./multiget.md), **not** any route handle |
| `furc_maximum_pool_size()` = `2^23` | `Ch3::new` size-bound check |
| default `Ch3` | `HashFunc::default() == Ch3` |

---

## implementation order

1. **`furc_hash` + `murmur_hash_64a` port, behind `Selector`/`Ch3`**, with golden
   vectors from mcrouter (test 3). Pure library code, no routing changes yet —
   get byte-compatibility green first.
2. **`Crc32` + `Salted`** selectors, with their tests. Still no routing changes.
3. **`SelectionRoute` handle + `routing_key`**: introduce the generic
   `SelectionRoute` handle (`Vec<Rc<dyn DynRoute>>` children) and the thin
   `PoolRoute` wrapper, replacing the old random-pick `PoolRoute`; route by the
   key's selected index, and add the `RouteError::{SelectorOutOfRange, EmptyGet}`
   variants. Drop the `rand` dependency. **Independent of the multiget split** —
   `routing_key` hashes a get's first key as an interim until that lands, so this
   step needs no sequencing against it.
4. **Config + two-tier wiring**: extend `RouteHandleConfig::PoolRoute` with
   `HashConfig`/`HashFunc`, parse string/object forms, default `Ch3`, error on
   unknown func; stop dropping the `hash` sibling. Wire `build_pool_handle` /
   `build_selector` in `route_builder`, change the pool cache to store
   destinations, and pass `servers` through.
5. **Docs**: write `../architecture/hash-routing.md` (as-built) and update this
   doc's status to Implemented.

Steps 1–2 are risky-but-isolated (the math); 3–4 are wiring; each step is
independently testable. The stateful tier (`Latest`/`LoadBalancer`) and the extra
selectors (`Rendezvous`/`ConstShard`/`WeightedCh3`) are **follow-ons enabled by
this seam** (§11) — not part of this cut, but each is a localized addition. The
multiget split is sequenced separately in [`./multiget.md`](./multiget.md) and is
**independent**: it can land before, after, or with step 3, since `routing_key`
hashes a get's first key as an interim until it does.

---

## open questions / decisions

- **Two tiers (decided):** stateless `Selector`s inside `SelectionRoute`;
  stateful `Latest`/`LoadBalancer` as their own `Route` handles. The `Route`
  trait's `&self` signature forces interior mutability for the latter, confirming
  they don't belong in a pure `Selector`. Revisit only if a strategy emerges that
  is both pure *and* needs reply access (none known).
- **Pool cache (decided):** cache **destinations** (`Vec<Rc<DestinationRoute>>`)
  by pool name; build the handle per route reference, coercing the shared
  `Rc<DestinationRoute>`s into `Rc<dyn DynRoute>` children. Resolves the
  same-pool/different-hash collision and keeps the expensive part (connections)
  shared. Update the existing `pool_referenced_twice_is_built_once_and_shared`
  test to assert shared *destinations* (not a shared handle), and add a regression
  test: two routes referencing one pool with **different** `hash` get distinct
  `SelectionRoute`s over the same destination `Rc`s.
- **`SelectionRoute` children type (decided: `Vec<Rc<dyn DynRoute>>`):** generic
  child routes, not `Rc<DestinationRoute>` — so a future `HashRoute` (explicit
  children) and selecting among sub-routes reuse the handle. One extra vtable hop
  to the destination, negligible vs the backend round-trip.
- **`PoolRoute` as a thin named wrapper (decided — reverses "config name only"):**
  `PoolRoute` is a real handle wrapping `SelectionRoute` and carrying the pool name
  for diagnostics; the `RouteHandleConfig::PoolRoute` variant maps 1:1 to it. Trade:
  one inlined forwarding hop + a second type, bought for discoverability and
  pool-scoped logs/metrics. No duplication — `SelectionRoute` stays the single
  selection mechanism. `HashRoute` (explicit, possibly non-leaf children) is the
  deferred sibling, built only when something needs to hash across sub-routes
  rather than one pool's backends.
- **Multiget independence (decided):** hash-routing does **not** depend on the
  multiget split (`./multiget.md` agrees — neither blocks the other). `routing_key`
  is fallible: empty `Get` → `RouteError::EmptyGet`; multi-key `Get` (pre-split)
  hashes its first key as the sanctioned interim. No `keys[0]` panic, no silent
  fan-out.
- **Ranked selection (future seam, not now):** deterministic failover ordering
  needs a *separate* `RankedSelector` (candidate iterator), not a wider `Selector`
  return type (§11). Don't build it until a `FailoverRoute` needs it.
- **`Selector::select(&[u8])` vs `(&Request)` (decided: `&[u8]`):** routing-key
  extraction is shared, so `SelectionRoute` does it once and hands selectors the
  key. If a future selector needs a non-key request field, add a richer trait
  rather than retrofitting `&Request` onto every selector.
- **`n`/identity bound at construction vs per call (decided: at construction):**
  matches mcrouter, allocation-free hot path. Revisit only if pools become dynamic.
- **`WeightedCh3` weights source:** route config `weights`, aligned positionally
  to pool `servers` (mcrouter defaults missing entries to `0.5`). The
  `build_selector(cfg, servers)` signature already carries what's needed. Deferred.
- **Byte-compatibility scope:** is matching mcrouter's `furc` exactly a hard
  requirement (mixed rusty/mcrouter fleet over one pool) or just nice-to-have? It
  drives how hard we pin the murmur constants. (Lean: hard requirement — it's
  cheap to get right and makes the golden-vector test meaningful.)
- **`|#|` hash-stop now or later (decided: now):** cheap; include so keys with
  explicit hash stops route like mcrouter. Routing-prefix stripping waits for
  prefix routing.

---

## done when

- `SelectionRoute` selects backends via a configurable `Box<dyn Selector>`;
  `random_range` and the `rand` dependency are gone.
- `Ch3`/`furc_hash` reproduces mcrouter golden vectors byte-for-byte (including
  `m = 1` and `m = 2^23`, with `Ch3::new` rejecting `0` and `> 2^23`); `Crc32` and
  `salt` (`Salted`) work; the consistency property (~`1/N` re-homing) is asserted
  for `Ch3` and shown to *fail* for `Crc32`.
- The **framework is demonstrably extensible**: adding a `Selector` is an
  additive trait impl + one builder arm (no routing-path change), and the stateful
  tier (`Latest`/`LoadBalancer`) has a defined home as separate `Route` handles —
  both documented in §11 with the dispatch seam in place.
- Every op routes to its key's selected backend; `routing_key` is fallible (empty
  get → `RouteError::EmptyGet`, never a panic) and — until the **independent**
  multiget split lands ([`./multiget.md`](./multiget.md)) — hashes a multi-key
  get's first key as an interim; no handle fans out.
- `hash` / `hash_func` / `salt` parse on `PoolRoute` (string + object forms),
  default `Ch3`, error on unknown function; the object form no longer drops the
  `hash` field; the pool cache no longer collides on same-pool/different-hash.
- `lsp_diagnostics` / clippy clean; `../architecture/hash-routing.md` written and
  this doc flipped to Implemented.
