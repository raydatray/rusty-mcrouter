# rusty-mcrouter testing (architecture)

> As-built — describes the test suite as it now stands.
> Designed in: [`../design/testing.md`](../design/testing.md) — the plan; this records what was built and where it diverged.
> Mirrors: real mcrouter's three-layer test model — cited inline against the mcrouter tree (`mcrouter/lib/test/RouteHandleTestUtil.h`, `mcrouter/lib/network/test/MockMc.*`, `mcrouter/test/MCProcess.py`). There is no `../mcrouter/testing.md`.
> Related: [`./backend-client.md`](./backend-client.md) — the concrete `Client` the `Backend` trait abstracts; [`./hash-routing.md`](./hash-routing.md) + [`./multiget.md`](./multiget.md) — the route/builder tests that are now socket-free.
> Citations are by file + symbol, relative to the repo root.

---

## tl;dr

- The test pyramid now matches the crate DAG. Counts (default-running): **protocol 132, core 54, net 15, config 46, bin 6** = **253**, plus **30** Docker integration tests gated `#[ignore]`.
- **L2 seam landed.** `rusty-mcrouter-net` defines `Backend` + `BackendFactory` (`src/backend.rs`); `Client` implements `Backend`, `ClientFactory` is the production factory. `DestinationRoute<B: Backend>` is generic with **no default type parameter**; `PoolRoute::new<B: Backend>` and `build_route<F: BackendFactory>(config, factory)` thread the type through and erase it to `Rc<dyn DynRoute>`. Routing/builder tests run **socket-free** via `MockBackend`/`MockBackendFactory`.
- **L2.5 landed.** The `Client` actor — previously 0 tests — is covered by 5 tests over one `Step`-based `scripted_backend` harness (pipelining, FIFO matching, EOF, partial-read reassembly, malformed teardown).
- **L1 hardened.** Protocol has `#[cfg(test)] mod fixtures`, collapsed verb matrices, a `MAX_VALUE_SIZE` cap, reply-parser error-consume symmetry, a CAS-ignored test, and per-command partial-read tests.
- **L3 landed.** A stateful in-process mock memcached (`src/mock_memcached.rs`) backs a **default-running** in-process e2e (`rusty-mcrouter/tests/mock_e2e.rs`); the Docker memcached suite is kept as an `#[ignore]` conformance gate over the same wire assertions.
- **Convention enforced:** explicit generics, no default type params — verified (every `<… = …>` in the tree is an `Output =`/`Value =` associated-type binding, never a default).

---

## L1 — protocol (`rusty-mcrouter-protocol`)

Pure, sync, no tokio. 132 tests.

- `src/fixtures.rs` (`#[cfg(test)] mod fixtures`, no feature) — shared request/`Value` builders, a generic `serialize`, and `assert_request_round_trips`/`assert_reply_round_trips`. Replaced the 5 byte-identical storage builders, the 11 `*_round_trips_with_serializer` bodies, and the 2 `serialize` helpers.
- Verb matrices in `parser/shared.rs` tests collapse add/replace/append/prepend (basic + extra-token + round-trip), labeled by verb. `incr` and `decr` keep their own per-verb tests; `set` stays the exhaustive storage reference.
- **`MAX_VALUE_SIZE = 1 MiB`** (`parser/shared.rs`) rejects an oversized declared body in both the storage path (`parse_storage_header`) and the reply `VALUE` path (`parser/reply.rs`) — closing the unbounded-buffer resource gap.
- **Reply-parser error-consume symmetry**: every error path in `parse_reply`/`parse_get_reply` now `split_to`s the offending frame before returning `Err`, matching the request parser's contract (previously the reply parser left the buffer intact).
- A CAS-ignored test (4th `VALUE` field accepted) and per-command partial-read tests (`parser/mod.rs`) for all nine commands (`Ok(None)` + buffer untouched).

## L2 — routing (`rusty-mcrouter-core` over `rusty-mcrouter-net`)

- `net/src/backend.rs`: `trait Backend { fn send(&self, Request) -> impl Future<Output = Result<Reply>> }`, `impl Backend for Client`, `trait BackendFactory { type Backend; fn connect(&self, &str) -> … }`, `struct ClientFactory`. The trait lives in `net` because `core → net` (a `core` trait couldn't be implemented by `Client` without a cycle). Non-`Send`, mirroring `Route`.
- `core/src/routes/destination_route.rs`: `DestinationRoute<B: Backend>` (no default). `core/src/routes/pool_route.rs`: `PoolRoute::new<B: Backend>`. `core/src/route_builder.rs`: `RouteBuilder<'a, F: BackendFactory>` + `build_route<F>(config, factory)`; `get_or_build_destinations` calls `factory.connect` instead of `Client::connect`. `B`/`F` never escape past `build_pool_handle` (coerced to `Rc<dyn DynRoute>`).
- The one production call site, `rusty-mcrouter/src/proxy/thread.rs`, passes `&ClientFactory` explicitly.
- Doubles in `net/src/testing.rs` (behind `feature = "testing"`, consumed by `core` dev-deps): `MockBackend` (records requests, scripted `Reply`/`NetError`; `Send + Sync`) and `MockBackendFactory` (per-addr mock, `failing(addr)` for the `ConnectFailed` path). The 22 destination/builder tests are socket-free; `errors_on_connect_failure` is now deterministic. This is the rusty analogue of mcrouter's `RecordingRoute`/`TestHandle` (`mcrouter/lib/test/RouteHandleTestUtil.h`).
- Shared `req_get` lives once in `core/src/test_support.rs` (`#[cfg(test)]`).

## L2.5 — the `Client` actor (`rusty-mcrouter-net`)

- `net/src/testing.rs`: one `Step`-based `scripted_backend(Vec<Step>)` harness (`ReadRequests`/`Write`/`WriteChunked`/`Close`) replaces the old per-scenario handrolled backends (the dead `looping_mock_backend` was deleted).
- `net/src/client/handle.rs` (`#[cfg(test)] mod tests`): pipelining (reads N before replying → a non-pipelining client deadlocks), FIFO reply matching, EOF fail-all, partial-read reassembly, malformed-reply teardown.

## L3 — end-to-end (`rusty-mcrouter`)

- `net/src/mock_memcached.rs` (behind `feature = "testing"`) — a stateful, hashmap-backed mock memcached: `MockMcStore::apply(Request) -> Reply` (get/set/add/replace/append/prepend/delete/incr/decr/touch with real semantics, lazy exptime, CAS), a multi-connection TCP server `spawn_mock_memcached() -> SocketAddr` over a shared `Arc<Mutex<MockMcStore>>`, and fault-injection keys (`__rusty__.want_server_error`, `__rusty__.want_error`). The analogue of mcrouter's `MockMc` (`mcrouter/lib/network/test/MockMc.cpp`).
- `rusty-mcrouter/tests/mock_e2e.rs` — **runs by default, no Docker**: spawns the in-test mock, writes a config, launches the **real router binary** (subprocess), parses `READY <addr>`, and round-trips (get/set/delete/incr/multiget). Proves the full router→backend path and the multiget split/merge.
- `rusty-mcrouter/tests/integration.rs` — the 30 Docker tests (testcontainers + `memcached:1.6`) stay `#[ignore]` as the conformance gate: they assert the **same** wire bytes the mock produces. The dead `RUSTY_MCROUTER_BACKEND` env was removed.

---

## where shared test code lives

| support code | home | gating |
|---|---|---|
| protocol builders / round-trip / fixtures | `rusty-mcrouter-protocol` | `#[cfg(test)]` (within-crate) |
| `Backend`/`BackendFactory` traits, `Client`/`ClientFactory` | `rusty-mcrouter-net/src/backend.rs` | none (production API) |
| `MockBackend`/`MockBackendFactory`, `scripted_backend`/`Step` | `rusty-mcrouter-net/src/testing.rs` | `feature = "testing"` (cross-crate: `core` tests consume it) |
| stateful mock memcached | `rusty-mcrouter-net/src/mock_memcached.rs` | `feature = "testing"` (cross-crate: bin e2e consumes it) |
| `req_get` | `rusty-mcrouter-core/src/test_support.rs` | `#[cfg(test)]` (within-crate) |

`feature = "testing"` is load-bearing for cross-crate doubles because `#[cfg(test)]` is not active for a crate compiled as a dependency. `rusty-mcrouter-net { features = ["testing"] }` (bin) is a dev-dependency only; resolver v2 keeps it out of release builds.

---

## divergences from the design

The [design](../design/testing.md) is faithful overall; the deliberate or forced differences:

1. **Handrolled-backend count.** The design (following the sweep) said "6 handrolled backends → 1 harness." The live tree had only **2** (`mock_backend`, `pipelining_mock_backend`); the other four were in `.sl/origbackups/` stale copies, not the working tree. Both live ones were replaced by `scripted_backend`.
2. **`MockRoute` not promoted.** The design (and sweep) referenced a `MockRoute` in `bin/src/proxy/connection.rs` to promote into `core`; the live `connection.rs` (230 lines) has **no test module** — that `MockRoute` was also a stale-backup artifact. Nothing was promoted; per-crate `#[cfg(test)]` doubles stand.
3. **Mock memcached placement.** Lives in its own `net/src/mock_memcached.rs`, not inside `testing.rs`.
4. **In-process e2e drives the router as a subprocess.** No `run()` library entrypoint was added (deferred in the design); `mock_e2e.rs` runs the **real binary** as a subprocess against the in-test mock backend — which also keeps the binary/CLI/config/`READY` path covered without Docker.
5. **Property/fuzz testing deferred.** Neither `proptest` nor `cargo-fuzz` was added; the partial-read, body-size-cap, reply-asymmetry, and CAS gaps are closed with explicit example-based tests.
6. **CI not added.** No `.github/workflows` was created; the Docker gate runs via `cargo test --test integration -- --ignored`.

---

## source map

| concept | symbol / file |
|---|---|
| backend seam | `Backend`, `BackendFactory`, `ClientFactory` — `rusty-mcrouter-net/src/backend.rs` |
| in-process doubles | `MockBackend`, `MockBackendFactory` — `rusty-mcrouter-net/src/testing.rs` |
| socket harness | `scripted_backend`, `Step` — `rusty-mcrouter-net/src/testing.rs` |
| `Client` tests | `client/handle.rs::tests` |
| generic leaf | `DestinationRoute<B: Backend>` — `rusty-mcrouter-core/src/routes/destination_route.rs` |
| generic builder | `build_route<F>`, `RouteBuilder<'a, F>` — `rusty-mcrouter-core/src/route_builder.rs` |
| protocol fixtures | `rusty-mcrouter-protocol/src/fixtures.rs` |
| mock memcached | `MockMcStore`, `spawn_mock_memcached` — `rusty-mcrouter-net/src/mock_memcached.rs` |
| in-process e2e | `rusty-mcrouter/tests/mock_e2e.rs` |
| Docker gate | `rusty-mcrouter/tests/integration.rs` (`#[ignore]`) |
