# rusty-mcrouter
vibecoded [mcrouter](https://github.com/facebook/mcrouter) in rust

## what's really different from real mcrouter
- `rusty-mcrouter` is [meta protocol](https://github.com/memcached/memcached/wiki/MetaCommands) compatible only
  - why: meta wasn't a thing when mcrouter was first made, so it needed classic ascii plus facebook's private binary protocols to do leases, stale-while-revalidate, etc. meta is the open successor that does all of that with one flag-based command set.

## routing prefixes
- exact `/region/cluster/` routing, regional `/region/*/` fanout and global `/*/*/` fanout are supported
- `PrefixSelectorRoute` selects policies by the longest prefix of the routing key
- `--route-prefix` chooses the default route; `--send-invalid-route-to-default` enables fallback for unmatched routing prefixes
- see [routing prefixes](docs/architecture/routing-prefixes.md) for config and execution details

## config reload
- the config file is watched and reloaded without a restart; open client connections pick up the new routes on their next request
- a config that fails to parse or build is rejected and the running one kept; `rusty_mcrouter_config_last_reload_successful` says whether the file on disk is live
- `--reconfiguration-delay-ms` sets the poll period, and `--disable-reload-configs` turns reloading off
- see [config reload](docs/architecture/config-reload.md) for what survives a reload and how failures are handled

## what's what:
- `bin/rusty-mcrouter/` — the binary. cli, construct-and-wire startup, executor/thread ownership, signals, and supervision.
- `crates/protocol/` — the meta protocol codec: semantic request/reply types, frontend encoder/decoder, backend encoder/decoder
- `crates/config/` — parses mcrouter-style json/jsonc config (pools + routes).
- `crates/observability-primitives/` — std-only `Counter`, `Gauge`, and `EventSink<T>` shared by fact owners.
- `crates/backend/` — the backend leg: memcached client, destinations, health tracking, and backend metrics.
- `crates/core/` — routing: root prefix selection, pool hashing, failover and destination routes, built from config.
- `crates/frontend/` — the client-facing leg: listeners, complete protocol connections, request transport, and frontend metrics.
- `crates/proxy/` — proxy worker mailboxes, routing generations, socket distribution, and proxy-thread orchestration.
- `crates/control/` — control runtime, command mailboxes, config reload coordination, and reload-owned metrics/reporting.
- `crates/observability/` — event logging, metrics aggregation, and the `/metrics` endpoint.
- `bench/` — an isolated Cargo workspace and Python harness for benchmarking (see `bench/README.md`).
- `docs/` — design / architecture / mcrouter notes (see `docs/README.md`)

the root `Cargo.toml` defines the application workspace and shared dependencies.
packages keep their `rusty-mcrouter-*` names; executable packages live in `bin/`
and libraries in `crates/`. each package uses the usual `src/` and `tests/` layout.

runtime construction uses explicit setup/resources: app-owned shared state and
mailboxes flow into worker-local assembly, while actors expose construction and
execution separately. frontend connections submit all routed requests through a
concrete proxy request sender and collect replies through their task sets; proxy
workers own routing execution and generation pinning. see
[dependency ownership](docs/architecture/README.md#construction-and-dependency-ownership).

```bash
cargo build --locked -p rusty-mcrouter
cargo test --workspace --locked
cargo test --manifest-path bench/Cargo.toml --workspace --locked
```
