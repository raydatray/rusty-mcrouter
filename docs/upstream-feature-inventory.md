# Upstream mcrouter feature inventory

Inventory of features present in upstream `mcrouter/`, with a decision for
whether this toy Rust implementation should implement them. This is a product
scope document, not a parity checklist: upstream mcrouter is a production cache
router and embedded client library; rusty-mcrouter should stay a small,
understandable memcached ASCII router unless a feature teaches something useful.

Source evidence comes from the local upstream checkout under `../mcrouter/`:

- Top-level feature list: `README.md`
- Protocol surface: `mcrouter/lib/network/gen/Memcache.thrift`,
  `mcrouter/lib/network/gen/Common.thrift`,
  `mcrouter/lib/network/McAsciiParser.rl`, `mcrouter/lib/network/AsciiSerialized.cpp`
- Route factories: `mcrouter/routes/McRouteHandleProvider.cpp`,
  `mcrouter/routes/McExtraRouteHandleProvider.h`
- Runtime/config/options: `mcrouter/mcrouter_options_list.h`,
  `mcrouter/standalone_options_list.h`, `mcrouter/AGENTS.md`
- Networking/security: `mcrouter/lib/network/AsyncMcClientImpl.h`,
  `mcrouter/lib/network/ThriftTransport.cpp`, `mcrouter/lib/network/SecurityOptions.cpp`
- Observability/admin: `mcrouter/stat_list.h`, `mcrouter/lib/debug/`,
  `mcrouter/tools/mcpiper/`

## Decision legend

| Decision | Meaning |
|---|---|
| **Implement** | In scope for a useful toy mcrouter and worth building. |
| **Maybe** | Useful only after core routing is healthy; defer until a concrete need. |
| **Do not implement** | Production, Meta-specific, or complexity-heavy feature that would bloat the toy router. |
| **Already partial** | Some shape exists today, but not upstream-equivalent behavior. |

## Current rusty-mcrouter baseline

Rusty currently implements a narrow subset:

- ASCII request/reply parsing and forwarding for `get`, `set`, `add`,
  `replace`, `append`, `prepend`, `delete`, `incr`, `decr`, and `touch`.
- Config parsing for mcrouter-like JSON with `pools` and one top-level `route`.
- Routes: `PoolRoute`, `NullRoute`, and `ErrorRoute` only.
- One TCP listener address from `--listen` and one config path from `--config`.
- Backend connections are eagerly opened and serialized per destination.

The more detailed command-level plan lives in `command-support.md`; the hot-path
performance comparison lives in `mcrouter-comparison.md`.

## TL;DR roadmap

| Priority | Feature group | Decision |
|---|---|---|
| 1 | Finish memcached ASCII core: `gets`, `cas`, `gat`, `gats`, `noreply`, `version`, `quit` | **Implement** |
| 2 | Correct backend connection model: lazy connect, reconnect, timeouts, pipelining, small connection pools | **Implement** |
| 3 | Routing tree essentials: prefix routing, hash/pool selection, failover, broadcast/fanout variants | **Implement** |
| 4 | Minimal admin/observability: stats counters, structured logs, debug-friendly errors | **Implement** |
| 5 | Online config reload from local files | **Maybe** |
| 6 | Shadow traffic, warm-up, L1/L2, migrations, rate limits | **Maybe** |
| 7 | TLS, Caret/Carbon/Thrift, compression, ServiceRouter, async delete stream, ACLs, QoS | **Do not implement** |

## Protocol and command surface

| Upstream feature | Evidence | Rusty decision | Notes |
|---|---|---|---|
| Memcached ASCII protocol | `README.md`; `McAsciiParser.rl`; `AsciiSerialized.cpp` | **Already partial / Implement** | Keep this as the primary protocol. Finish the command gaps documented in `command-support.md`. |
| Retrieval commands: `get`, `gets`, `gat`, `gats`, `metaget`, `lease-get` | `Memcache.thrift`; `AsciiSerialized.cpp` | **Implement** `gets`, `gat`, `gats`; **Do not implement** `metaget`, `lease-get` | CAS/TTL retrievals are standard enough to keep. Meta-specific metadata and lease commands are not. |
| Storage commands: `set`, `add`, `replace`, `append`, `prepend`, `cas`, `lease-set` | `Memcache.thrift`; `AsciiSerialized.cpp` | **Implement** `cas`; **Do not implement** `lease-set` | CAS is standard memcached. Lease set is Meta-specific dogpile prevention. |
| Deletion, counters, TTL: `delete`, `incr`, `decr`, `touch` | `Memcache.thrift`; `AsciiSerialized.cpp` | **Already partial** | Present in rusty today; keep as pass-through commands. |
| Flush commands: `flush_all`, `flushre` | `Memcache.thrift`; `mcrouter_options_list.h` `enable_flush_cmd` | **Maybe** `flush_all`; **Do not implement** `flushre` | `flush_all` needs a broadcast/all-pools routing policy. `flushre` is nonstandard and regex-flush-specific. |
| Admin/control commands: `stats`, `version`, `quit`, `shutdown`, `exec`, `goaway` | `Common.thrift`; upstream parser/IDL | **Implement** `version`, `quit`, minimal `stats`; **Do not implement** `shutdown`, `exec`, `goaway` | Keep only the admin surface useful for local debugging. |
| `noreply` modifier | `McAsciiParser.rl` has `noreply` action | **Implement** | Important ASCII compatibility; strip on backend forwarding so the router can still observe backend errors. |
| Binary Caret protocol | `mcrouter/lib/network/CaretProtocol.cpp`; compression option applies only to Caret replies | **Do not implement** | Too much protocol surface for a toy router. ASCII is enough. |
| Carbon/Thrift/Rocket transport | `ThriftTransport.cpp`; generated Carbon/Thrift files | **Do not implement** | Production client-library protocol, not needed for memcached ASCII routing. |
| Memcached binary protocol, UDP, SASL, modern meta protocol | Absent from mcrouter IDL/parser | **Do not implement** | Not upstream mcrouter parity and not useful for this project. |

## Routing and route handles

Upstream route factories registered in `McRouteHandleProvider.cpp`, plus the
extra provider hooks in `McExtraRouteHandleProvider.h`, are the best single
inventory of user-configurable routing behavior.

| Upstream route/feature | What it does | Rusty decision | Notes |
|---|---|---|---|
| `PoolRoute` | Hashes/chooses destinations from a named pool. | **Already partial / Implement** | Rusty has a simple pool route. Needs proper hashing, lazy backend connections, per-destination health, and connection pools. |
| `HashRoute` | Hash-based child selection with configurable hash functions. | **Implement** | Core to real mcrouter behavior. Start with one stable hash, then add compatible algorithms if needed. |
| Multiple hashing schemes | README feature; `WeightedCh3HashFunc`, `RendezvousHashFunc`, `WeightedRendezvousHashFunc`, `WeightedFurcHash` | **Implement subset** | Implement one or two understandable schemes: consistent hash / rendezvous. Skip every historical variant unless config needs it. |
| Prefix routing / route prefixes | README feature; `RoutingPrefix`, `PrefixPolicyRoute`, `OperationSelectorRoute` | **Implement** | Rusty already rejects prefixed `routes`; this is the next major config feature after PoolRoute. |
| `OperationSelectorRoute` / `PrefixPolicyRoute` / `RoutingGroupRoute` | Route by operation class or prefix policy. | **Implement subset** | Useful for read/write split, broadcast flush, and future failover policies. |
| `AllSyncRoute` | Send to all children synchronously. | **Implement** | Needed for broadcast operations and `flush_all`. |
| `AllAsyncRoute` | Fire-and-forget fanout. | **Maybe** | Useful for shadowing/logging, but less critical than sync fanout. |
| `AllFastestRoute` | Race children and return fastest usable reply. | **Maybe** | Interesting but not essential. Keep out until multiple pools work. |
| `AllInitialRoute` | Send to an initial subset/sequence. | **Do not implement** | Too specialized. |
| `AllMajorityRoute` | Return based on majority result. | **Do not implement** | Adds distributed-consensus-like behavior not needed here. |
| `RandomRoute` | Random child selection. | **Maybe** | Simple and useful as a teaching route; lower priority than hash. |
| `LatestRoute` | Select latest/freshest result among children. | **Do not implement** | Specialized multi-replica semantics. |
| `FailoverRoute` / `FailoverWithExptimeRoute` / `MissFailoverRoute` | Retry alternate children on errors, timeouts, misses, or exptime-aware policies. | **Implement subset** | Implement simple ordered failover on network/server errors. Defer miss/exptime/custom policies. |
| Destination health / TKO tracking | README feature; `TkoTracker`, `TkoLog`, TKO options | **Implement subset** | Basic mark-down-after-failures and probe/reconnect are valuable. Skip full TKO counters/policies initially. |
| `LoadBalancerRoute` | Load-aware child selection. | **Do not implement** | Needs server load telemetry and balancing policy surface. |
| `RateLimitRoute`, `OutstandingLimitRoute`, failover rate limiter | Throttles requests/queues/failover. | **Maybe** | Useful only after concurrency/pipelining exists. Start with simple bounded queues instead. |
| `BigValueRoute` | Splits large values into chunks internally. | **Do not implement** | Complex compatibility story; not needed for a toy router. Keep single-value pass-through. |
| `L1L2CacheRoute`, `L1L2SizeSplitRoute` | Multi-level cache routing. | **Maybe** | Educational but large. Defer until basic route composition is done. |
| `WarmUpRoute`, `SlowWarmUpRoute` | Cold cache warm-up / gradual traffic shifting. | **Maybe** | Useful after failover and multi-pool routes. Not core. |
| `MigrateRoute`, `DistributionRoute`, `StagingRoute` | Migration, distribution, staging traffic workflows. | **Do not implement** | Production rollout tooling, not a toy router goal. |
| Shadow traffic / `ShadowRoute` settings | README feature; `ShadowRoute*`, `ShadowSettings` | **Maybe** | Good learning feature after async fanout. Keep disabled by default. |
| `ModifyKeyRoute`, `KeySplitRoute`, `KeyParseRoute`, `ShardSplitRoute`, `ShardSelectionRoute`, `McBucketRoute` | Key rewriting, sharding, bucket selection. | **Maybe subset** | Prefix/key transforms are useful; shard/bucket-specific machinery is not. |
| `ModifyExptimeRoute` | Rewrites TTL/exptime. | **Maybe** | Small and useful for policy demos. |
| `HashStopAllowListRoute`, `OriginalClientHashRoute`, `HostIdRoute` | Specialized hashing/client-affinity policies. | **Do not implement** | Tied to production traffic management. |
| `BlackholeRoute`, `DevNullRoute`, `NullRoute`, `ErrorRoute`, `LoggingRoute`, `LatencyInjectionRoute` | Test/debug/synthetic routes. | **Implement subset** | Rusty has `NullRoute` and `ErrorRoute`. Add `LoggingRoute` and `LatencyInjectionRoute` only if useful for local testing. |
| `CarbonLookasideRoute`, `SRRoute`, `AxonLogRoute` | Carbon/ServiceRouter/Axon integrations. | **Do not implement** | Meta-specific integration surface. |

## Configuration and runtime behavior

| Upstream feature | Evidence | Rusty decision | Notes |
|---|---|---|---|
| JSON config from file or inline string | `mcrouter_options_list.h` `--config`, `--config-file`, `--config-str`; README quick start | **Implement subset** | Rusty supports `--config PATH`. Add `--config-str` only if tests/docs benefit. |
| Pools and servers | README quick start; `PoolFactory.cpp`; route builder | **Already partial / Implement** | Add server metadata later: protocol, timeouts, QoS tags, weights, IPv6, TLS only if in scope. |
| Named route handles / references | route config model | **Implement** | Rusty currently treats references as unresolved except bare built-ins. Needed for composable configs. |
| Prefix `routes` map | upstream prefix routing; rusty `PrefixRoutingNotImplemented` | **Implement** | Major missing config feature. |
| Config preprocessor and `config_params` | `mcrouter_options_list.h`; `mcrouter/lib/config/` | **Do not implement** | Adds a second config language. Keep JSON explicit. |
| Online reconfiguration from watched files | README feature; `FileObserver`, reload options | **Maybe** | Useful if the toy router becomes a long-running demo. Start with SIGHUP or polling local file only. |
| Config dump/fallback to last valid config | `config_dump_root`, `max_dumped_config_age` | **Do not implement** | Production resilience; overkill here. |
| Runtime vars file | `runtime_vars_file`; `RuntimeVarsData` | **Do not implement** | Another dynamic-control plane. |
| Per-flavor settings / Luna / Configerator / JustKnobs | `mcrouter/AGENTS.md` | **Do not implement** | Meta deployment system, not relevant to OSS toy. |
| Multi-cluster/region routing prefixes and timeout overrides | README feature; cross-region/cluster timeout options | **Maybe subset** | Prefix parsing is useful; real region/cluster policy is not. |

## Networking, concurrency, and backend lifecycle

| Upstream feature | Evidence | Rusty decision | Notes |
|---|---|---|---|
| Connection pooling and persistent backend clients | README feature; `AsyncMcClientImpl`; `ProxyDestination*` | **Implement** | Core router behavior. Rusty currently serializes requests behind one mutex per destination. |
| Backend request pipelining / inflight queue | `AsyncMcClientImpl` request queue, writer loop, `nextMsgId` | **Implement** | Highest performance gap already documented in `mcrouter-comparison.md`. |
| Lazy backend connect, reconnect, retry on connect timeout | `AsyncMcClientImpl`, `connect_timeout_retries` | **Implement subset** | Avoid failing startup when one backend is down; reconnect on demand. |
| Per-destination and proxy throttling | max inflight/pending options | **Maybe** | Add simple bounded queues after pipelining. Skip production-grade knobs. |
| Request deadlines and waiting timeouts | timeout options | **Maybe** | Useful once queues exist. |
| Multiple proxy/server threads, fiber pools, Mux IO thread pool | `num_proxies`, fibers options, thread utilities | **Do not implement** | Tokio already provides the concurrency model; do not mirror C++ fibers. |
| Multiple listen addresses/ports and inherited sockets | standalone options | **Maybe** | Multiple listen sockets are easy but not important. Inherited FDs are production deployment detail. |
| Unix domain socket listener | standalone options | **Maybe** | Small and useful for local testing, but low priority. |
| TCP keepalive, backlog, RTO, inactive reset | network options | **Maybe subset** | Expose only if needed for local robustness. |
| IPv6 support | README feature | **Implement** | Rusty's `ToSocketAddrs` path may already work for IPv6; document/verify when networking is revisited. |
| TCP Fast Open / zero-copy / pass-through mode | standalone options; performance docs | **Do not implement** | Platform-specific optimizations that distract from route correctness. |
| Server load reporting | standalone options; `ServerLoad` | **Do not implement** | Only needed for load-aware routing. |

## Reliability and data movement

| Upstream feature | Evidence | Rusty decision | Notes |
|---|---|---|---|
| Reliable delete stream / async log | README feature; `AsyncLog` writes delete entries; `AsynclogRoute` | **Do not implement** | Durable delete replay is a production guarantee, not a toy feature. |
| Production traffic shadowing | README feature; `ShadowRoute*` | **Maybe** | See routing section. Treat as optional learning feature. |
| Cold cache warm-up | README feature; `WarmUpRoute` | **Maybe** | See routing section. |
| Migration/distribution/replay | `DistributionRoute`, `MigrateRoute`, stats | **Do not implement** | Production data movement system. |
| Large values | README feature; `BigValueRoute` | **Do not implement** | Support normal memcached value sizes by pass-through; skip chunking protocol. |
| Multi-level caches | README feature; L1/L2 routes | **Maybe** | Interesting route-composition example, not core. |

## Security and access control

| Upstream feature | Evidence | Rusty decision | Notes |
|---|---|---|---|
| TLS/SSL client and server support | README feature; PEM/server SSL options; `ThriftTransport.cpp`; `McSSLUtil` | **Do not implement** | Adds certificate management, Fizz/OpenSSL choices, and test burden. Keep toy cleartext. |
| TLS 1.3 Fizz, TLS-to-plaintext, KTLS, TLS session cache/tickets | network/security files and options | **Do not implement** | Deep production transport stack. |
| Service identity verification/authorization | SSL identity options | **Do not implement** | Coupled to TLS and deployment identity. |
| ACL checker and prefix ACL checker | standalone options; `RequestAclChecker` | **Do not implement** | Production perimeter control. Not needed for local toy. |
| Key-client binding / crypto auth token stats | standalone options; stats | **Do not implement** | Meta-specific security policy. |

## Observability, admin, and tooling

| Upstream feature | Evidence | Rusty decision | Notes |
|---|---|---|---|
| Rich stats/debug commands | README feature; `stat_list.h`; `stats.cpp`; `StatsReply` | **Implement subset** | Add basic counters: uptime, connections, requests by command/result, backend errors, per-pool stats. |
| External stats handler / ODS-style stats | `ExternalStatsHandler`, stat groups | **Do not implement** | Production metrics pipeline. |
| Request logs, failure logs, TKO logs | `McrouterLogger`, `ProxyRequestLogger`, `TkoLog` | **Maybe subset** | Use structured stderr/tracing for local debugging. Skip rotating/spooling systems. |
| Debug FIFO / mcpiper traffic inspection | `mcrouter/lib/debug/`, `tools/mcpiper/` | **Do not implement** | Useful production tool, but large side-channel surface. Rusty can use normal logs/tests. |
| `stats` detail groups and rate counters | `stat_list.h` | **Maybe** | Implement one simple `stats` response first; avoid full group taxonomy. |
| Service info/version/build metadata | `ServiceInfo`, README `mcrouter --help` | **Implement subset** | `version` and startup banner are enough. |

## Compression and storage format helpers

| Upstream feature | Evidence | Rusty decision | Notes |
|---|---|---|---|
| LZ4, LZ4 immutable, Zstd compression codecs | `Compression*.cpp`, `Lz4*`, `Zstd*`; `enable_compression` | **Do not implement** | Upstream option only compresses Caret replies; rusty is ASCII-only. |
| `folly::IOBuf` chains and scatter/gather writes | `AsciiSerialized.cpp`, `WriteBuffer` | **Implement concept, not API** | Use Rust vectored writes where practical; do not copy Folly abstractions. |
| Buffer pools / object pools / no-dump allocator | `WriteBuffer`, `ObjectPool`, `jemalloc_nodump_buffers` | **Maybe subset** | Reuse `BytesMut` where simple. Skip custom allocator/no-dump. |

## Build, packaging, and embedding surface

| Upstream feature | Evidence | Rusty decision | Notes |
|---|---|---|---|
| Standalone binary | `main.cpp`; standalone options | **Already partial / Implement** | Rusty is primarily a binary. |
| Embedded CarbonRouterClient/libmcrouter | `CarbonRouterClient*`, `CarbonRouterInstance*` | **Do not implement** | Rust library API can exist, but not Carbon client parity. |
| CMake/autotools package, Ubuntu package | README, `CMakeLists.txt`, `configure.ac` | **Do not implement** | Cargo packaging is enough. |
| Generated Thrift/Carbon code | `lib/network/gen`, `lib/carbon` | **Do not implement** | Avoid codegen/protocol expansion. |
| Extensive upstream test harness | `mcrouter/test`, `routes/test`, `lib/network/test` | **Implement relevant tests only** | Add tests for Rust behavior; do not port the harness wholesale. |

## Recommended implementation order

1. **Finish ASCII command compatibility**: `gets`, `cas`, `gat`, `gats`,
   `noreply`, `version`, `quit`, and a minimal `stats` command.
2. **Fix destination lifecycle**: lazy connect, reconnect, request timeout, and
   per-destination pipelining.
3. **Make config composition useful**: named route handles, `routes` prefix map,
   and route references.
4. **Add core routing primitives**: stable `HashRoute`, `AllSyncRoute`, and
   simple ordered `FailoverRoute`.
5. **Add minimal health and observability**: mark-down-after-failure, probe or
   lazy retry, basic counters and logs.
6. **Only then consider maybe-features**: shadowing, warm-up, L1/L2, rate limits,
   Unix sockets, and config reload.

Everything in the **Do not implement** bucket should stay out unless the project
goal changes from “toy mcrouter in Rust” to “production mcrouter clone.”
