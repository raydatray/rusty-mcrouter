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

## what's what:
- `rusty-mcrouter-protocol/` — the meta protocol codec: semantic request/reply types, frontend encoder/decoder, backend encoder/decoder
- `rusty-mcrouter-config/` — parses mcrouter-style json/jsonc config (pools + routes).
- `rusty-mcrouter-observability-primitives/` — std-only `Counter`, `Gauge`, and `EventSink<T>` shared by fact owners.
- `rusty-mcrouter-backend/` — the backend leg: memcached client, destinations, health tracking, and backend metrics.
- `rusty-mcrouter-core/` — routing: root prefix selection, pool hashing, failover and destination routes, built from config.
- `rusty-mcrouter-proxy/` — the frontend leg: client connections, proxy workers, and proxy-thread orchestration.
- `rusty-mcrouter-observability/` — event logging, metrics aggregation, and the `/metrics` endpoint.
- `rusty-mcrouter/` — the binary. cli, options, and construct-and-wire startup only.
- `docs/` — design / architecture / mcrouter notes (see `docs/README.md`)
