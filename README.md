# rusty-mcrouter
vibecoded [mcrouter](https://github.com/facebook/mcrouter) in rust

## what's really different from real mcrouter
- `rusty-mcrouter` is [meta protocol](https://github.com/memcached/memcached/wiki/MetaCommands) compatible only
  - why: meta wasn't a thing when mcrouter was first made, so it needed classic ascii plus facebook's private binary protocols to do leases, stale-while-revalidate, etc. meta is the open successor that does all of that with one flag-based command set.

## what's what:
- `rusty-mcrouter-protocol/` — the meta protocol codec: semantic request/reply types, frontend encoder/decoder, backend encoder/decoder
- `rusty-mcrouter-config/` — parses mcrouter-style json/jsonc config (pools + routes).
- `rusty-mcrouter-backend/` — the backend leg: memcached client, destinations, health tracking, and backend counters.
- `rusty-mcrouter-core/` — routing: the route trait + route types (pool, destination, null, error), built from config.
- `rusty-mcrouter-proxy/` — the frontend leg: client connections, proxy workers, and proxy-thread orchestration.
- `rusty-mcrouter-observability/` — event logging, metrics aggregation, and the `/metrics` endpoint.
- `rusty-mcrouter/` — the binary. cli, options, and construct-and-wire startup only.
- `docs/` — design / architecture / mcrouter notes (see `docs/README.md`)
