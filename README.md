# rusty-mcrouter
vibecoded mcrouter in rust

## what's what:
- `rusty-mcrouter/` — the binary. cli + config parsing, spawns the proxy threads.
- `rusty-mcrouter-protocol/` — memcached ascii protocol: request/reply types, parser, serializer.
- `rusty-mcrouter-net/` — the client-facing tcp server + the backend memcache client.
- `rusty-mcrouter-core/` — routing: the route trait + route types (pool, destination, null, error), built from config.
- `rusty-mcrouter-config/` — parses mcrouter-style json/jsonc config (pools + routes).
- `docs/` — design / architecture / mcrouter notes (see `docs/README.md`).
