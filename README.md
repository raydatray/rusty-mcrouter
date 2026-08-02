# rusty-mcrouter
vibecoded [mcrouter](https://github.com/facebook/mcrouter) in rust

## what's really different from real mcrouter
- `rusty-mcrouter` is [meta protocol](https://github.com/memcached/memcached/wiki/MetaCommands) compatible only
  - why: meta wasn't a thing when mcrouter was first made, so it needed classic ascii plus facebook's private binary protocols to do leases, stale-while-revalidate, etc. meta is the open successor that does all of that with one flag-based command set.

## what's what:
- `rusty-mcrouter/` — the binary. cli + config parsing, spawns the proxy threads.
- `rusty-mcrouter-protocol/` — the meta protocol codec: semantic request/reply types, frontend encoder/decoder, backend encoder/decoder
- `rusty-mcrouter-net/` — the client-facing tcp server + the backend memcache client.
- `rusty-mcrouter-core/` — routing: the route trait + route types (pool, destination, null, error), built from config.
- `rusty-mcrouter-config/` — parses mcrouter-style json/jsonc config (pools + routes).
- `docs/` — design / architecture / mcrouter notes (see `docs/README.md`)
