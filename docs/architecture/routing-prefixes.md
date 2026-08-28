# routing prefixes

rusty-mcrouter recognizes mcrouter routing prefixes at the start of a key:

```text
/region/cluster/key
```

the prefix selects one or more root routes. it is removed before the request is
encoded for memcached.

## key pieces

for this key:

```text
/us/cache/user:42|#|debug
```

the protocol layer exposes three views:

| view | value | use |
|---|---|---|
| routing prefix | `/us/cache/` | root route selection |
| routing key | `user:42` | prefix policy selection and pool hashing |
| backend key | `user:42\|#\|debug` | encoded for memcached |

`|#|` and its suffix are excluded from the routing key but remain in the
backend key.

## configuration

the top-level `routes` object maps exact routing prefixes to routes:

```json
{
  "pools": {
    "us-a": { "servers": ["cache-a:11211"] },
    "us-b": { "servers": ["cache-b:11211"] }
  },
  "routes": {
    "/us/a/": "PoolRoute|us-a",
    "/us/b/": "PoolRoute|us-b"
  }
}
```

the array form lets one route have multiple aliases:

```json
{
  "routes": [
    {
      "aliases": ["/us/a/", "/us/alias/"],
      "route": "PoolRoute|us-a"
    }
  ]
}
```

aliases are normalized to `/region/cluster/`. when aliases collide, the later
entry wins, matching mcrouter.

## default route

unprefixed keys use the route selected by `--route-prefix`:

```bash
rusty-mcrouter --config config.json --route-prefix /us/a/
```

the default is `/././`. a plural `routes` config must contain the selected
default prefix or startup fails.

an unknown routing prefix normally returns a route error. it can instead use
the default route:

```bash
rusty-mcrouter \
  --config config.json \
  --route-prefix /us/a/ \
  --send-invalid-route-to-default
```

## key-prefix policies

`PrefixSelectorRoute` chooses the longest matching prefix of the routing key:

```json
{
  "type": "PrefixSelectorRoute",
  "policies": {
    "user:": "PoolRoute|users",
    "user:vip:": "PoolRoute|vip-users"
  },
  "wildcard": "PoolRoute|default"
}
```

`user:vip:42` selects `user:vip:`. an unmatched key uses `wildcard`; if no
wildcard exists, the selector produces no target.

policy selection uses an immutable lower-bound prefix map built with sorted
prefixes, eight-byte search buckets and links to shorter configured prefixes.
the request path performs no allocation for common prefix lookups.

## exact routing and fallback

an exact prefix selects its configured route:

```text
/us/a/key -> /us/a/
```

when an exact prefix is not configured, rusty-mcrouter tries that region's
`fallback` cluster:

```text
/us/missing/key -> /us/fallback/
```

fallback is tried only when the exact alias is absent. an existing selector
that has no matching key policy does not use the regional fallback.

## wildcard fanout

common wildcard forms use precomputed target maps:

```text
/us/*/key -> every configured route in us
/*/*/key  -> every configured route
```

other component-scoped patterns use a slower scan:

```text
/u*/*prod/key
```

`*` never crosses `/`. matching is byte-oriented and follows mcrouter's route
pattern behavior.

the configured default route is first whenever it matches. other secondary
ordering is deterministic but is not a public API guarantee. duplicate route
handles are removed before dispatch.

## fanout execution

`RootRoute` executes the first target in the foreground and returns its reply
to the client. every additional unique target runs in a detached task on the
same proxy thread:

```text
primary target    -> awaited -> client reply
secondary targets -> detached local tasks -> replies discarded
```

each secondary gets an independent routing context. its destination attempts
and backend durations are recorded, but only the primary contributes the final
client-outcome metrics. secondaries are best effort: they are not queued
durably, do not delay the client reply and may be cancelled during proxy-thread
shutdown.
