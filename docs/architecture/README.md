# architecture overview
rusty-mcrouter is a memcached routing proxy. clients reach it via the **meta protocol**, and rusty-mcrouter routes each request thru a tree of route handles to a destination server, tracking server health and failing over along the way

## crates
there are eight crates. dependencies point from lower-level primitives and fact owners toward
composition and presentation (`A --> B` means B depends on A):

- **`rusty-mcrouter-protocol`** - the meta protocol codec, request and reply types, the encoders and decoders for both requests and replies and key parsing
- **`rusty-mcrouter-config`** - config file parsing into pools, routes and policies
- **`rusty-mcrouter-observability-primitives`** - std-only metric cells and event sink mechanics shared by fact-owning crates; no domain records or presentation logic
- **`rusty-mcrouter-backend`** - the memcached-facing leg. a connection actor that does pipelining and FIFO reply matching, destinations that own connections and probes, and TKO tracking per destination, pool and router
- **`rusty-mcrouter-core`** - the routing graph, where a config file is transformed into a tree of route handles
- **`rusty-mcrouter-proxy`** - the client-facing leg and orchestration: accept loops, frontend protocol handling, proxy threads, workers and cross-thread dispatch
- **`rusty-mcrouter-observability`** - the event bus and log presentation, metrics aggregation and the `/metrics` endpoint
- **`rusty-mcrouter`** - the thin binary: cli, options and construct-and-wire startup

```mermaid
flowchart LR
    P[rusty-mcrouter-protocol]
    K[rusty-mcrouter-config]
    Q[rusty-mcrouter-observability-primitives]
    B[rusty-mcrouter-backend]
    C[rusty-mcrouter-core]
    X[rusty-mcrouter-proxy]
    O[rusty-mcrouter-observability]
    R[rusty-mcrouter]

    P --> B
    P --> C
    P --> X
    K --> C
    K --> X
    B --> C
    B --> X
    C --> X
    B --> O
    X --> O
    K --> R
    B --> R
    X --> R
    O --> R
    Q --> B
    Q --> X
    Q --> O
```

## request lifecycle
```mermaid
sequenceDiagram
    participant C as client
    participant P as frontend (proxy)
    participant R as route tree (core)
    participant D as destination (backend)
    participant S as server

    C->>P: mg foo v q O123
    Note over P: MetaRequestDecoder<br/>Request + MetaReplyPlan<br/>seq=N, plan pinned to conn
    P->>R: Request
    Note over R: pool, hash, failover<br/>skips TKO'd destinations<br/>consults fail-open
    R->>D: Destination::send
    Note over D: MetaRequestEncoder<br/>canonical bytes + Expectation<br/>q/O/k stripped
    D->>S: mg foo v
    S-->>D: HD
    Note over D: MetaReplyDecoder, FIFO match<br/>result feeds TKO tracker
    D-->>R: Reply
    Note over R: failover may retry siblings
    R-->>P: Reply
    Note over P: slot N ready, flush in seq order<br/>MetaReplyEncoder applies plan<br/>order, O, q
    P-->>C: HD O123
```

the identity of a request is split into three distinct components

| piece                  | what it does                                                                        | lives where                                   |
|------------------------|-------------------------------------------------------------------------------------|-----------------------------------------------|
| `Request`              | the request, stripped down to just the command, key and typed flags                 | crosses the routing graph                     |
| `MetaReplyPlan`        | how to present the reply to the client (quiet policy, opaque echo, token ordering) | pinned to the client connection, never routed |
| `MetaReplyExpectation` | the expected reply shape from a backend                                             | pinned to the backend connection's FIFO       |

three consequences of this design are:
1. **the backend never sees the client's spelling of the request** - rusty-mcrouter re-encodes from the parsed `Request`, not the client's original bytes. this means that the routing prefix is stripped from the original key, presentation flags (q,O,k) are removed, and the flag order is normalized. removed flags are stored in `MetaReplyPlan` and reapplied when encoding the reply back to the client
2. **reply matching is positional** - no opaque tokens are sent to the backend. the connection actor matches replies on a FIFO of `MetaReplyExpectation`s, with tombstones keeping alignment across per-request timeouts
3. **clients see strict request order** - replies may complete out of order (thru fanout or failovers), but the frontend reserializes thru sequence-numbered slots before writing

## divergences from mcrouter

| area          | mcrouter                                  | rusty-mcrouter                              |
|---------------|-------------------------------------------|---------------------------------------------|
| protocol      | ascii + binary + meta                     | meta only, on both legs                     |
| runtime       | libevent + folly fibers                   | tokio, thread-per-worker, thread-local `Rc` |
| route types   | full zoo: shadow, prefix, AllSync, WarmUp | pool, hash, failover, selection so far      |
