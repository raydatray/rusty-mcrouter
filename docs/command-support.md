# Command support: scope and plan

Which memcached ASCII commands rusty-mcrouter will accept, with the rationale
for each inclusion or omission. mcrouter is the yardstick — if mcrouter
doesn't speak a command, rusty-mcrouter won't either, except where noted.

The canonical source of truth for what mcrouter accepts is its Carbon IDL
(`mcrouter/lib/network/Memcache.idl` + `Common.idl`) and the Ragel grammar
(`mcrouter/lib/network/McAsciiParser.rl`). The canonical memcached spec is
[`memcached/doc/protocol.txt`](https://github.com/memcached/memcached/blob/master/doc/protocol.txt).
Paths to mcrouter source are noted without clickable links; the upstream repo
is checked out elsewhere.

## TL;DR

| Category | mcrouter | rusty (now) | rusty (planned) |
|---|---|---|---|
| Retrieval | `get` `gets` `gat` `gats` `metaget` | `get` | `get` `gets` `gat` `gats` |
| Storage | `set` `add` `replace` `append` `prepend` `cas` | `set` | `set` `add` `replace` `append` `prepend` `cas` |
| Deletion | `delete` | — | `delete` |
| Counters | `incr` `decr` | — | `incr` `decr` |
| TTL | `touch` | — | `touch` |
| Modifier | `noreply` | parser rejects | `noreply` (server-side) |
| Router admin | `version` `quit` `stats` `flush_all` `shutdown` `exec` `flushre` `goaway` | — | `version` `quit` |
| FB-specific | `lease-get` `lease-set` `flushre` `exec` `shutdown` `goaway` `metaget` | — | — |
| Meta protocol | — | — | — |

Target: **16 commands total (14 new) + `noreply` modifier**. Everything
beyond that is explicitly out of scope, with reasons recorded below.

---

## Currently supported

[`Request`](../rusty-mcrouter-protocol/src/request.rs) variants and
[`Reply`](../rusty-mcrouter-protocol/src/reply.rs) variants today:

| Request | Wire format | Reply variants the parser already understands |
|---|---|---|
| `Get { keys }` | `get <key>+\r\n` | `Get { hits }` via `VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n` |
| `Set { key, flags, exptime, data }` | `set <key> <flags> <exptime> <bytes>\r\n<data>\r\n` | `Stored` / `NotStored` / `Exists` / `NotFound` / `Error` / `ClientError(msg)` / `ServerError(msg)` |

The reply parser already classifies the full set of single-line storage
replies (`STORED` / `NOT_STORED` / `EXISTS` / `NOT_FOUND` / `ERROR` /
`CLIENT_ERROR` / `SERVER_ERROR`) into the corresponding `Reply` variants —
see [`classify_first_line`](../rusty-mcrouter-protocol/src/parser.rs).
The `VALUE` parser also silently tolerates the extra `<cas_unique>` field
on `gets` replies (see the comment at
[`parser.rs:276`](../rusty-mcrouter-protocol/src/parser.rs)).

Several upcoming commands therefore ship reply-side handling almost for
free — the gap is mostly on the request side (parse + serialize + route
arm coverage) and on adding the few genuinely new reply variants
(`Deleted`, `Touched`, `Numeric(u64)`, `Version(Bytes)`).

---

## Tier 1 — Data plane, single-line or set-shape

Eight commands. None invent a new wire-format pattern; each is either a copy
of an existing shape with a different verb, or a single-line command/reply.

### `delete`

Wire: `delete <key> [noreply]\r\n` → `DELETED\r\n` or `NOT_FOUND\r\n`.

New `Request::Delete { key }`, new `Reply::Deleted` variant (re-uses existing
`Reply::NotFound`). Smallest possible new command — best template for the
rest of Tier 1.

### `add`, `replace`

Wire: identical to `set`, just a different verb. Both share
`<cmd> <key> <flags> <exptime> <bytes>\r\n<data>\r\n` and the same reply set
(`STORED` / `NOT_STORED` / `EXISTS` / `NOT_FOUND`). `add` stores only if the
key is absent; `replace` stores only if it's present.

Decision required: whether to factor the storage header parser/serializer
across all six storage commands now, or duplicate per-variant. See the
"Storage variant factoring" decision below.

### `append`, `prepend`

Same wire as `set`. Per the spec, the `flags` and `exptime` fields are
ignored by the server — they update existing data only. The router still
parses and forwards them faithfully; semantics are the backend's problem.

### `incr`, `decr`

Wire: `incr <key> <delta> [noreply]\r\n` → `<value>\r\n` or `NOT_FOUND\r\n`.

New `Request::Incr { key, delta }` / `Request::Decr { key, delta }`. New
`Reply::Numeric(u64)` variant (single line containing just the new value).
`delta` is a 64-bit unsigned integer per the spec; underflow on `decr`
clamps to 0, overflow on `incr` wraps — both enforced by the backend, not
the router.

### `touch`

Wire: `touch <key> <exptime> [noreply]\r\n` → `TOUCHED\r\n` or `NOT_FOUND\r\n`.

New `Request::Touch { key, exptime }`, new `Reply::Touched` variant.

Note that mcrouter classifies `touch` in `no_group` rather than `update`
(see the `routing_groups` block at the bottom of
`mcrouter/lib/network/Memcache.idl`). It's a state-modifying command that
doesn't fit either the read or write hash-routing paths cleanly. For rusty
this only matters once routing topologies beyond `PoolRoute` exist — for
Tier 1 it's just another command to parse and forward.

---

## Tier 2 — Retrieval/storage extensions (changes the type system)

Four commands. Three of them (`gets`, `cas`, `gats`) thread a 64-bit
CAS token through the request and/or reply types; one (`gat`) is the
TTL-on-fetch sibling of `get` without CAS. All four reuse the
`get`/`set` wire-shape families with one extra field.

### `gets`

Wire: `gets <key>+\r\n` → `VALUE <key> <flags> <bytes> <cas_unique>\r\n<data>\r\n... END\r\n`.

Same request shape as `get`, different reply: each `VALUE` line carries a
trailing `<cas_unique>` token. Two design options:

- Separate `Request::Gets { keys }` variant + extend `Value` with
  `cas_unique: Option<u64>`. The parser populates the field only on `gets`
  responses.
- Unify with `Request::Get { keys, with_cas: bool }` + the same `Value`
  extension. One bool in flight; one fewer variant.

The current style across the codebase is one-variant-per-command (see the
`Request` enum), so the separate variant matches existing conventions
better. Either works.

### `cas`

Wire: `cas <key> <flags> <exptime> <bytes> <cas_unique> [noreply]\r\n<data>\r\n`
→ same reply set as `set` plus `EXISTS\r\n` (cas mismatch).

`Request::Cas { key, flags, exptime, data, cas_unique }`. `Reply::Exists`
already exists in the enum — currently it's a dead variant from the parser
side (since no command in Tier 0 can produce it). cas activates it.

### `gat`, `gats`

Wire: `gat <exptime> <key>+\r\n` / `gats <exptime> <key>+\r\n` → same as
`get` / `gets`. New request variants carrying an `exptime` ahead of the key
list; replies share the existing `VALUE ... END` machinery.

---

## Tier 3 — `noreply` modifier (breaks the 1:1 request/reply invariant)

`noreply` is an optional trailing token on every write command in Tier 1
and `cas` (so: `set` / `add` / `replace` / `append` / `prepend` / `cas` /
`delete` / `incr` / `decr` / `touch`). When set, the server processes the
request but writes no reply.

This is the only Tier 1/2 feature that genuinely changes the protocol
shape. Specifically it affects:

- **Server-side**: [`serve_session`](../rusty-mcrouter-net/src/server.rs)
  must skip the reply serialize/write step when `noreply` is set on the
  parsed request. The reply loop currently writes one reply per request
  unconditionally.
- **Client-side**: the router never originates `noreply` requests; it
  forwards what came in from a client. The current
  [`Client::send`](../rusty-mcrouter-net/src/client.rs) signature
  (`&mut self -> Result<Reply, ...>`) waits for a reply. Two reasonable
  policies:
  1. **Strip `noreply` on forward**: server parses `noreply`, suppresses
     the client-facing reply, then sends the request to the backend
     *without* the `noreply` flag and waits for the backend reply
     normally. Simplest correct behavior; preserves error visibility.
  2. **Forward `noreply` as-is**: pass the flag through. Faster (no wait
     for backend ack) but the router loses error visibility and would
     need to change `Client::send` to support no-reply requests.

  Option 1 is what real mcrouter does on forwarding; recommended.

Per-variant `noreply: bool` field on each affected `Request` variant.
Cross-cutting wrapper types (`Request::NoReply(Box<Request>)`) lose
compile-time guarantees that `noreply` only attaches to commands that
accept it.

---

## Tier 4 — Router admin (intercept, do not forward)

### `version` and `quit`

Trivial. `version` returns `VERSION rusty-mcrouter <version>\r\n`. `quit`
closes the session. Both are intercepted at the server layer and never
hit a route. Three lines of code each.

### `flush_all`

Wire: `flush_all [<delay>] [noreply]\r\n` → `OK\r\n`.

Trivial *parser*; non-trivial *router policy*. mcrouter's `McFlushAllRequest`
exists; the routing question is how to dispatch a flush to *every* backend
in *every* pool, since there's no single hash target. Deferred until there's
a use case — the parser can return `Reply::ServerError("flush_all not
supported")` for now.

### `stats`

Wire: `stats [<args>]\r\n` → multiple `STAT <name> <value>\r\n` lines + `END\r\n`.

Distinct from "protocol completeness" — needs router-internal stats
accounting (connection counts, request rates, per-pool latencies, etc.).
That's an observability project, not a parser project. Deferred.

---

## Explicitly out of scope

### mcrouter-specific (not in canonical memcached)

These appear in `Memcache.idl` / `Common.idl` but are Facebook protocol
extensions that real memcached servers don't speak. Implementing them
doesn't improve memcached compatibility; it just expands the proprietary
surface.

| Command | Type | Why skip |
|---|---|---|
| `lease-get` / `lease-set` | `McLeaseGetRequest` / `McLeaseSetRequest` | Meta's dogpile-prevention primitive; only useful against Meta's memcached fork |
| `flushre` | `McFlushReRequest` | Regex-based flush; mcrouter-internal admin |
| `exec` / `admin` | `McExecRequest` | mcrouter admin shell; no analog in our config |
| `shutdown` | `McShutdownRequest` | mcrouter-specific control plane |
| `goaway` | `GoAwayAcknowledgement` | mcrouter's graceful-shutdown handshake; needs the full connection-lifecycle machinery to be meaningful |
| `metaget` | `McMetagetRequest` | mcrouter's older metadata-inspection command (not the new memcached meta protocol); near-zero real-world traffic |

### Not in mcrouter either

These are in modern memcached but mcrouter predates them or chose not to
support them. Adding them to rusty-mcrouter would diverge from mcrouter
without a clear win.

| Feature | Why skip |
|---|---|
| Meta protocol (`mg` / `ms` / `md` / `me` / `ma`) | Whole parallel protocol surface with flag-token responses, opaque IDs, recache-win semantics. mcrouter has no IDL for it. ~50% of `protocol.txt` covers this; would dominate the parser work. |
| Binary protocol | mcrouter has its own efficient binary (Caret); plain memcached binary is legacy and slated for removal upstream |
| UDP protocol | mcrouter is TCP-only |
| SASL auth | mcrouter does cleartext on a trusted internal network; SASL is the wrong abstraction here |

### Server-tuning ops (not useful through a router)

Memcached exposes these as server commands; they tune the *server*'s
internal behavior. Forwarding them to one backend at random is meaningless
and broadcasting them is a different kind of admin tool.

- `stats slabs`, `stats settings`, `stats sizes`, `stats conns`, ~12 more subcommands
- `slabs reassign` / `slabs automove`
- `lru` / `lru_crawler` family
- `cache_memlimit`
- `verbosity`

---

## Side-by-side: mcrouter vs rusty-mcrouter (planned)

Full enumeration. ✓ = supported; ✗ = explicitly not supported.

| Command | mcrouter | rusty (planned) | Notes |
|---|---|---|---|
| `get` | ✓ | ✓ (have) | |
| `gets` | ✓ | ✓ | CAS token in reply |
| `gat` | ✓ | ✓ | get + touch |
| `gats` | ✓ | ✓ | gat + CAS |
| `metaget` | ✓ | ✗ | mcrouter-internal, near-zero usage |
| `set` | ✓ | ✓ (have) | |
| `add` | ✓ | ✓ | |
| `replace` | ✓ | ✓ | |
| `append` | ✓ | ✓ | |
| `prepend` | ✓ | ✓ | |
| `cas` | ✓ | ✓ | CAS token in request |
| `delete` | ✓ | ✓ | |
| `incr` | ✓ | ✓ | |
| `decr` | ✓ | ✓ | |
| `touch` | ✓ | ✓ | |
| `lease-get` | ✓ | ✗ | Facebook extension |
| `lease-set` | ✓ | ✗ | Facebook extension |
| `version` | ✓ | ✓ | intercept at server |
| `quit` | ✓ | ✓ | intercept at server |
| `flush_all` | ✓ | ✗ (deferred) | router-policy decision |
| `stats` | ✓ | ✗ (deferred) | needs router-internal stats |
| `shutdown` | ✓ | ✗ | mcrouter-specific |
| `exec` / `admin` | ✓ | ✗ | mcrouter admin shell |
| `flushre` | ✓ | ✗ | Facebook extension |
| `goaway` | ✓ | ✗ | mcrouter graceful-shutdown |
| `noreply` modifier | ✓ | ✓ | server-side suppress + strip on forward |
| Meta protocol (`mg`/`ms`/`md`/`me`/`ma`) | ✗ | ✗ | not in mcrouter |
| SASL auth | ✗ | ✗ | not in mcrouter |
| UDP | ✗ | ✗ | not in mcrouter |
| Binary protocol | ✗ | ✗ | not in mcrouter |

---

## Decisions to make before Tier 1

These shape the diff and the public types; worth settling once.

### 1. Storage variant factoring

Six storage commands (`Set` / `Add` / `Replace` / `Append` / `Prepend` /
`Cas`) share the same wire header `<cmd> <key> <flags> <exptime> <bytes>
[<cas_unique>] [noreply]\r\n<data>\r\n`. Two options:

- **Per-command variants**: `Request::Set { ... }` / `Request::Add { ... }`
  / etc. Matches the existing convention (one variant per command); makes
  pattern-matching downstream read cleanly; six near-identical bodies in
  the request enum.
- **Unified storage variant**: `Request::Storage { op: StorageOp, key,
  flags, exptime, data, cas_unique: Option<u64>, noreply: bool }`. Less
  duplication; every consumer matches on `op` instead of the outer variant.

Recommendation: per-command variants. The current style is already that,
and the duplication is regular enough that future shared logic can hang
off a small `StorageOp` enum if needed.

### 2. `get` / `gets` representation

Same wire shape, different reply (CAS token). Options: separate variants
vs `Request::Get { keys, with_cas: bool }`. Both are defensible. Separate
variants match the rest of the codebase; the bool form is slightly
smaller.

Recommendation: separate variants for consistency with #1.

### 3. `noreply` placement

Per-variant `noreply: bool` field on each affected request. Cross-cutting
wrappers (`Request::NoReply(Box<Request>)`) lose compile-time guarantees
that the modifier only attaches to commands that accept it.

Recommendation: per-variant field.

### 4. `noreply` forward policy

Strip on forward (option 1 above) or pass through (option 2). Strip is
what mcrouter does and preserves error visibility on the router. Pass-through
needs `Client::send` to grow a no-reply path.

Recommendation: strip on forward.

---

## Recommended order

1. **`delete` first** — smallest new command, exercises the full stack
   (parser variant + serializer variant + `Request::Delete` + `Reply::Deleted` +
   `NullRoute` arm + integration test). Template for the rest of Tier 1.
2. **Storage family** — `add`, `replace`, `append`, `prepend`. Decide #1
   above when implementing the first of these; refactor `set` to the new
   shape at the same time.
3. **Counters and touch** — `incr`, `decr`, `touch`. Three small, similar
   single-line shapes.
4. **Tier 1 done**. Pause and reassess whether routing work
   (`HashRoute`, prefix routing, named_handles resolution) is the right
   next step before committing to Tier 2.
5. **CAS family** — `gets`, `cas`, `gat`, `gats` as one feature (the CAS
   token threads through both request and reply types simultaneously).
6. **`noreply`** — server-side parse + reply suppression, strip-on-forward
   policy on the client side.
7. **`version` and `quit`** when convenient — trivial, no real ordering
   constraint.
8. **`flush_all` and `stats`** if and when a use case appears.
