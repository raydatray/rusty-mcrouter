# mcrouter consistent hashing (Ch3 / furc_hash)

how Meta's mcrouter maps a key to one backend in a pool: the route handle that
does the selection, the family of hash functions it can use, and — in detail —
the default consistent hash (`Ch3`, built on `furc_hash`), why it's *consistent*
(adding a server moves only ~1/N of keys), and how a key is reduced to the
bytes that actually get hashed.

> Source pinned to `facebook/mcrouter` @ `42aa391189c7`. Citations are by symbol
> and file path (relative to the repo root); line numbers drift, symbols don't.
> This is reference-only — no rusty-mcrouter content. See
> [`../design/hash-routing.md`](../design/hash-routing.md) for what we copy and
> `../architecture/hash-routing.md` for what we end up building. For the route
> handle layer this sits inside, see [`threading-model.md`](./threading-model.md).

---

## tl;dr

- A pool is just an ordered list of destinations. To pick one for a request,
  mcrouter wraps the destinations in a **hash route handle** that computes
  `index = hashFunc(routingKey, numChildren)` and forwards to `children[index]`.
- There is **no dedicated `HashRoute` class**: it is
  `SelectionRoute<RouterInfo, HashSelector<HashFunc>>`. `HashSelector::select`
  computes the index; `SelectionRoute::select` indexes the children.
- The **default hash function is `Ch3`** ("consistent hash, version 3"), a thin
  wrapper over **`furc_hash`** — Facebook's consistent hash. `furc_hash(key, len, m)`
  returns a bucket in `[0, m)` such that **increasing `m` from `k` to `k+1`
  reassigns only ~`1/(k+1)` of keys** and leaves the rest where they were.
- `furc_hash` derives its bits from **MurmurHash64A** (re-hashed for more bits as
  needed) and walks a **binary decision tree**. Max pool size is
  `1 << FURC_SHIFT = 1 << 23 = 8,388,608`.
- Other selectable `hash_func`s: `Crc32` (fast, **not** consistent),
  `WeightedCh3` (+ per-server weights), `ConstShard`, `Rendezvous`/
  `WeightedRendezvous`, and the route-strategy shims `Latest` / `LoadBalancer`.
- mcrouter hashes the **routing key**, not the raw key: a leading
  `/region/cluster/` routing prefix is stripped, and everything from `|#|`
  onward is excluded. An optional `salt` is mixed in before hashing.

---

## where it sits: SelectionRoute + HashSelector

`PoolRoute` (and the explicit `HashRoute` form) desugar to a `SelectionRoute`
parameterized by a `HashSelector`. The selector returns a child index; the
selection route forwards to that child (`mcrouter/lib/routes/SelectionRoute.h`,
`SelectionRoute::select`):

```cpp
size_t idx = selector_.select(req, children_.size());
if (idx >= children_.size()) { /* error */ }
return *children_[idx];
```

`HashSelector::select` is where the key becomes an index
(`mcrouter/lib/HashSelector.h`):

```cpp
size_t select(const Request& req, size_t size) const {
  return this->selectInternal(req.key_ref()->routingKey(), size);
}
// selectInternal: salt-aware
n = salt_.empty() ? hashFunc_(key) : hashWithSalt(key, salt_, hashFunc_);
```

So three things compose: **which bytes** (`routingKey()` + optional `salt`),
**which function** (`hashFunc_`, e.g. `Ch3HashFunc`), and **the bound** (`size`
= number of children).

```mermaid
flowchart LR
  REQ["request"] --> RK["routingKey() (+ salt)"]
  RK --> HF["hashFunc(key, numChildren)"]
  HF --> IDX["index in [0, numChildren)"]
  IDX --> CH["children[index]"]
  CH --> DST["one ProxyDestination → backend"]
```

---

## the hash function family

`createHashRoute` (`mcrouter/routes/HashRouteFactory.h`) is the authoritative
dispatch from the `hash_func` string to an implementation. The true hash
functions are enumerated by `HashFunctionType` (`mcrouter/lib/HashFunctionType.h`):

| `hash_func` | Consistent? | Notes |
|---|---|---|
| **`Ch3`** (default) | **yes** | `furc_hash`; the subject of this doc. `mcrouter/lib/Ch3HashFunc.h` |
| `Crc32` | no | `crc32(key) % n`; reshuffles all keys when `n` changes. `mcrouter/lib/Crc32HashFunc.h` |
| `WeightedCh3` | yes | `Ch3` + per-server weights in `[0,1]`. `mcrouter/lib/WeightedCh3HashFunc.h`, base `WeightedChHashFuncBase.cpp` |
| `WeightedCh3Rv` | yes | weighted rendezvous variant |
| `ConstShard` | n/a | key carries an explicit shard id; maps shard→index |
| `Rendezvous` / `WeightedRendezvous` | yes | highest-random-weight hashing |
| `Latest` | — | route *strategy* (sticky-to-last-good), not a literal hash |
| `LoadBalancer` | — | route *strategy* (load-aware), not a literal hash |

`Latest`/`LoadBalancer` are accepted in the same `hash_func` slot but are route
behaviors, not hash algorithms. Everything below is about `Ch3`/`furc_hash`,
because it's the default and the thing a cache router lives or dies by.

---

## furc_hash: the consistent hash

Declared in `mcrouter/lib/fbi/hash.h`, defined in `mcrouter/lib/fbi/hash.c`:

```c
uint32_t furc_hash(const char* const key, const size_t len, const uint32_t m);
```

It returns a bucket in `[0, m)`. The headline property is in the `hash.h`
comment — this is the entire reason it exists:

```c
// if |m| is increased from 11 to 12, 1/12th of keys for each output value
// [0 : 10] will be reassigned the value of 11 while the remaining 11/12th of
// keys will produce the same value as before.
```

That is consistency: grow the pool by one and only a `1/m` slice of traffic
re-homes; shrink it and the inverse. `Crc32`'s `% n` has no such property —
changing `n` remaps essentially every key, which for a cache is a near-total
miss storm.

### the algorithm (binary decision tree)

The core loop (`furc_hash`, `mcrouter/lib/fbi/hash.c`):

```c
d = 32u - (uint32_t)__builtin_clz(m - 1u);   // tree depth for m buckets
a = d;
for (try = 0; try < MAX_TRIES; try++) {
  while (!furc_get_bit(key, len, a, hash, &old_ord)) {
    if (--d == 0) {
      return 0;
    }
    a = d;
  }
  a += FURC_SHIFT;
  num = 1;
  for (i = 0; i < d - 1; i++) {
    num = (num << 1) | furc_get_bit(key, len, a, hash, &old_ord);
    a += FURC_SHIFT;
  }
  if (num < m) {
    return num;
  }
}
```

Conceptually: `d` is the number of bits needed to index `m` buckets (the tree
depth). The function reads key-derived bits to descend the tree to a leaf and
emits that leaf's index `num`. If the candidate leaf is `< m` it's the answer;
otherwise it retries (up to `MAX_TRIES`) deeper in the bitstream. Bit positions
are spaced `FURC_SHIFT` apart so successive decisions draw on independent parts
of the bit generator. A pool of size 1 always yields bucket 0.

> The exact index arithmetic (`a`, `d`, the `FURC_SHIFT` stride) is best read
> from the source; the *contract* is the consistency comment quoted above, and
> that `furc_hash(key, len, m) ∈ [0, m)` is deterministic for a given key+m.

### the bit generator: MurmurHash64A, lazily extended

`furc_get_bit` produces an effectively unbounded stream of key-derived bits by
hashing the key once with **MurmurHash64A**, then re-hashing the previous 64-bit
word whenever more bits are needed (`mcrouter/lib/fbi/hash.c`):

```c
int32_t ord = (idx >> 6);                 // which 64-bit word
...
hash[n] = ((n == 0) ? murmur_hash_64A(key, len, SEED)
                    : murmur_rehash_64A(hash[n - 1]));
...
return (hash[ord] >> (idx & 0x3f)) & 0x1; // pick the bit within the word
```

So bit index `idx` lives in word `idx >> 6`, at bit `idx & 0x3f`. Word 0 is
`murmur_hash_64A(key, len, SEED)`; each later word is
`murmur_rehash_64A(previous_word)`. The words are cached per call (`hash[]`,
tracked by `old_ord`) so each is computed at most once.

### max pool size

```c
#define FURC_SHIFT 23
uint32_t furc_maximum_pool_size(void) { return (1 << FURC_SHIFT); }  // 8,388,608
```

`furc_hash` supports up to `2^23` buckets.

---

## Ch3HashFunc

`Ch3HashFunc` (`mcrouter/lib/Ch3HashFunc.h`) is a stateless functor bound to the
pool size `n`, delegating straight to `furc_hash`:

```cpp
explicit Ch3HashFunc(size_t n) : n_(n) {
  if (!n_ || n_ > furc_maximum_pool_size()) {
    throw std::logic_error("Pool size out of range for Ch3");
  }
}
size_t operator()(folly::StringPiece hashable) const {
  return furc_hash(hashable.data(), hashable.size(), n_);
}
static const char* type() { return "Ch3"; }   // "consistent hash, version 3"
```

The constructor enforces `1 <= n <= 2^23`. The functor takes the already-prepared
`hashable` bytes (the routing key, possibly salted) and returns an index in
`[0, n)`.

## WeightedCh3 (brief)

`WeightedCh3` layers per-destination weights on top of `Ch3`. Weights are doubles
in `[0, 1]` aligned positionally to the pool's servers; missing entries default
to `0.5`, out-of-range is rejected, extras are ignored
(`mcrouter/lib/WeightedChHashFuncBase.cpp`). After `furc_hash` picks a candidate,
the weight acts as an accept/skip probability (also key-derived, so still
deterministic) — a server with weight `0.5` receives roughly half the share of a
weight-`1.0` server. Used for gradual capacity changes / draining.

## salt

If a `salt` is configured, `HashSelector` mixes it into the key before hashing
via `hashWithSalt` (`mcrouter/lib/HashUtil.h`) instead of hashing the bare key.
Two pools with the same servers but different salts therefore distribute the
same keys differently — useful to decorrelate replicas.

---

## routing key: what actually gets hashed

mcrouter does **not** hash the raw memcached key. `HashSelector` hashes
`req.key_ref()->routingKey()`, and `Keys::update()` (`mcrouter/lib/carbon/Keys-inl.h`,
layout comment in `Keys.h`) derives it:

- a leading **routing prefix** of the form `/region/cluster/` is stripped (it
  selects *where* to route, not *what* to hash);
- everything from the **`|#|`** marker onward is excluded from the routing key
  (the "hash stop" — lets callers attach a non-hashed suffix);
- what remains is the `routingKey` handed to the hash function.

```cpp
// Keys::update — split routing key at "|#|"
if (pos != std::string::npos) {
  routingKey_     = keyWithoutRoute_.subpiece(0, pos);
  afterRoutingKey_ = keyWithoutRoute_.subpiece(pos);
}
```

```mermaid
flowchart LR
  RAW["/region/cluster/user:123|#|meta"] --> P1["strip /region/cluster/ prefix"]
  P1 --> KWR["user:123|#|meta"]
  KWR --> P2["cut at |#|"]
  P2 --> RK["routingKey = user:123"]
  RK --> H["furc_hash(routingKey, n)"]
```

(No `{...}`-style hash-tag is used on this path in the pinned source; the
mechanisms are the routing prefix and the `|#|` hash stop.)

---

## config surface

The hash is configured on the **route** (the `PoolRoute`/`HashRoute` object), not
on the pool definition. `makePoolRoute` (`mcrouter/routes/McRouteHandleProvider-inl.h`)
reads `hash` from the route JSON and feeds it to `createHashRoute`;
`PoolFactory::parsePool` does not parse a hash.

`hash` may be a bare string (just the function name) or an object:

```json
// bare string form
{ "type": "PoolRoute", "pool": "A-foo", "hash": "ConstShard" }
```

```json
// object form with weights
{
  "type": "PoolRoute",
  "pool": "A.wildcard",
  "hash": {
    "hash_func": "WeightedCh3",
    "weights": [0, 1, 1.0, 0.0, 0.5, 1.0, 0.3, 0.5]
  }
}
```

Recognized keys inside the `hash` object (superset; many are route-specific):
`hash_func`, `salt`, `weights`, `tags`, `bucketize`, `client_fanout`, and the
`Latest`/`LoadBalancer` strategy knobs (`failover_count`, `load_ttl_ms`, …).

## PoolRoute desugars to a hash route

`PoolRoute` is sugar for "build the pool's destinations, then wrap them in hash
selection" (`makePoolRoute`):

```cpp
auto [destinations, weights] = makePool(factory, poolJson);
...
auto route = createHashRoute<RouterInfo>(
    jhashWithWeights, std::move(destinations), factory.getThreadId(), proxy_);
```

The mcrouter wiki states it plainly: *"PoolRoute provides the same functionality
as HashRoute."* The difference is ergonomic — `PoolRoute` names a pool;
`HashRoute` takes an explicit `children` list.

## defaults

- `hash_func` omitted → **`Ch3`** (`createHashRoute` initializes
  `funcType = Ch3HashFunc::type()` before dispatch).
- `salt` omitted → empty (hash the bare routing key).
- `WeightedCh3` with missing weight entries → `0.5` per missing server.

---

## the knobs that shape all of this

| Option | Effect |
|---|---|
| `hash_func` | Selects the function; default `Ch3` (consistent). `Crc32` is fast but non-consistent. |
| `salt` | Mixed into the key before hashing; decorrelates otherwise-identical pools. |
| `weights` (`WeightedCh3`) | Per-server share in `[0,1]`; default `0.5`; for draining / capacity changes. |
| pool server **order** | The index space. `furc_hash` returns an index into the server list, so reordering servers re-homes keys — append, don't reorder. |
| pool **size** | Bound passed to `furc_hash`; growing by one moves ~`1/N` of keys (the consistency guarantee). |

---

## source map

| Concept | Symbol | File @ `42aa391189c7` |
|---|---|---|
| Selection by index | `SelectionRoute::select` | `mcrouter/lib/routes/SelectionRoute.h` |
| Key → index (+ salt) | `HashSelector::select`, `selectInternal` | `mcrouter/lib/HashSelector.h` |
| `hash_func` dispatch | `createHashRoute` | `mcrouter/routes/HashRouteFactory.h` |
| Hash function enum | `HashFunctionType` | `mcrouter/lib/HashFunctionType.h` |
| Consistent hash | `furc_hash`, `furc_get_bit`, `FURC_SHIFT`, `furc_maximum_pool_size` | `mcrouter/lib/fbi/hash.c`, `hash.h` |
| Bit generator | `murmur_hash_64A`, `murmur_rehash_64A` | `mcrouter/lib/fbi/hash.c` |
| Default hash functor | `Ch3HashFunc` | `mcrouter/lib/Ch3HashFunc.h` |
| Non-consistent option | `Crc32HashFunc` | `mcrouter/lib/Crc32HashFunc.h` |
| Weighted variant | `WeightedCh3HashFunc`, `WeightedChHashFuncBase` | `mcrouter/lib/WeightedCh3HashFunc.h`, `WeightedChHashFuncBase.cpp` |
| Salt mixing | `hashWithSalt` | `mcrouter/lib/HashUtil.h` |
| Routing key | `Keys::update`, `Keys::routingKey` | `mcrouter/lib/carbon/Keys-inl.h`, `Keys.h` |
| PoolRoute desugar | `makePoolRoute` | `mcrouter/routes/McRouteHandleProvider-inl.h` |
| Pool parse (no hash) | `PoolFactory::parsePool` | `mcrouter/PoolFactory.cpp` |
