# Caching agent work

> Every other page here makes a byte cheaper. This one makes a whole model call
> disappear, which is worth about 10,000× more.

---

## Copy this

```ts
import { connect, AgentCache } from './src/kit/index.ts';

const redis = await connect();
const cache = new AgentCache(redis, {
  prefix: 'llm',
  capacity: 1_000_000,
  ttl: 3600,
  compress: true,
  semantic: { dim: 1536, threshold: 0.92 },   // optional, catches paraphrases
});

const res = await cache.fetch(promptKey, async () => callTheModel(prompt), embedding);
res.value;   // Buffer - from cache, or freshly computed
res.kind;    // 'exact' | 'semantic' | 'miss'
```

`fetch` is the one you want. On a miss, **exactly one worker in your entire
fleet computes**; everyone else waits for that result instead of calling the
model too.

---

## The economics

Measured: **95 B per cached entry** (1.3 KB payload, sharded hash + `HEXPIRE` +
zstd) versus 1,634 B for a key-per-entry cache. That's **17.1×**.

But the RAM is not really the point:

| | |
|---|--:|
| RAM cost of one cached entry | ~$0.0000011 / month |
| Value of one cache hit (20k-token call at $3/M) | ~$0.06 |

**An entry pays for itself if it is hit once every 50,000 months.** So the
number to optimize is hit *rate*, not cache size - and the only reason to
optimize the RAM at all is that cheap entries let you cache far more
aggressively and keep them far longer.

---

## Stampede protection is the whole feature

A popular prompt expires. 200 workers miss it in the same millisecond. Without
protection that is 200 model calls - a 200× bill spike and, usually, a
rate-limit incident on top.

The naive fix (`GET`, then `SETNX` if missing) has a race window in which every
worker loses and every worker calls the model anyway. nibble does the lookup and
the lease claim in **one Lua script**, so the decision is atomic:

```lua
local v = redis.call('HGET', KEYS[1], ARGV[1])
if v then return {1, v} end                                   -- hit
if redis.call('SET', KEYS[2], ARGV[3], 'NX', 'PX', ARGV[2]) then
  return {2, ''}                                              -- you compute
end
return {3, ''}                                                -- someone else is
```

Measured with 200 workers on 200 separate connections, all missing the same key:

```
✔ the model was called 1 time(s), not 200
  1 computed, 199 waited on the lease, 0 lease timeouts
✔ all 200 workers got a value back
  p50 170 ms · p99 171 ms · wall 173 ms
```

Every waiter got the answer in about the time of the single underlying call.

**If the winner dies**, the lease has a TTL, so the next caller takes over
rather than the key being wedged forever. Measured recovery: 1 ms after lease
expiry.

### A race worth knowing about

The waiter loop checks *value, then lease*. If the winner publishes between
those two reads, a waiter sees "no value, no lease" and concludes the winner
died - so it computes too.

Under 200-way concurrency that hit exactly one worker in testing, and it is why
`fetch` re-checks the value once more after finding the lease gone. If you write
your own lease, this is the bug you will have.

---

## Synchronized expiry

Ten thousand entries written during a deploy share a TTL, so they all expire in
the same second and you get the stampede again - on everything, at once.

`AgentCache` jitters every TTL by ±15% (`jitter` option), so a batch written
together decays over a window instead of a cliff. It costs nothing.

---

## The semantic tier

Agents do not send byte-identical prompts. `"what's the weather in Paris"` and
`"weather in paris?"` are the same request and a different cache key, so an
exact-key cache on natural language has a **0% hit rate** on paraphrases by
construction.

Pass an embedding and nibble checks exact first (one round trip, cannot be wrong),
then falls back to a vector search over cached prompts:

```ts
semantic: { dim: 1536, threshold: 0.92, quant: 'Q8' }
```

Measured on 400 cached prompts:

| | |
|---|--:|
| paraphrased prompts caught | **200 / 200** |
| unrelated prompts wrongly served | **0 / 100** |

The index uses int8-quantized vectors (`Q8`) - see
[embeddings.md](embeddings.md) for why that's the right quantization and binary
is not.

**Tune the threshold against your own traffic.** Too low and you will serve a
confidently wrong answer to a different question, which is far worse than a
miss. Start at 0.92, measure, and treat any drop below ~0.9 as needing evidence.

---

## Distributed at scale

Everything for one logical entry - the value, the lease, the semantic index -
shares a `{hash tag}`, so it lands in one cluster slot. Without that, Redis
Cluster rejects the Lua script outright, because it refuses multi-key operations
that span slots.

```
llm:e:{7}          the shard holding the value
llm:l:{7}:1a2b3c   the lease for a key in that shard
```

Practical consequences at scale:

- **Shard count is fixed at construction** from `capacity`. Changing it
  re-routes every key, which is a cache flush, not a migration. Over-provision.
- **The semantic index is a single key** (`llm:v`), so it does not shard with
  the entries. Above a few million vectors, partition it by tenant or route
  yourself - this is the main scaling limit of the semantic tier.
- **Entries carry a TTL, so they are evictable** under `volatile-ttl` while your
  durable records are not. That is exactly the separation
  [production.md](production.md#eviction) is built around: cache and primary
  data can share an instance safely, because eviction follows the TTL.

---

## When not to cache

- **Anything non-deterministic that the user can tell is stale.** A cached
  "current time" answer is a bug.
- **Per-user private data on a semantic key.** Similarity does not respect
  tenancy - include the tenant in the cache key, and use a separate semantic
  index per tenant, or you will serve one customer another's answer.
- **Cheap calls.** The break-even above assumes an expensive model call. Caching
  a 2 ms local computation just adds a round trip.
