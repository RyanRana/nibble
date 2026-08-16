# nibble docs

**Find your use case. Copy the snippet. Done.**

Every snippet below runs against a plain Redis with no install and no build
step — nibble has zero dependencies. Every number is measured on Redis 8.10 and
reproducible with `bash scripts/run-all.sh`.

```bash
node doctor.mjs           # first: what is YOUR Redis actually wasting?
```

---

## Pick your use case

| I'm storing… | Use | Cost | Was |
|---|---|--:|--:|
| [agent runs / sessions / checkpoints](records.md) | `Pouch` + `schema()` | **~70 B** | 401 B |
| [conversation history](packing.md#when-to-compress-instead) | `LIST` + dict-deflate | **~595 B/turn** | 2,010 B |
| [tool-call spans / traces](records.md#the-tax) | `LIST` of packed spans | **~37 B/span** | 346 B |
| [embeddings / semantic memory](embeddings.md) | `VectorMemory` (`Q8`) | **~2.3 KB** | 32 KB |
| [**LLM / tool-call responses**](caching.md) | `AgentCache` | **~95 B** | 1,634 B |
| [cached tool results (with TTL)](records.md#what-it-costs-you) | `ShardedCache` | **~160 B** | 277 B |
| ["have I already done this?"](records.md) | `ExactSeen` / `BloomSeen` | **9 B / 1.4 B** | 108 B |
| [a task queue](production.md) | `LIST` of packed tasks | **~55 B/task** | 217 B |
| [latency / cost percentiles](metrics.md) | TimeSeries, or t-digest | **2.1–2.6 B/sample** | 88.5 B |
| [counters over time](metrics.md) | TimeSeries, integer values | **2.1 B/sample** | 88.6 B |
| [an index to query by](production.md#querying) | hybrid index doc | **~520 B/run** | 1,058 B |

---

## Instant setup

Two lines, once, at startup. This is the entire configuration story.

```ts
import { connect, applyKitConfig, applySafeEviction } from './src/kit/index.ts';

const redis = await connect();                    // reads REDIS_URL, else localhost
await applyKitConfig(redis, { shardWidth: 124 }); // listpack thresholds
await applySafeEviction(redis, 8 * 1024 ** 3);    // durable records can't be evicted
```

`applySafeEviction` is the important one. Under `allkeys-lru` — the policy every
cache tutorial recommends — filling an instance destroyed **99.8% of durable
records while reporting success on every single write**. Under `volatile-ttl` it
destroyed none, because a key with no TTL is never an eviction candidate.

---

## The 30-second version of each technique

**Store many records in one key.** A Redis key costs 60–90 B before your data.
124 records per hash pays that once instead of 124 times. → [records.md](records.md)

```ts
const runs = new Pouch(redis, { prefix: 'run', capacity: 2_000_000, width: 124,
                                encode: Run.encode, decode: Run.decode });
await runs.set(id, run);
```

**Stop shipping field names.** The reader knows the order; JSON tells it anyway,
20 times per record. → [packing.md](packing.md)

```ts
const Run = schema({ run_id: uuid(), status: enum_(['queued','running']), steps: varint() });
Run.encode(run);   // 43 B.  As JSON: 258 B.
```

**Round metrics to integers.** Gorilla compression XORs consecutive float64s;
`123` has a short stable mantissa, `123.45` has a long noisy one. Same data,
**8.6 → 2.1 B/sample**. → [metrics.md](metrics.md)

**Quantize embeddings to int8.** Held 98.7–100% recall across a full sweep of
corpus difficulty. Binary ranged 83% → 32% on the same code. → [embeddings.md](embeddings.md)

**Sweep your shard width.** 124 fields/shard cost 167 B/entry; 128 cost 193 B.
One constant, ~15%. → [tuning.md](tuning.md)

---

## Reference

| | |
|---|---|
| [caching.md](caching.md) | LLM response caching — stampedes, semantic hits, cluster |
| [records.md](records.md) | sharding — the biggest single win (15.4×) |
| [packing.md](packing.md) | the schema DSL, compression, and migrating a schema safely |
| [embeddings.md](embeddings.md) | quantization, and how to not fool yourself about recall |
| [metrics.md](metrics.md) | percentiles and time series — including when a sketch *loses* |
| [tuning.md](tuning.md) | allocator size classes, shard width, which bytes actually matter |
| [production.md](production.md) | durability, eviction, atomicity, querying |
| [measuring.md](measuring.md) | the harness, and why `used_memory_dataset` is a trap |

---

## Before you adopt any of it

nibble is worth the complexity only when **all three** hold: millions of records,
RAM is your binding constraint, and you control the access patterns.

Under ~1M records, 8.6× of 2 GB saves ~$20/month and costs you a codec to
maintain forever. Run the doctor, fix anything it marks CRITICAL, and walk away.
It will tell you so itself:

```
Verdict: this instance is small enough that layout work is not worth the
complexity. Fix anything marked CRITICAL, ignore the rest until you are RAM-bound.
```
