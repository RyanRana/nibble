<div align="center">

# █ nibble

**Redis uses less RAM than you think. nibble measures how much less - and applies it.**

</div>

---

```bash
node doctor.mjs                    # what is this Redis wasting?  read-only, ~80 ms
node doctor.mjs --fix              # show me the plan
node doctor.mjs --fix --apply      # do it, and tell me what you freed
```

That's the whole setup. No `npm install`, no build step, no config file - nibble
carries its own Redis client. `node doctor.mjs redis://host:6379` for a remote one.

It tells you what **your** instance is wasting, with the fix and the saving:

```
█ nibble doctor  Redis 8.10.0 · 63.0 MiB used · 61,001 keys · sampled 2,000 in 79 ms

Where the memory is
  emb:<uuid>    string  24.3 MiB   39% ██████████  1,769 keys · raw
  run:<uuid>    string  13.3 MiB   21% █████       19,947 keys · raw
  vec:<uuid>    string  10.5 MiB   17% ████        1,525 keys · raw

  CRITICAL  maxmemory-policy is "allkeys-lru" and ~61,000 keys have no TTL
    SAVING  emb:<uuid> - embeddings stored as JSON arrays (1536 dims)   ~19.2 MiB
    SAVING  run:<uuid> - 45% of these JSON bytes are field names        ~5.0 MiB
   WARNING  tasks group "g" has 30,000 un-acked messages                 ~6.8 MiB
```

On a small or already-tidy instance it says so and exits - it is not trying to
sell you the library:

```
  ✔ Nothing worth changing. This instance is configured safely and laid out well.
```

---

## What am I storing?

Find your row, copy the snippet, done. Full index: **[docs/](docs/README.md)**

| I'm storing… | Use | Cost | Was |
|---|---|--:|--:|
| [agent runs / sessions](docs/records.md) | `Pouch` + `schema()` | **~70 B** | 401 B |
| [conversation history](docs/packing.md) | `LIST` + dict-deflate | **~595 B/turn** | 2,010 B |
| [tool-call spans](docs/records.md) | `LIST` of packed spans | **~37 B/span** | 346 B |
| [embeddings](docs/embeddings.md) | `VectorMemory` (`Q8`) | **~2.3 KB** | 32 KB |
| [**LLM responses**](docs/caching.md) | `AgentCache` | **~95 B** | 1,634 B |
| [cached tool results](docs/records.md) | `ShardedCache` + `HEXPIRE` | **~160 B** | 277 B |
| ["seen this already?"](docs/records.md) | `ExactSeen` / `BloomSeen` | **9 B / 1.4 B** | 108 B |
| [task queues](docs/production.md) | `LIST` of packed tasks | **~55 B/task** | 217 B |
| [latency percentiles](docs/metrics.md) | TimeSeries (integer ms) | **2.1 B/sample** | 88.6 B |
| [something to query](docs/production.md) | hybrid index doc | **~520 B/run** | 1,058 B |

```
919 GiB  →  107 GiB      8.6×      same workload, same queries, same answers
 $11.9k  →   $1.4k /mo   at ElastiCache list, 25% reserve
```

---

## Three principles

**1. Small shards.**
jemalloc doesn't charge you for bytes, it charges you for size classes. A shard
of 124 records measured 166.6 B/entry; the same data at 128 measured 192.5 B.
Make your allocation land *just under* a step, not just over.

**2. Field names are the payload.**
Twenty fields of JSON is ~180 bytes of key names and ~60 bytes of data. The
reader already knows the order. Stop telling it.

**3. Nothing is free - say what it costs.**
Every technique here takes something: a codec to migrate, a TTL you can't set, a
recall point you can't get back. A benchmark that only reports wins is an ad.

---

## Using the primitives

Also zero install - the kit carries its own ~150-line Redis client:

```bash
git clone … && cd nibble
node src/kit/smoke.ts      # every snippet in these docs, actually executed
```

`REDIS_URL` if it isn't on localhost. Node ≥ 22 runs the TypeScript directly.

```ts
import { connect, applyKitConfig, applySafeEviction } from './src/kit/index.ts';

const redis = await connect();
await applyKitConfig(redis, { shardWidth: 124 });
await applySafeEviction(redis, 8 * 1024 ** 3);
```

Only the **benchmark and proof suite** has dependencies (`ioredis`,
`@msgpack/msgpack`) - it needs a battle-tested client and a comparison encoding,
and it is not what you deploy. `pnpm install` only if you want to re-run the
measurements.

---

## What it costs, per component

Measured on Redis 8.10, jemalloc 5.3, extrapolated to 2M live runs / 80M turns /
200M spans / 20M memories / 500M dedup keys a day. 12 benchmark cases,
~140 variants - full tables with `OBJECT ENCODING` per row in `results/bench.md`.

| Component | Naive | Optimized | Factor |
|---|--:|--:|--:|
| Telemetry | 41.3 GiB | 1.0 GiB | **42.6×** |
| Agent run state | 4.2 GiB | 0.3 GiB | 15.3× |
| Secondary indexes | 6.1 GiB | 0.4 GiB | 14.7× |
| Semantic memory | 611.2 GiB | 42.7 GiB | 14.3× |
| Tool-call spans | 64.5 GiB | 6.8 GiB | 9.5× |
| Idempotency / dedup | 25.2 GiB | 4.2 GiB | 6.0× |
| Task queue | 0.4 GiB | 0.1 GiB | 3.8× |
| Conversation transcripts | 153.5 GiB | 44.3 GiB | 3.5× |
| Tool-result cache | 12.9 GiB | 7.5 GiB | 1.7× |
| **Total** | **919 GiB** | **107 GiB** | **8.6×** |

Full per-variant tables with `OBJECT ENCODING` for every row: `results/bench.md`.

---

## Things we learned

**The two biggest wins are mutually exclusive.** Redis 8.10's hash templates
store field names once across hashes sharing a schema - worth 1.78× (729 → 409
B/run) for one-hash-per-record. Sharding is worth more (147 B/run). But a
shard's field names are unique record ids, so there's no schema to share and the
template becomes pure overhead: **70.1 → 84.4 B/record, a 20% regression.** Pick
one. We pick sharding, and `applyKitConfig` explicitly turns templates *off*.

**Shard width is worth 15% and nobody tunes it.** One constant. Sweep it.

**Half a percent of your traffic can cause an 11× error.** Agent tool calls are
bimodal - a body of normal latencies plus calls that hit the timeout - and p99
lands exactly on the cliff between them. Same t-digest, same compression, only
the input changes: **p99 error 0.95% on clean data, 10.32% with a 0.5% timeout
spike, 0.89% once timeouts are counted separately.** Exclude timeouts from the
digest and count them exactly; both numbers get better.

**A sketch is not automatically the cheap answer.** A t-digest allocates its
full size at `CREATE` and never grows - verified: 9,864 B before *and* after
100,000 adds. At 5,000 samples/series it ties RedisTimeSeries (2.1 vs 2.6
B/sample), and TimeSeries is *exact*. The break-even is ~4,700 samples; below
that the sketch just loses.

**Round your metrics to integers.** Gorilla compression XORs consecutive
float64s. `123` has a short stable mantissa; `123.45` has a long noisy one. Same
random walk, same series: **8.6 → 2.1 bytes/sample.**

**Compression and schema-packing are substitutes, not complements.** Packing
already removed the redundancy. zstd on a 110 B packed record made it *bigger*.

**Streams already dedupe field names** across entries sharing a fieldset - the
first entry acts as a master and later ones store only values. So the intuitive
optimization backfires: stuffing a span into one MessagePack field cost **250.9
B/entry**, while plain field-per-attribute cost **168.2 B**. Pack into a *schema*
(86.8 B) or don't pack at all, but don't reach for a blob.

**The allocator's cliff isn't the 44-byte embstr limit** everyone repeats. In
Redis 8 it's `16 + (keylen+3) + (4+vallen) ≤ 64` - key length and value length
share one budget. Short key names buy you longer embedded values.

**`WAITAOF` under `everysec` is a trap.** It waits for the *next scheduled*
fsync instead of triggering one: **201 ops/s**. Under `always`, the same pattern
sustained ~138k ops/s.

---

## Things that didn't work

Kept in the repo on purpose.

- **`REDUCE 256` + binary vectors** - *larger* than plain binary. At 256 dims
  the projection matrix costs more than the dimensions it saves. Once you're at
  binary the HNSW graph is ~80% of the cost and there's nothing left to squeeze.
- **Bitmaps for sparse indexes** - a bitmap costs the same at 2% density as at
  100%. It lost to sharded intsets at 2% (9.4 vs 4.4 B/member) and won by 20× at
  60%. The crossover is measured, not assumed.
- **Compressing packed records** - see above.
- **MessagePack as a general answer** - it drops the quotes and braces but still
  ships every field name, so it lands roughly halfway: 216.6 → 191.4 B on queue
  tasks where schema packing reached 56.3 B. Inside a stream it is actively
  worse than plain fields (250.9 vs 168.2 B/span). Worth it only when you can't
  commit to a schema.

---

## The number we withdrew

Our first vector benchmark reported **binary quantization at 83% recall@10**. It
was a real measurement. It was also nearly meaningless.

Binary recall is a property of your **corpus**, not your quantizer. Same code,
same quantizer, only the corpus's intrinsic rank changing:

| corpus rank | int8 | binary |
|--:|--:|--:|
| 16 | 100.0% | **83.3%** |
| 128 | 100.0% | 62.0% |
| 768 | 99.3% | **34.7%** |
| full | 98.7% | **32.0%** |

Our 83% was a rank-16 artifact. At rank 768 the same code measures 34.7% -
within a point of Redis's own published 35.5% for Vector Sets `BIN` on real
Word2Vec. Real text embeddings live in that regime.

**Use int8.** It held 98.7–100% across the entire sweep.

A compression ratio without a recall number, measured on a representative
corpus, is not a result. Run `measureRecall()` on *your* embeddings.

---

## Is Redis actually a primary database?

Four of the five objections are fixable, and each one is a program that exits
non-zero if it stops being true.

| Objection | Verdict | Evidence |
|---|---|---|
| Loses acknowledged writes on crash | **fixed** | defaults: 0/20,000 survived. AOF: 20,000/20,000 |
| Silently deletes data when full | **fixed** | `allkeys-lru` destroyed 99.8%, zero errors. `volatile-ttl`: none |
| No transactions, can't hold invariants | **fixed** | read-modify-write: 69 violations. Lua: 0, no retries |
| Can only query by key | **fixed** | hybrid index, full query language, 2.04× cheaper |
| Dataset must fit in RAM | **true** | tiering is commercial-only. Stated, not spun |

```bash
node src/proof/durability.ts   # SIGKILL it, count survivors, measure the fsync window
node src/proof/eviction.ts     # fill it up, see what disappears
node src/proof/integrity.ts    # 64 workers racing, 4 invariants
node src/proof/query.ts        # indexes vs sharding, checked against ground truth
```

Details: [docs/production.md](docs/production.md).

---

## Docs

| | |
|---|---|
| **[docs/](docs/README.md)** | **use-case index - start here** |
| [caching](docs/caching.md) | LLM response caching - stampede protection, semantic hits |
| [records](docs/records.md) | sharding - the biggest single win (15.4×) |
| [packing](docs/packing.md) | the schema DSL, compression, safe schema migration |
| [embeddings](docs/embeddings.md) | quantization, and how to not fool yourself |
| [metrics](docs/metrics.md) | percentiles and time series - including when a sketch *loses* |
| [tuning](docs/tuning.md) | size classes, shard width, which bytes actually matter |
| [production](docs/production.md) | durability, eviction, atomicity, querying |
| [measuring](docs/measuring.md) | the harness, and why `used_memory_dataset` is a trap |

---

## When not to use this

nibble is a RAM-cost optimization layer, not a framework. It is worth the
complexity only when **all three** hold: you have millions of records, RAM is
your binding constraint, and you control the access patterns.

Skip it when:

- **You're under ~1M records.** 8.6× of 2 GB saves ~$20/month and costs you a
  codec to maintain forever. Run the doctor, fix anything CRITICAL, walk away.
- **You need what sharding takes away** - per-key TTL semantics, `FT.SEARCH`
  over records, per-key eviction, `SCAN` across individual records, per-record
  keyspace notifications. Each has a workaround; each workaround costs something.
- **You're on managed Redis with locked-down parameter groups.** ElastiCache and
  Memorystore gate `CONFIG SET`, and some of what nibble tunes may not be settable.
  Untested there.
- **You're CPU-bound, not RAM-bound.** This trades CPU for RAM and will make
  things worse.
- **The data belongs somewhere else.** Transcripts are the largest line item
  even optimized. Most of that belongs in object storage with a hot tail in Redis.

And to be clear about what this isn't: nibble has no execution model, no
checkpoint/resume, no tool routing, no streaming, no OTel integration, and no
non-TypeScript client. If you're choosing what to *build an agent on*,
LangGraph's Redis checkpointer, redis-vl, or Postgres + pgvector are more
complete answers. nibble is narrow on purpose - it is about making the bytes small
and proving the number.

---

## Caveats

Single-node Docker on one laptop - no cluster, no replication, no network.
Synthetic data throughout; text is Zipf-sampled from a 20k-word vocabulary,
which is *harder* to compress than real prose, so compression figures are
conservative. Prices are August 2026 list, us-east-1. `docker kill` proves
process-crash durability and cannot prove machine-loss durability, which is why
the fsync window is measured separately rather than asserted.

Re-run everything and check us: `bash scripts/run-all.sh`.
