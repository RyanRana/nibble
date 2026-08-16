# Tuning for the allocator

> Redis does not charge you for the bytes you stored. It charges you for the size class you landed in.
> class you landed in.

---

## Copy this

```ts
await applyKitConfig(redis, { shardWidth: 124 });   // sets the listpack thresholds
```

Then sweep the width for *your* record size - it is one constant and it is worth
about 15%:

```bash
node src/bench/run-all.ts 11    # case 11 is the shard-width sweep
```

The rest of this page explains why 124 beats 128, and why that is not a typo.

---

## The staircase

jemalloc rounds every allocation up to a size class:

```
8  16  32  48  64  80  96  112  128  160  192  224  256  320  384  448  512 …
```

So a 129-byte value costs the same as a 160-byte one. A 257-byte value costs the
same as 320. **Shaving three bytes off a record is usually worth nothing and
occasionally worth 20%** - the only savings that count are the ones that cross a
step.

Measured, 50,000 keys with a fixed 8-character key name:

| Value | B/key | |
|--:|--:|---|
| 40 | 95.3 | |
| 44 | 95.3 | ← same cost as 40 |
| 45 | 108.8 | ← crossed a step |
| 48 | 103.3 | |
| 128 | 207.3 | |
| 129 | 207.3 | ← one byte over, same class |
| 256 | 372.8 | |
| 257 | 367.3 | |

Full sweep including the large-object regime: `results/bench.md`, case 08.

**Design your records to land just *under* a step.** Add a field for free; do
not add the field that pushes you over.

---

## The cliff everyone gets wrong

The folklore is "values ≤ 44 bytes are stored as `embstr`, longer ones as
`raw`". That is not the rule in Redis 8. The decision in `kvobjSet` is:

```
16 + (keylen + 3) + (4 + vallen) ≤ 64
```

**Key length and value length share one 64-byte budget.** A short key name buys
you a longer embedded value, and vice versa. Which is a second, independent
reason to keep key names short - one you won't find by reading about `embstr`.

---

## The 1 KiB you're wasting on every vector

A 1536-dimensional `float32` embedding is 6,144 bytes. In the large-object
regime jemalloc's classes go `… 4096, 5120, 6144, 7168, 8192 …`, and your value
needs a 3-byte SDS header plus a terminator:

```
6144 + 3 + 1 = 6148  →  rounds to 7168
```

**1,020 bytes wasted per vector, ~19 GiB across 20M memories.** Measured: 7,214
B/key for a 6,144-byte payload.

Fixes, in order of preference:

1. **Quantize.** int8 takes you to 1,540 B, which sits comfortably inside the
   2048 class. This is the right answer for other reasons too - see
   [05](embeddings.md).
2. **Truncate the vector.** If your model supports Matryoshka embeddings,
   1536 → 1024 dims lands at 4,096 + header → 5,120 class.
3. **Pack them into a sharded hash**, where the listpack amortizes the rounding
   across many vectors instead of paying it per key.

---

## Shard width: the 15% nobody tunes

This is where the staircase meets [sharding](records.md), and it is the
least obvious result in the repo.

A shard's listpack is **one allocation** whose size is roughly
`width × bytes_per_entry`. Pick a round number and it may land just over a size
class; pick a slightly smaller one and it lands just under.

Measured, 120,000 entries of ~140 B:

| Fields/shard | B/entry (no TTL) | B/entry (per-field TTL) |
|--:|--:|--:|
| 96 | 171 | 214 |
| 112 | 183 | 184 |
| 120 | 171 | 205 |
| **124** | **167** | 199 |
| 128 | 193 | 193 |
| **160** | 180 | **180** |
| 192 | 171 | 214 |
| 256 | 192 | 193 |

**124 vs 128 is a 15% swing from one constant.** And the best width differs
depending on whether you carry TTLs, because the TTL changes the per-entry size
and therefore where the listpack lands.

There is no formula worth memorizing here - the entry size depends on your
record. Sweep it:

```bash
node src/bench/run-all.ts 11    # case 11 is the shard-width sweep
```

Copy `src/bench/case-11-shardsize.ts`, swap in your record, keep the cheapest.

---

## The TTL mystery this explains

Per-field TTLs (`HEXPIRE`) appeared to cost **~31 bytes per field**. The
intrinsic cost in a `listpackex` is a 64-bit expiry, about **10 bytes**.

The other ~21 bytes were the shard's listpack crossing a 4 KiB boundary. At an
aligned width the same TTLs cost almost nothing:

```
width 128:  no TTL 192.5   with TTL 192.6    ← +0.1 B
width 120:  no TTL 171.1   with TTL 205.4    ← +34.3 B
```

Same feature. Same data. The difference is entirely allocator alignment.

If you measure a surprising per-feature cost in Redis, suspect the staircase
before you suspect the feature.

---

## Measuring fragmentation properly

Do **not** use `mem_fragmentation_ratio`. Redis's own docs say it "doesn't only
include fragmentation, but also other process overheads."

Use `allocator_frag_ratio` - the true external fragmentation metric. The harness
in this repo reports it from every sample (`src/lib/measure.ts`).

RSS is also sticky: fill an instance to 5 GB, delete 2 GB, and RSS stays near
5 GB. **Provision for peak, not steady state.**

`activedefrag` is off by default, requires the bundled jemalloc, and can consume
up to 25% of the main thread. It's a latency trade, not a free win.

---

## Rules of thumb

1. Only byte savings that cross a size class change the bill.
2. Key name length and value length share one 64-byte embedding budget.
3. Sweep your shard width. It's one constant and it's worth ~15%.
4. A surprising per-feature cost is usually the staircase, not the feature.
5. Provision for peak RSS, because the allocator does not give it back.
