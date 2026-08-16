# Measuring memory honestly

> How you take a number matters more than any number you take.

Everything in this repo is a `used_memory` delta. This document is how, and
where the traps are.

---

## Copy this

```ts
import { sample } from './src/lib/measure.ts';

const before = await sample(redis);
await loadYourData();
const after = await sample(redis);

console.log(`${(after.attributable - before.attributable) / records} B/record`);
```

Run it twice and keep the **cheaper** number — allocator noise is one-sided.
The rest of this page is the six ways this measurement goes wrong.

---

## The method

```
1. FLUSHALL SYNC   empty the keyspace — synchronously, or async free
                   drops memory AFTER you sampled
2. CONFIG SET      apply the encoding thresholds this variant needs
3. MEMORY PURGE    make jemalloc return dirty pages, so the baseline isn't
                   inflated by the previous variant's frees
4. sample          record `attributable` memory
5. load            write the data through bounded pipelines
6. MEMORY PURGE    again
7. sample          delta = the cost of the data
```

Every variant runs **twice** and the *cheaper* result is kept. Allocator noise
is one-sided: a sample can be inflated by fragmentation, a background rehash, or
pages jemalloc hasn't returned — but it can never be deflated below the true
allocation. So the minimum is the best estimator, and it's far more stable
run-to-run than the mean.

```ts
import { sample } from './src/lib/measure.ts';

const before = await sample(redis);
await loadYourData();
const after = await sample(redis);

console.log(`${(after.attributable - before.attributable) / records} B/record`);
```

---

## Trap #1: `used_memory_dataset` hides the win

Redis reports several memory figures. The tempting one is `used_memory_dataset`
— "the size of your data" — and it is **wrong for this purpose**.

Redis classifies the top-level key dictionary (`dictEntry` + `robj` + the key's
SDS) as *overhead*, not dataset. Per-key overhead is precisely what
[sharding](records.md) attacks. Measure with `used_memory_dataset` and the
single largest optimization in this repo reports as **zero improvement**.

What we use instead:

```
attributable = used_memory
             − mem_clients_normal      ← your own pipeline's buffers
             − mem_clients_slaves
             − mem_aof_buffer
             − mem_replication_backlog
             − mem_cluster_links
```

Subtract only the genuinely non-dataset buffers. Key overhead stays where it
belongs: in the bill. The fixed startup allocation appears in both samples and
cancels in the delta.

---

## Trap #2: your own client inflates the number

Redis counts client output buffers in `used_memory`. An unbounded pipeline of
100,000 commands can add megabytes that look like data.

`pipe()` batches at 1,000 commands by default for exactly this reason, and we
subtract `mem_clients_normal` on top. If you write your own loader, bound your
batches.

---

## Trap #3: `mem_fragmentation_ratio` is not fragmentation

Redis's own docs: it "doesn't only include fragmentation, but also other process
overheads."

Use **`allocator_frag_ratio`**. `sample()` returns it as `allocatorFrag`.

---

## Trap #4: the encoding changed under you

A number without `OBJECT ENCODING` is unexplained. Half the results in this repo
are "the encoding flipped", not "the data got smaller":

```
one hash per run, hashtable    1,142 B/run
one hash per run, listpack       729 B/run     ← same data, same commands
```

Every benchmark variant declares `encodingProbes`, and `results/bench.md`
carries the encoding for every row. If you can't explain a number by its
encoding, you don't understand the number yet.

Watch for: `listpack` → `hashtable`, `intset` → `listpack` → `hashtable`,
`listpack` → `template-listpack`, `listpack` → `listpackex` (field TTLs),
`quicklist` node compression, `embstr` → `raw`.

**Encoding conversions are one-way.** A hash that exceeds
`hash-max-listpack-entries` and converts to `hashtable` does *not* convert back
when you delete fields.

---

## Trap #5: config leaks between measurements

Every variant resets to a known baseline before applying its own settings.
`DEFAULT_ENCODING_CONFIG` in `src/lib/redis.ts` explicitly restores all of them
— including `hash-min-template-entries: 0`, which we added after a templating
setting from one case silently improved the next.

---

## Trap #6: your synthetic data is too easy

The one that cost us a headline number.

Two failures we found in our own generators:

**Text.** The first version drew from a 60-word lexicon. A 32 KiB trained
dictionary memorizes that entire vocabulary, so dictionary compression scored
4.9:1 — a number nobody could reproduce. Now: 20,000 real words sampled
Zipf(α≈1.07). That's *harder* to compress than real prose (no syntax, no phrase
repetition), so our compression figures are conservative.

**Vectors.** The first version used tight clusters plus iid noise, which makes
the top-10 neighbours differ *only* by high-frequency noise. Every lossy method
scored ~0%. The replacement is a low-rank topic model whose similarity
distribution matches real embedding corpora — and it revealed that our
binary-quantization result was a corpus artifact. → [05](embeddings.md)

**Always report a property of your corpus alongside your result.** For text, the
vocabulary size and sampling. For vectors, effective rank or relative contrast.
A ratio without it can't be compared to anything.

---

## Adding your own case

```ts
import type { BenchCase } from './src/lib/measure.ts';

export const myCase: BenchCase = {
  id: '12-my-records',
  title: 'My record type (100k records)',
  question: 'What is this actually costing?',
  unit: 'record',
  variants: [
    {
      name: 'A · what we ship today',       // variant[0] is the baseline
      note: 'one key per record, JSON',
      load: async (r) => { /* … */ return N; },
      encodingProbes: ['rec:0'],
    },
    {
      name: 'B · sharded + packed',
      note: 'the candidate',
      config: { 'hash-max-listpack-entries': 256 },
      load: async (r) => { /* … */ return N; },
      encodingProbes: ['rec:{0}'],
      probe: async (r) => ({ 'payload B': /* … */ }),
    },
  ],
};
```

Register it in `src/bench/run-all.ts`, then:

```bash
node src/bench/run-all.ts 12
```

Two fields carry the honesty:

**`caveat`** — set it when a variant does *not* answer the same question as the
baseline. Lossy retention, cardinality-only, probabilistic membership. Caveated
rows are excluded from "best result" rankings, because otherwise HyperLogLog
wins every comparison by answering a different question very cheaply.

**`kind: 'sweep'`** — set it when variants are points on an axis (value size,
shard width) rather than competing designs, so no ratios are computed between
them.

---

## What we can't measure here

- **Cluster.** Single node only. No slot migration, no cross-slot behaviour, no
  gossip overhead.
- **Replication.** No replica, so no replica output buffers and no full-sync
  fork spike.
- **Fork / copy-on-write.** Redis "can use up to 2× the memory" during BGSAVE
  because COW copies whole 4 KiB pages. Our instance has persistence off during
  benchmarks *specifically* so it doesn't contaminate samples — which means our
  numbers are steady-state, and you should provision above them.
- **Machine-loss durability.** `docker kill` is process loss. See
  [04](production.md#durability).

Provision against peak RSS, not against these numbers.
