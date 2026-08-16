# Embeddings and semantic memory

> Use int8. The rest of this page is why, and how I nearly got it wrong.

---

## Copy this

```ts
import { connect, VectorMemory } from './src/kit/index.ts';

const redis = await connect();
const mem = new VectorMemory(redis, { key: 'mem', dim: 1536, quant: 'Q8' });

await mem.add(memoryId, embedding, { tenant });
await mem.search(queryVector, 10);
```

**Use `Q8`.** That single word is the most load-bearing thing on this page:
int8 held 98.7–100% recall across a full sweep of corpus difficulty, while
binary ranged from 83% down to 32% on identical code. And never store an
embedding as a JSON array - it is 18× worse than int8.

---

## The short version

```ts
const mem = new VectorMemory(redis, { key: 'mem', dim: 1536, quant: 'Q8' });
await mem.add(id, embedding, { tenant });
await mem.search(query, 10);
```

Measured at 1536 dimensions, 8,000 vectors, **including** the HNSW graph:

| Quantization | B/vector | recall@10 (VSIM) |
|---|--:|--:|
| `NOQUANT` (f32) | 6,900 | 100% |
| **`Q8` (int8)** | **2,291** | **100%** |
| `BIN` | 947 | 83.5% ← *see below* |
| `REDUCE 512` + `Q8` | 1,661 | 85.5% |
| `REDUCE 256` + `BIN` | 985 | 52.5% |

Storage only, no index:

| Encoding | B/vector | recall@10 |
|---|--:|--:|
| JSON array | 32,815 | lossless |
| raw float32 | 7,214 | lossless |
| float16 | 3,630 | 100% |
| **int8 + scale** | **1,838** | **100%** |
| binary (1 bit/dim) | 270 | *unquotable* |

That JSON row is not a joke. `JSON.stringify(Array.from(embedding))` is a real
thing people ship, and it is **4.5× worse than the raw floats** and 18× worse
than int8.

---

## The number we withdrew

Our first pass reported **binary quantization at 83% recall@10**, with
binary→int8 reranking recovering 100%. Both were real measurements taken by the
code in this repo. Both were close to meaningless.

**Binary recall is a property of your corpus, not of your quantizer.**

A sign-bit code keeps one bit per dimension. Whether that's enough depends
entirely on how much of the embedding space your data actually occupies. If
documents sit on a low-dimensional manifold - a handful of latent topics - then
the top-10 neighbours are separated by wide margins and one bit resolves them
easily. If the data fills the space, the top-10 are separated by hairline
margins a single bit cannot represent.

```bash
node src/bench/vector-rank-study.ts
```

Same code. Same quantizer. Only the corpus's intrinsic rank changes:

| corpus rank | effective rank | contrast | int8 | binary | bin→int8 ×10 | ×50 |
|--:|--:|--:|--:|--:|--:|--:|
| 8 | 10.2 | 6.09 | 100.0% | 79.3% | 100.0% | 100.0% |
| 16 | 17.0 | 2.52 | 100.0% | **83.3%** | 100.0% | 100.0% |
| 64 | 57.3 | 3.80 | 99.3% | 67.3% | 99.3% | 99.3% |
| 128 | 100.0 | 3.66 | 100.0% | 62.0% | 98.7% | 100.0% |
| 256 | 162.2 | 1.26 | 100.0% | 50.7% | 95.3% | 100.0% |
| 768 | 277.6 | 5.06 | 99.3% | **34.7%** | 76.7% | 97.3% |
| full | 338.3 | 1.12 | 98.7% | **32.0%** | 71.3% | 94.7% |

Our 83% was a rank-16 artifact.

At rank 768 the same code measures **34.7%** - within a point of Redis's own
published **35.5%** for Vector Sets `BIN` on real Word2Vec embeddings. Real text
corpora live in that regime.

### Calibration against published work

| Source | Result |
|---|--:|
| Redis Vector Sets `BIN`, Word2Vec 3M×300d | 35.5% |
| Redis Vector Sets `Q8`, same corpus (graph error only) | 96.0% |
| pgvector binary, dbpedia-1536, no rescore | 68.3% |
| pgvector binary, dbpedia-1536, **with** rescore | 99.0% |
| pgvector binary, gist-960, any config | **0.0%** |
| Relative contrast of real text embeddings | 1.75–2.05 |

That gist-960 row is the one to remember. A 960-dimensional dataset scoring
**zero** - because GIST descriptors are non-negative, so nearly every sign bit
is 1. Dimension count does not save you.

**int8 held 98.7–100% across the entire sweep.** That is the finding that
generalizes, and it matches every published source.

---

## Two structural facts about Vector Sets

**1. The full-precision vector is destroyed on insert.** `VEMB` returns a
dequantized approximation. So there is **no rescoring path inside a vector
set** - the binary→int8 rerank that rescues binary quantization elsewhere cannot
be expressed.

Emulating it with a second key costs more than just using Q8:

```
Q8 vector set                            = dim + 758        = 2,294 B
BIN vector set + external int8 for rescore ≈ 1.125·dim + 822 = 2,550 B
```

It never wins. Algebraically.

**2. `REDUCE` is not a random projection**, despite the docs saying so. The
implementation is a deterministic truncated Walsh-Hadamard transform - which
means it reproduces identically across replicas, but the Johnson-Lindenstrauss
distortion bound does not formally apply. Treat its recall as something to
measure, not assume.

Its projection matrix is stored **once per key** at `input_dim × output_dim × 4`
bytes. At 1536→512 that's 3 MiB:

| N vectors | matrix cost/vector |
|--:|--:|
| 8,000 | 393 B (24% of the total!) |
| 100,000 | 31 B |
| 1,000,000 | 3 B |

**Our `REDUCE` rows are pessimistic by construction.** Judge them at production
scale, not at 8,000.

---

## Where the bytes actually go

At `M=16` the HNSW graph is a fixed ~758 B/vector. So:

| Quantization | vector | graph | graph's share |
|---|--:|--:|--:|
| f32 | 6,144 | 758 | 11% |
| int8 | 1,536 | 758 | **33%** |
| binary | 192 | 758 | **80%** |

Two consequences people miss:

- Binary is documented as "32× smaller" than f32. End to end at 1536 dims the
  realized ratio is **7.3×**, and binary vs int8 is only **2.4×**. Below ~256
  dims binary barely beats int8 at all.
- **Once you're at binary, further vector compression is pointless.** That's why
  `REDUCE 256 + BIN` (985 B) came out *larger* than plain `BIN` (947 B). The only
  lever left is `M` - dropping to `M=8` saves ~150 B/vector.

---

## The allocator trap

A 1536-d float32 vector is 6,144 bytes. With the SDS header it needs 6,148,
which rounds to jemalloc's **7,168** class: **1,020 bytes wasted per vector**,
~19 GiB across 20M memories.

Quantizing to int8 (1,540 B) lands inside the 2,048 class and fixes this as a
side effect. → [03 · the allocator](tuning.md)

---

## Measure it on your own embeddings

This is the part you should not skip. It's twenty lines.

```ts
import { measureRecall } from './src/kit/index.ts';

const recall = await measureRecall(
  async (q, k) => (await mem.search(q, k)).map((h) => h.id),
  corpus,     // [{ id, vector }] with the ORIGINAL float32 vectors
  queries,    // real queries, or held-out documents
  10,
);
```

Report it next to any compression ratio you quote. Also report your corpus's
**relative contrast** or effective rank - without one of those, a recall number
can't be compared to anyone else's.

---

## Recommendations

1. **Default to `Q8`.** 98.7–100% recall, 3× smaller than f32, boring and
   correct.
2. **Do not use `BIN` in Vector Sets for production retrieval.** Symmetric
   sign-hashing with no centering or rotation, and no rescoring path.
3. **Never store embeddings as JSON.** 18× worse than int8.
4. **Consider Matryoshka truncation** if your model supports it - 1536→512
   typically costs 1–5% NDCG@10 and shrinks both the index *and* the source copy.
5. **Watch for double storage.** The Query Engine keeps the source vector in the
   keyspace *and* the index copy, and rebuilds the index from the keyspace on
   restart, so you cannot delete the source. Storing it as `FLOAT16` halves both.
6. **Measure recall on your corpus before you trust any of the above.**
