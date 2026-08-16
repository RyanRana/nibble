/**
 * CASE 04 — Semantic memory (embeddings).
 *
 * Usually the single largest line item. 1536-d float32 is 6 KiB of raw floats
 * per memory: 20M memories = 117 GiB before a byte of index or key overhead.
 *
 * Two separate questions, deliberately kept apart:
 *
 *   1. Storage  — what does it cost to *keep* the vector?
 *   2. Index    — what does it cost to *search* it? (HNSW graph links are not
 *                 free, and a benchmark that compares a bare string blob to an
 *                 indexed vector set without saying so is cheating.)
 *
 * Every lossy variant reports recall@10 against exact float32 cosine over the
 * same corpus. Compression without a recall number is not a result.
 *
 * ── READ THIS BEFORE QUOTING THE RECALL NUMBERS ─────────────────────────
 *
 * The recall figures here are measured on a synthetic corpus of moderate
 * intrinsic rank, and binary-quantization recall is almost entirely a property
 * of that rank rather than of the quantizer. Run `vector-rank-study.ts`: on the
 * same code, binary-only recall@10 moves from 83% to 32% purely by changing how
 * much of the space the corpus occupies. At rank 768 it measures 34.7%, which
 * is within a point of Redis's own published 35.5% for Vector Sets `BIN` on
 * real Word2Vec embeddings.
 *
 * So: treat the int8 numbers as generalizable (they hold at 98.7–100% across
 * the entire rank sweep, matching every published source) and treat the binary
 * numbers as an upper bound that your real corpus will not reach.
 *
 * ── Two structural facts about Vector Sets that change the recommendation ─
 *
 * 1. Vector Sets DISCARD the full-precision vector on insert — `VEMB` returns
 *    a dequantized approximation. There is therefore no rescoring path inside a
 *    vector set, so the binary→int8 rerank that rescues binary quantization
 *    elsewhere is not expressible here. Emulating it with a second key costs
 *    MORE than simply using Q8.
 * 2. `REDUCE`'s projection matrix is stored once per key at
 *    `input_dim × output_dim × 4` bytes. At the N=8,000 used here that is 393
 *    B/vector for 1536→512 — 24% of the total — but only 3 B/vector at N=1M.
 *    The REDUCE rows below are therefore pessimistic by construction; judge
 *    them at production scale, not this one.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { makeEmbeddingCorpus, describeCorpus } from '../lib/rng.ts';
import {
  encodeF32, encodeF16, decodeF16, encodeInt8, decodeInt8, encodeBinary,
  hamming, cosine, recallAtK, exactTopK,
} from '../lib/quantize.ts';

const N = 8_000;
const DIM = 1536;
const K = 10;
const NQ = 20;

const CORPUS = makeEmbeddingCorpus(20260816, N, DIM, NQ);
const VECS = CORPUS.vectors;
const QUERIES = CORPUS.queries;

// Precompute quantized forms once — reused by both the loaders and the recall math.
const F32 = VECS.map(encodeF32);
const F16 = VECS.map(encodeF16);
const I8 = VECS.map(encodeInt8);
const BIN = VECS.map(encodeBinary);
const QBIN = QUERIES.map(encodeBinary);
const QI8 = QUERIES.map(encodeInt8);

const F16D = F16.map(decodeF16);
const I8D = I8.map(decodeInt8);
const QI8D = QI8.map(decodeInt8);

/** Exact float32 cosine ground truth — computed once, shared by every measurement. */
let truthIdx: Set<number>[] | null = null;
function truth(): Set<number>[] {
  if (!truthIdx) truthIdx = exactTopK(VECS, QUERIES, K);
  return truthIdx;
}

let recallCache: Record<string, string> | null = null;
function jsRecall(): Record<string, string> {
  if (recallCache) return recallCache;
  const t = truth();
  const pct = (x: number) => `${(x * 100).toFixed(1)}%`;
  recallCache = {
    'recall@10 float16': pct(recallAtK(t, N, K, (q, d) => cosine(QUERIES[q], F16D[d]))),
    'recall@10 int8': pct(recallAtK(t, N, K, (q, d) => cosine(QI8D[q], I8D[d]))),
    'recall@10 binary': pct(recallAtK(t, N, K, (q, d) => -hamming(QBIN[q], BIN[d]))),
    'recall@10 binary→int8 rerank(×10)': pct(
      recallAtK(t, N, K, (q, d) => -hamming(QBIN[q], BIN[d]), {
        widen: 10,
        score: (q, d) => cosine(QI8D[q], I8D[d]),
      }),
    ),
  };
  return recallCache;
}

/** recall@10 of an actual VSIM search against the exact ground truth. */
async function vsimRecall(r: Redis, key: string): Promise<string> {
  const t = truth();
  let hits = 0;
  for (let q = 0; q < NQ; q++) {
    // @ts-ignore raw variadic
    const res = (await r.callBuffer('VSIM', key, 'FP32', encodeF32(QUERIES[q]), 'COUNT', String(K), 'EF', '200')) as Buffer[];
    for (const el of res) if (t[q].has(Number(el.toString().slice(1)))) hits++;
  }
  return `${((hits / (NQ * K)) * 100).toFixed(1)}%`;
}

const vaddAll = (key: string, blobs: Buffer[], tail: string[] = []) =>
  async (r: Redis) => {
    const cmds = blobs.map((b, i) => ['VADD', key, 'FP32', b, `v${i}`, ...tail]);
    await pipe(r, cmds as any, 200);
    return N;
  };

export const case04: BenchCase = {
  id: '04-vectors',
  title: `Semantic memory (${N.toLocaleString()} × ${DIM}-d embeddings)`,
  question: 'How small can a searchable embedding get before retrieval quality actually degrades?',
  unit: 'vector',

  variants: [
    {
      name: 'A · STRING, JSON array',
      note: 'Storage only. The "we JSON.stringify()-ed the embedding" starting point — and it is a disaster.',
      load: async (r: Redis) => {
        await pipe(
          r,
          VECS.map((v, i) => ['SET', `mem:${i}`, JSON.stringify(Array.from(v))]) as any,
          100,
        );
        return N;
      },
    },
    {
      name: 'B · STRING, raw float32',
      note: 'Storage only. Lossless: the vector as its actual 6,144 bytes.',
      load: async (r: Redis) => {
        await pipe(r, F32.map((b, i) => ['SET', `mem:${i}`, b]) as any, 100);
        return N;
      },
    },
    {
      name: 'C · STRING, float16',
      note: 'Storage only. Halves it; embeddings have nothing like 24 bits of useful mantissa.',
      load: async (r: Redis) => {
        await pipe(r, F16.map((b, i) => ['SET', `mem:${i}`, b]) as any, 100);
        return N;
      },
      probe: async () => ({ 'recall@10': jsRecall()['recall@10 float16'] }),
    },
    {
      name: 'D · STRING, int8 + scale',
      note: 'Storage only. One byte per dimension plus a float32 scale header.',
      load: async (r: Redis) => {
        await pipe(r, I8.map((b, i) => ['SET', `mem:${i}`, b]) as any, 100);
        return N;
      },
      probe: async () => ({ 'recall@10': jsRecall()['recall@10 int8'] }),
    },
    {
      name: 'E · STRING, binary (1 bit/dim)',
      note: 'Storage only. 192 bytes. Recall is corpus-dependent to the point of being unquotable — see vector-rank-study.ts.',
      caveat: 'binary recall is a rank artifact; real corpora do far worse',
      load: async (r: Redis) => {
        await pipe(r, BIN.map((b, i) => ['SET', `mem:${i}`, b]) as any, 100);
        return N;
      },
      probe: async () => ({
        'recall@10 binary only': jsRecall()['recall@10 binary'],
        'recall@10 + int8 rerank': jsRecall()['recall@10 binary→int8 rerank(×10)'],
      }),
    },
    {
      name: 'F · sharded HASH, int8',
      note: 'Storage only. int8 vectors packed into shared hashes — removes the per-key overhead too.',
      config: { 'hash-max-listpack-entries': 0 },
      load: async (r: Redis) => {
        const shards = Math.ceil(N / 512);
        await pipe(
          r,
          I8.map((b, i) => ['HSET', `mem:{${i % shards}}`, String(i), b]) as any,
          100,
        );
        return N;
      },
      encodingProbes: ['mem:{0}'],
    },
    {
      name: 'G · VECTOR SET, NOQUANT (f32)',
      note: 'Searchable. Full float32 in an HNSW graph — this row is where the index cost becomes visible.',
      load: vaddAll('vs', F32, ['NOQUANT']),
      encodingProbes: [],
      probe: async (r) => ({
        'quant': 'f32',
        'recall@10 (VSIM)': await vsimRecall(r, 'vs'),
        'includes': 'HNSW graph',
      }),
    },
    {
      name: 'H · VECTOR SET, Q8 (default)',
      note: 'Searchable. int8 quantization inside the index. Redis 8 default for good reason.',
      load: vaddAll('vs', F32, ['Q8']),
      probe: async (r) => ({ 'quant': 'int8', 'recall@10 (VSIM)': await vsimRecall(r, 'vs') }),
    },
    {
      name: 'I · VECTOR SET, BIN',
      note: 'Searchable. 1 bit per dimension. Note Redis does NOT rerank internally — the source vector is discarded, so there is nothing to rerank against.',
      caveat: 'no rescoring path exists; Redis measures 35.5% on real Word2Vec',
      load: vaddAll('vs', F32, ['BIN']),
      probe: async (r) => ({ 'quant': 'bin', 'recall@10 (VSIM)': await vsimRecall(r, 'vs') }),
    },
    {
      name: 'J · VECTOR SET, REDUCE 512 + Q8',
      note: 'Searchable. Projection to 512-d before quantizing. Despite the docs saying "random projection", the implementation is a deterministic truncated Walsh–Hadamard transform — so it is reproducible across replicas, but the Johnson–Lindenstrauss distortion bound does not formally apply.',
      caveat: 'projection matrix is 24% of this figure at N=8k, ~0.2% at N=1M',
      load: async (r: Redis) => {
        const cmds = F32.map((b, i) => ['VADD', 'vs', 'REDUCE', '512', 'FP32', b, `v${i}`, 'Q8']);
        await pipe(r, cmds as any, 200);
        return N;
      },
      probe: async (r) => ({
        'quant': 'int8 @ 512-d',
        'recall@10 (VSIM)': await vsimRecall(r, 'vs'),
      }),
    },
    {
      name: 'K · VECTOR SET, REDUCE 256 + BIN',
      note: 'Searchable, maximally squeezed — and a negative result: it is LARGER than plain BIN, because at 256 dims the projection matrix costs more than the dimensions it saves. Once you reach BIN, the HNSW graph is 80% of the cost and further vector compression is pointless.',
      caveat: 'larger than plain BIN at this N; quality cliff',
      load: async (r: Redis) => {
        const cmds = F32.map((b, i) => ['VADD', 'vs', 'REDUCE', '256', 'FP32', b, `v${i}`, 'BIN']);
        await pipe(r, cmds as any, 200);
        return N;
      },
      probe: async (r) => ({
        'quant': 'bin @ 256-d',
        'recall@10 (VSIM)': await vsimRecall(r, 'vs'),
      }),
    },
  ],
};
