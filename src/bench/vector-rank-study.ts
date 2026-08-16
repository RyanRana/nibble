/**
 * VECTOR RANK STUDY — why most published quantization-recall numbers are wrong,
 * including the one this benchmark originally reported.
 *
 * ── How this file came to exist ──────────────────────────────────────────
 *
 * Case 04 measured binary quantization at 83% recall@10, and binary→int8
 * reranking at 100%. Both numbers are real measurements. Both are close to
 * meaningless, because they are properties of the *corpus*, not of the
 * quantizer.
 *
 * A sign-bit binary code keeps one bit per dimension. Whether that is enough
 * depends entirely on how much of the embedding space the data actually
 * occupies. If documents live on a low-dimensional manifold — a handful of
 * latent topics — then the top-10 neighbours are separated by wide margins, and
 * one bit per dimension resolves them easily. If the data fills the space, the
 * top-10 are separated by hairline margins that a single bit cannot represent.
 *
 * So: recall is a function of the corpus's *effective rank*, and quoting a
 * recall number without quoting the rank is like quoting a compression ratio
 * without saying what you compressed.
 *
 * This study sweeps rank from 8 to full and measures the collapse directly.
 *
 * ── The calibration point ────────────────────────────────────────────────
 *
 * Redis's own published measurement for Vector Sets `BIN` on real Word2Vec
 * embeddings (3M × 300-d, recall@10 against an exact scan) is 35.5%. Any
 * synthetic corpus that reports binary recall far above that is too easy. This
 * sweep shows exactly which rank reproduces which number, so a reader can see
 * where their own data probably sits instead of trusting a single figure.
 *
 * ── Relative contrast ───────────────────────────────────────────────────
 *
 * Reported alongside recall: C_r = mean distance / nearest distance. It is the
 * standard measure of whether nearest-neighbour search is even meaningful on a
 * dataset (Beyer et al. 1999; He et al. 2012). Published values for real text
 * embeddings land around 1.75–2.05. If a synthetic corpus scores far above
 * that, its recall numbers will not transfer.
 */
import fs from 'node:fs';
import path from 'node:path';
import { makeEmbeddingCorpus } from '../lib/rng.ts';
import {
  encodeF16, decodeF16, encodeInt8, decodeInt8, encodeBinary,
  hamming, cosine, recallAtK, exactTopK,
} from '../lib/quantize.ts';

const N = 4_000;
const DIM = 1536;
const NQ = 15;
const K = 10;

/** Ranks to sweep. `DIM` means "fills the space" — the hardest realistic case. */
const RANKS = [8, 16, 32, 64, 128, 256, 768, DIM];

/**
 * Effective rank via the participation ratio of the covariance spectrum,
 * estimated from the Gram matrix of a sample. A cheap, standard proxy:
 * (Σλ)² / Σλ² — equals r exactly for a flat rank-r spectrum.
 */
function effectiveRank(vs: Float32Array[], sampleSize = 600): number {
  const s = vs.slice(0, sampleSize);
  const n = s.length;
  // eigenvalues of the Gram matrix = eigenvalues of the covariance (up to scale)
  const gram: number[] = [];
  for (let i = 0; i < n; i++) {
    for (let j = 0; j <= i; j++) {
      let d = 0;
      for (let k = 0; k < s[i].length; k++) d += s[i][k] * s[j][k];
      gram.push(d * (i === j ? 1 : 2));
    }
  }
  // trace and Frobenius norm give Σλ and Σλ² without an eigendecomposition
  let trace = 0, fro = 0;
  let idx = 0;
  for (let i = 0; i < n; i++) {
    for (let j = 0; j <= i; j++) {
      const v = gram[idx++];
      if (i === j) {
        trace += v;
        fro += v * v;
      } else {
        fro += (v / 2) * (v / 2) * 2;
      }
    }
  }
  return (trace * trace) / fro;
}

/** C_r = mean pairwise distance / nearest distance, averaged over queries. */
function relativeContrast(vs: Float32Array[], qs: Float32Array[]): number {
  let acc = 0;
  for (const q of qs) {
    let sum = 0, min = Infinity;
    for (const v of vs) {
      // cosine distance on unit vectors
      const d = 1 - cosine(q, v);
      sum += d;
      if (d < min && d > 1e-9) min = d;
    }
    acc += sum / vs.length / (min || 1e-9);
  }
  return acc / qs.length;
}

interface Row {
  rank: number;
  effectiveRank: number;
  relativeContrast: number;
  f16: number;
  int8: number;
  binary: number;
  rerank10: number;
  rerank50: number;
}

const rows: Row[] = [];

console.log(`\n▸ Vector quantization recall vs corpus intrinsic rank`);
console.log(`  ${N.toLocaleString()} vectors × ${DIM}-d, recall@${K} vs exact float32 cosine, ${NQ} queries\n`);
console.log(
  '  rank  eff.rank  C_r    f16     int8    binary  bin→int8 ×10  bin→int8 ×50',
);
console.log('  ' + '─'.repeat(74));

for (const rank of RANKS) {
  // topicsPerDoc must scale WITH rank. Capping it (an earlier bug here) keeps
  // every document inside a k-dimensional subspace no matter how many basis
  // directions the corpus has, so the corpus never actually becomes full-rank
  // and binary quantization never degrades — which is exactly the artifact this
  // study exists to expose.
  const { vectors, queries } = makeEmbeddingCorpus(
    20260816 + rank, N, DIM, NQ,
    rank,
    rank,
  );

  const truth = exactTopK(vectors, queries, K);
  const f16 = vectors.map((v) => decodeF16(encodeF16(v)));
  const i8 = vectors.map((v) => decodeInt8(encodeInt8(v)));
  const qi8 = queries.map((v) => decodeInt8(encodeInt8(v)));
  const bin = vectors.map(encodeBinary);
  const qbin = queries.map(encodeBinary);

  const row: Row = {
    rank,
    effectiveRank: +effectiveRank(vectors).toFixed(1),
    relativeContrast: +relativeContrast(vectors.slice(0, 1200), queries.slice(0, 5)).toFixed(3),
    f16: recallAtK(truth, N, K, (q, d) => cosine(queries[q], f16[d])),
    int8: recallAtK(truth, N, K, (q, d) => cosine(qi8[q], i8[d])),
    binary: recallAtK(truth, N, K, (q, d) => -hamming(qbin[q], bin[d])),
    rerank10: recallAtK(truth, N, K, (q, d) => -hamming(qbin[q], bin[d]), {
      widen: 10, score: (q, d) => cosine(qi8[q], i8[d]),
    }),
    rerank50: recallAtK(truth, N, K, (q, d) => -hamming(qbin[q], bin[d]), {
      widen: 50, score: (q, d) => cosine(qi8[q], i8[d]),
    }),
  };
  rows.push(row);

  const p = (x: number) => `${(x * 100).toFixed(1)}%`.padStart(6);
  console.log(
    `  ${String(rank).padStart(4)}  ${String(row.effectiveRank).padStart(8)}  ` +
      `${row.relativeContrast.toFixed(2).padStart(5)}  ${p(row.f16)}  ${p(row.int8)}  ` +
      `${p(row.binary)}  ${p(row.rerank10).padStart(12)}  ${p(row.rerank50).padStart(12)}`,
  );
}

console.log('');
console.log('  Reference points from published measurements on REAL embeddings:');
console.log('    Redis Vector Sets BIN, Word2Vec 3M×300d, recall@10 ............ 35.5%');
console.log('    Redis Vector Sets Q8,  same corpus (HNSW graph error only) .... 96.0%');
console.log('    pgvector binary, dbpedia-1536, ef=200, no rescore .............. 68.3%');
console.log('    pgvector binary, dbpedia-1536, ef=200, WITH rescore ............ 99.0%');
console.log('    pgvector binary, gist-960, any ef, with or without rescore ...... 0.0%');
console.log('    Relative contrast measured on real text embeddings ....... 1.75–2.05');
console.log('');

const lowRank = rows.find((r) => r.rank === 16)!;
const highRank = rows[rows.length - 1];
console.log(
  `  Conclusion: binary-only recall falls from ${(lowRank.binary * 100).toFixed(1)}% at rank ${lowRank.rank} ` +
    `to ${(highRank.binary * 100).toFixed(1)}% at full rank — a ${(lowRank.binary / Math.max(highRank.binary, 1e-9)).toFixed(1)}× swing ` +
    `from the CORPUS alone, with the quantizer unchanged.`,
);
console.log(
  `  int8 holds at ${(rows[0].int8 * 100).toFixed(1)}%–${(highRank.int8 * 100).toFixed(1)}% across the entire sweep. ` +
    `That is the finding that generalizes.\n`,
);

const dir = path.resolve(import.meta.dirname, '../../results');
fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(
  path.join(dir, 'vector-rank-study.json'),
  JSON.stringify({ n: N, dim: DIM, k: K, queries: NQ, rows }, null, 2),
);
console.log('  → results/vector-rank-study.json\n');
