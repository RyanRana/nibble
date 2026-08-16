/** Deterministic PRNG + synthetic agentic workload data. */
import fs from 'node:fs';

/** mulberry32 - small, fast, good enough distribution for payload synthesis. */
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return function () {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function pick<T>(r: () => number, arr: readonly T[]): T {
  return arr[Math.floor(r() * arr.length)];
}

export function intBetween(r: () => number, lo: number, hi: number): number {
  return lo + Math.floor(r() * (hi - lo + 1));
}

/** RFC-4122-shaped id (36 chars) - what most agent frameworks actually store. */
export function uuidLike(r: () => number): string {
  const hex = '0123456789abcdef';
  let s = '';
  for (let i = 0; i < 32; i++) s += hex[Math.floor(r() * 16)];
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
}

const MODELS = [
  'claude-opus-5',
  'claude-sonnet-5',
  'claude-haiku-4-5-20251001',
  'claude-fable-5',
] as const;

const TOOLS = [
  'web_search', 'read_file', 'write_file', 'bash', 'sql_query',
  'vector_search', 'http_request', 'send_email', 'code_interpreter', 'browser',
] as const;

const STATUSES = ['queued', 'running', 'awaiting_tool', 'succeeded', 'failed', 'cancelled'] as const;

/**
 * A realistic agent-run record. 20 fields is typical of orchestration
 * frameworks (LangGraph checkpoints, Temporal-style run rows, Eve sessions).
 */
export interface AgentRun {
  run_id: string;
  tenant_id: string;
  agent_id: string;
  parent_run_id: string;
  thread_id: string;
  status: string;
  model: string;
  created_at: number;
  updated_at: number;
  started_at: number;
  deadline_at: number;
  step_count: number;
  tool_call_count: number;
  prompt_tokens: number;
  completion_tokens: number;
  cached_tokens: number;
  cost_micros: number;
  retry_count: number;
  priority: number;
  region: string;
}

export function makeRun(r: () => number, i: number): AgentRun {
  const now = 1_760_000_000 + i;
  return {
    run_id: uuidLike(r),
    tenant_id: uuidLike(r),
    agent_id: uuidLike(r),
    parent_run_id: uuidLike(r),
    thread_id: uuidLike(r),
    status: pick(r, STATUSES),
    model: pick(r, MODELS),
    created_at: now,
    updated_at: now + intBetween(r, 1, 900),
    started_at: now + intBetween(r, 0, 5),
    deadline_at: now + 3600,
    step_count: intBetween(r, 1, 240),
    tool_call_count: intBetween(r, 0, 180),
    prompt_tokens: intBetween(r, 500, 400_000),
    completion_tokens: intBetween(r, 50, 32_000),
    cached_tokens: intBetween(r, 0, 380_000),
    cost_micros: intBetween(r, 100, 9_000_000),
    retry_count: intBetween(r, 0, 4),
    priority: intBetween(r, 0, 9),
    region: pick(r, ['iad1', 'sfo1', 'fra1', 'hnd1', 'syd1']),
  };
}

/** One tool-call span: the highest-volume record class in an agentic system. */
export interface ToolSpan {
  span_id: string;
  run_id: string;
  step: number;
  tool: string;
  ts_ms: number;
  dur_ms: number;
  ok: number;
  input_bytes: number;
  output_bytes: number;
  retry: number;
  cache_hit: number;
  http_status: number;
}

export function makeSpan(r: () => number, runId: string, step: number): ToolSpan {
  return {
    span_id: uuidLike(r),
    run_id: runId,
    step,
    tool: pick(r, TOOLS),
    ts_ms: 1_760_000_000_000 + step * intBetween(r, 40, 5000),
    dur_ms: intBetween(r, 3, 30_000),
    ok: r() > 0.06 ? 1 : 0,
    input_bytes: intBetween(r, 40, 40_000),
    output_bytes: intBetween(r, 40, 200_000),
    retry: r() > 0.9 ? intBetween(r, 1, 3) : 0,
    cache_hit: r() > 0.55 ? 1 : 0,
    http_status: pick(r, [200, 200, 200, 200, 201, 400, 429, 500, 503]),
  };
}

/**
 * Text corpus.
 *
 * The compression results are only as honest as the entropy of the text they
 * compress. A hand-written 60-word lexicon would let a trained dictionary
 * memorize the entire vocabulary and report compression ratios nobody will
 * reproduce on real transcripts.
 *
 * So: a 20,000-word vocabulary drawn from the system word list, sampled with a
 * Zipf distribution (α≈1.07, roughly what natural language shows). This is
 * *harder* to compress than real prose, not easier - Zipf-sampled words have no
 * syntax, no phrase-level repetition and no shared sentence structure for a
 * compressor to exploit. Every compression number in this repo is therefore a
 * conservative lower bound on what real transcripts would achieve.
 */
const VOCAB: string[] = (() => {
  const fallback = 'agent repository endpoint deploy dashboard summarize engineer reproduction payload traceback worker production latency embedding retrieval orchestration checkpoint'.split(' ');
  try {
    const all = fs.readFileSync('/usr/share/dict/words', 'utf8').split('\n').filter((w) => w.length > 2 && w.length < 14);
    if (all.length < 5000) return fallback;
    // deterministic sample of 20k words
    const r = rng(9182736);
    const out: string[] = [];
    for (let i = 0; i < 20_000; i++) out.push(all[Math.floor(r() * all.length)].toLowerCase());
    return out;
  } catch {
    return fallback;
  }
})();

/** Zipf CDF over VOCAB, precomputed once so sampling is a binary search. */
const ZIPF_CDF: Float64Array = (() => {
  const n = VOCAB.length;
  const cdf = new Float64Array(n);
  let acc = 0;
  for (let i = 0; i < n; i++) {
    acc += 1 / Math.pow(i + 1, 1.07);
    cdf[i] = acc;
  }
  for (let i = 0; i < n; i++) cdf[i] /= acc;
  return cdf;
})();

function zipfWord(r: () => number): string {
  const x = r();
  let lo = 0, hi = ZIPF_CDF.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (ZIPF_CDF[mid] < x) lo = mid + 1;
    else hi = mid;
  }
  return VOCAB[lo];
}

/** Natural-language-ish text with realistic word-frequency structure. */
export function text(r: () => number, words: number): string {
  const out: string[] = [];
  for (let i = 0; i < words; i++) out.push(zipfWord(r));
  return out.join(' ');
}

export interface Turn {
  role: string;
  content: string;
  model: string;
  ts: number;
  in_tok: number;
  out_tok: number;
  stop: string;
}

export function makeTurn(r: () => number, i: number, words: number): Turn {
  return {
    role: i % 2 === 0 ? 'user' : 'assistant',
    content: text(r, words),
    model: pick(r, MODELS),
    ts: 1_760_000_000_000 + i * 1500,
    in_tok: intBetween(r, 100, 60_000),
    out_tok: intBetween(r, 10, 4_000),
    stop: pick(r, ['end_turn', 'tool_use', 'max_tokens']),
  };
}

/**
 * Synthetic embedding corpus with the structure real text embeddings have.
 *
 * This generator matters more than it looks. Two tempting shortcuts both
 * produce dishonest recall numbers:
 *
 *   uniform random vectors      - near-orthogonal in 1536-d, so every pair is
 *                                 equidistant and every quantizer scores ~100%.
 *   tight clusters + iid noise  - the top-10 neighbours then differ *only* by
 *                                 high-frequency noise, so every lossy method
 *                                 scores ~0%, which is equally wrong.
 *
 * Real embeddings live on a low-dimensional manifold: documents are mixtures of
 * a limited number of latent topics, similarity falls off smoothly, and the
 * top-10 neighbours are genuinely closer than the bulk. So: a low-rank topic
 * model - each vector is a sparse, heavy-tailed mixture of `rank` random basis
 * directions plus a little noise, then L2-normalized.
 *
 * `describeCorpus` reports the resulting similarity distribution so the corpus
 * can be sanity-checked against a real one instead of taken on faith.
 */
export function gaussianFactory(r: () => number): () => number {
  return () => {
    let u = 0, v = 0;
    while (u === 0) u = r();
    while (v === 0) v = r();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  };
}

function normalize(v: Float32Array): Float32Array {
  let n = 0;
  for (let i = 0; i < v.length; i++) n += v[i] * v[i];
  n = Math.sqrt(n) || 1;
  for (let i = 0; i < v.length; i++) v[i] /= n;
  return v;
}

export interface Corpus {
  vectors: Float32Array[];
  /** Queries are perturbed documents - "find memories like this one". */
  queries: Float32Array[];
}

export function makeEmbeddingCorpus(
  seed: number,
  count: number,
  dim: number,
  nQueries: number,
  rank = 96,
  topicsPerDoc = 5,
  noise = 0.18,
  queryPerturb = 0.30,
): Corpus {
  const r = rng(seed);
  const g = gaussianFactory(r);

  const basis: Float32Array[] = [];
  for (let t = 0; t < rank; t++) {
    const b = new Float32Array(dim);
    for (let d = 0; d < dim; d++) b[d] = g();
    basis.push(normalize(b));
  }

  const vectors: Float32Array[] = [];
  for (let i = 0; i < count; i++) {
    const v = new Float32Array(dim);
    for (let t = 0; t < topicsPerDoc; t++) {
      const b = basis[Math.floor(r() * rank)];
      // heavy-tailed weight: a few topics dominate each document
      const w = Math.pow(r(), 2.2) * (r() < 0.5 ? -1 : 1);
      for (let d = 0; d < dim; d++) v[d] += b[d] * w;
    }
    for (let d = 0; d < dim; d++) v[d] += g() * noise * 0.05;
    vectors.push(normalize(v));
  }

  const rq = rng(seed ^ 0x5eed);
  const gq = gaussianFactory(rq);
  const queries: Float32Array[] = [];
  for (let i = 0; i < nQueries; i++) {
    const src = vectors[Math.floor(rq() * count)];
    const q = new Float32Array(dim);
    for (let d = 0; d < dim; d++) q[d] = src[d] + gq() * queryPerturb * 0.05;
    queries.push(normalize(q));
  }
  return { vectors, queries };
}

/** Similarity distribution of a corpus - the sanity check on the generator. */
export function describeCorpus(
  vectors: Float32Array[],
  queries: Float32Array[],
  k: number,
): Record<string, string> {
  const dot = (a: Float32Array, b: Float32Array) => {
    let s = 0;
    for (let i = 0; i < a.length; i++) s += a[i] * b[i];
    return s;
  };
  let top1 = 0, topK = 0, rand = 0;
  const r = rng(1234);
  for (const q of queries) {
    const sims = vectors.map((v) => dot(q, v)).sort((a, b) => b - a);
    top1 += sims[0];
    topK += sims.slice(0, k).reduce((a, b) => a + b, 0) / k;
    for (let i = 0; i < 50; i++) rand += sims[Math.floor(r() * sims.length)];
  }
  const n = queries.length;
  return {
    'mean cosine, top-1': (top1 / n).toFixed(3),
    [`mean cosine, top-${k}`]: (topK / n).toFixed(3),
    'mean cosine, random pair': (rand / (n * 50)).toFixed(3),
  };
}
