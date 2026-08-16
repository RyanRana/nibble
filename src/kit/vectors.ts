/**
 * Semantic memory.
 *
 * Use int8. Measured recall@10: int8 held 98.7–100% across a full sweep of
 * corpus difficulty; binary ranged 83% -> 32% on identical code. Vector Sets
 * discard the source vector, so no rescoring path exists. docs/embeddings.md.
 */
import { type Client, type Reply, num, str } from './client.ts';
import {
  encodeF32, encodeF16, decodeF16, encodeInt8, decodeInt8, encodeBinary, cosine,
} from '../lib/quantize.ts';

export type Quant = 'f32' | 'f16' | 'int8' | 'binary';

/** Bytes per vector for raw storage, before any Redis overhead. */
export function storageBytes(dim: number, quant: Quant): number {
  switch (quant) {
    case 'f32': return dim * 4;
    case 'f16': return dim * 2;
    case 'int8': return dim + 4;
    case 'binary': return Math.ceil(dim / 8);
  }
}

export function encodeVector(v: Float32Array, quant: Quant): Buffer {
  switch (quant) {
    case 'f32': return encodeF32(v);
    case 'f16': return encodeF16(v);
    case 'int8': return encodeInt8(v);
    case 'binary': return encodeBinary(v);
  }
}

export function decodeVector(b: Buffer, quant: Quant): Float32Array {
  switch (quant) {
    case 'f16': return decodeF16(b);
    case 'int8': return decodeInt8(b);
    case 'f32': {
      const copy = Buffer.from(b);
      return new Float32Array(copy.buffer, copy.byteOffset, copy.length / 4);
    }
    case 'binary':
      throw new Error('binary codes are not invertible — compare with hamming(), do not decode');
  }
}

export interface VectorSetOpts {
  key: string;
  dim: number;
  /**
   * 'Q8' (int8, the default and the right answer), 'NOQUANT' (f32), or 'BIN'.
   * Measured per-vector cost at dim=1536 including the HNSW graph:
   *   NOQUANT 6,900 B · Q8 2,291 B · BIN 947 B
   */
  quant?: 'Q8' | 'NOQUANT' | 'BIN';
  /**
   * HNSW connectivity. Default 16. The graph is a fixed ~758 B/vector at M=16,
   * so at BIN it is 80% of your cost — dropping to M=8 saves ~150 B/vector and
   * is the only lever left once you have quantized.
   */
  m?: number;
  /** Search effort. Higher = better recall, slower. Sensible range 50–1000. */
  ef?: number;
}

/**
 * Thin wrapper over Redis 8 Vector Sets (VADD/VSIM). Searchable — the HNSW
 * graph is included in the cost, unlike the raw-storage helpers above.
 */
export class VectorMemory {
  private readonly r: Client;
  private readonly key: string;
  readonly dim: number;
  private readonly quant: 'Q8' | 'NOQUANT' | 'BIN';
  private readonly m: number;
  private readonly ef: number;

  constructor(r: Client, opts: VectorSetOpts) {
    this.r = r;
    this.key = opts.key;
    this.dim = opts.dim;
    this.quant = opts.quant ?? 'Q8';
    this.m = opts.m ?? 16;
    this.ef = opts.ef ?? 200;
  }

  async add(id: string, v: Float32Array, attrs?: Record<string, unknown>): Promise<boolean> {
    if (v.length !== this.dim) {
      throw new Error(`expected ${this.dim}-d vector, got ${v.length}`);
    }
    const args: (string | Buffer)[] = [
      'VADD', this.key, 'FP32', encodeF32(v), id, this.quant, 'M', String(this.m),
    ];
    if (attrs) args.push('SETATTR', JSON.stringify(attrs));
    return num(await this.r.cmd(...args)) === 1;
  }

  /** Nearest ids with cosine scores, highest first. */
  async search(q: Float32Array, count = 10, filter?: string): Promise<{ id: string; score: number }[]> {
    const args: (string | Buffer)[] = [
      'VSIM', this.key, 'FP32', encodeF32(q), 'WITHSCORES',
      'COUNT', String(count), 'EF', String(this.ef),
    ];
    if (filter) args.push('FILTER', filter);
    const flat = (await this.r.cmd(...args)) as Reply[];
    const out: { id: string; score: number }[] = [];
    for (let i = 0; i < flat.length; i += 2) {
      out.push({ id: str(flat[i]), score: num(flat[i + 1]) });
    }
    return out;
  }

  async remove(id: string): Promise<boolean> {
    return num(await this.r.cmd('VREM', this.key, id)) === 1;
  }

  async size(): Promise<number> {
    return num(await this.r.cmd('VCARD', this.key));
  }

  async info(): Promise<Record<string, string>> {
    const flat = (await this.r.cmd('VINFO', this.key)) as Reply[];
    const o: Record<string, string> = {};
    for (let i = 0; i < flat.length; i += 2) o[str(flat[i])] = str(flat[i + 1]);
    return o;
  }
}

/**
 * recall@k of your own pipeline against exact float32 cosine.
 *
 * Run this on YOUR embeddings before trusting any compression choice — ours
 * included. It is twenty lines and it is the difference between a saving and a
 * silent regression.
 */
export async function measureRecall(
  search: (q: Float32Array, k: number) => Promise<string[]>,
  corpus: { id: string; vector: Float32Array }[],
  queries: Float32Array[],
  k = 10,
): Promise<number> {
  let hits = 0;
  for (const q of queries) {
    const exact = corpus
      .map((c) => ({ id: c.id, s: cosine(q, c.vector) }))
      .sort((a, b) => b.s - a.s)
      .slice(0, k);
    const truth = new Set(exact.map((e) => e.id));
    for (const id of await search(q, k)) if (truth.has(id)) hits++;
  }
  return hits / (queries.length * k);
}
