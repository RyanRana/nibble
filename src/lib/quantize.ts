/**
 * Vector quantization + recall measurement.
 *
 * Compression without a recall number, measured on a representative corpus,
 * is not a result. See src/bench/vector-rank-study.ts.
 */

// ─────────────────────────── float16 (half) ────────────────────────────

export function f32to16(v: number): number {
  const f = new Float32Array(1);
  const i = new Int32Array(f.buffer);
  f[0] = v;
  const x = i[0];
  const sign = (x >>> 16) & 0x8000;
  let exp = (x >>> 23) & 0xff;
  let mant = x & 0x7fffff;
  if (exp === 255) return sign | 0x7c00 | (mant ? 1 : 0);
  exp = exp - 127 + 15;
  if (exp >= 31) return sign | 0x7c00;
  if (exp <= 0) {
    if (exp < -10) return sign;
    mant = (mant | 0x800000) >> (1 - exp);
    return sign | (mant >> 13);
  }
  return sign | (exp << 10) | (mant >> 13);
}

export function f16to32(h: number): number {
  const sign = (h & 0x8000) << 16;
  let exp = (h >>> 10) & 0x1f;
  let mant = h & 0x3ff;
  if (exp === 0) {
    if (mant === 0) {
      const f = new Float32Array(1);
      new Int32Array(f.buffer)[0] = sign;
      return f[0];
    }
    while (!(mant & 0x400)) {
      mant <<= 1;
      exp--;
    }
    exp++;
    mant &= 0x3ff;
  } else if (exp === 31) {
    const f = new Float32Array(1);
    new Int32Array(f.buffer)[0] = sign | 0x7f800000 | (mant << 13);
    return f[0];
  }
  const f = new Float32Array(1);
  new Int32Array(f.buffer)[0] = sign | ((exp + 112) << 23) | (mant << 13);
  return f[0];
}

// ────────────────────────────── encoders ───────────────────────────────

export function encodeF32(v: Float32Array): Buffer {
  return Buffer.from(v.buffer, v.byteOffset, v.byteLength);
}
export function decodeF32(b: Buffer): Float32Array {
  const copy = Buffer.from(b); // guarantee 4-byte alignment
  return new Float32Array(copy.buffer, copy.byteOffset, copy.length / 4);
}

export function encodeF16(v: Float32Array): Buffer {
  const b = Buffer.allocUnsafe(v.length * 2);
  for (let i = 0; i < v.length; i++) b.writeUInt16LE(f32to16(v[i]), i * 2);
  return b;
}
export function decodeF16(b: Buffer): Float32Array {
  const out = new Float32Array(b.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = f16to32(b.readUInt16LE(i * 2));
  return out;
}

/** Symmetric int8: one float32 scale in the header, then one byte per dim. */
export function encodeInt8(v: Float32Array): Buffer {
  let max = 0;
  for (let i = 0; i < v.length; i++) {
    const a = Math.abs(v[i]);
    if (a > max) max = a;
  }
  const scale = max / 127 || 1e-9;
  const b = Buffer.allocUnsafe(4 + v.length);
  b.writeFloatLE(scale, 0);
  for (let i = 0; i < v.length; i++) {
    b.writeInt8(Math.max(-127, Math.min(127, Math.round(v[i] / scale))), 4 + i);
  }
  return b;
}
export function decodeInt8(b: Buffer): Float32Array {
  const scale = b.readFloatLE(0);
  const out = new Float32Array(b.length - 4);
  for (let i = 0; i < out.length; i++) out[i] = b.readInt8(4 + i) * scale;
  return out;
}

/** Sign-bit binary quantization: 1 bit per dimension, 32× smaller than f32. */
export function encodeBinary(v: Float32Array): Buffer {
  const b = Buffer.alloc(Math.ceil(v.length / 8));
  for (let i = 0; i < v.length; i++) {
    if (v[i] > 0) b[i >> 3] |= 1 << (i & 7);
  }
  return b;
}

const POPCNT = new Uint8Array(256);
for (let i = 0; i < 256; i++) POPCNT[i] = (i & 1) + POPCNT[i >> 1];

export function hamming(a: Buffer, b: Buffer): number {
  let d = 0;
  for (let i = 0; i < a.length; i++) d += POPCNT[a[i] ^ b[i]];
  return d;
}

// ─────────────────────────────── metrics ────────────────────────────────

export function cosine(a: Float32Array, b: Float32Array): number {
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    na += a[i] * a[i];
    nb += b[i] * b[i];
  }
  return dot / (Math.sqrt(na) * Math.sqrt(nb) || 1);
}

export function topK(scores: Float64Array, k: number): number[] {
  const idx = Array.from(scores.keys());
  idx.sort((x, y) => scores[y] - scores[x]);
  return idx.slice(0, k);
}

/**
 * Exact float32 cosine top-k per query. Computed once and reused by every
 * quantizer's recall measurement - it is by far the most expensive step, and
 * recomputing it per variant would dominate the benchmark's runtime.
 */
export function exactTopK(
  vectors: Float32Array[],
  queries: Float32Array[],
  k: number,
): Set<number>[] {
  return queries.map((q) => {
    const s = new Float64Array(vectors.length);
    for (let d = 0; d < vectors.length; d++) s[d] = cosine(q, vectors[d]);
    return new Set(topK(s, k));
  });
}

/**
 * recall@k of an approximate scorer against precomputed exact ground truth.
 *
 * `rerank` models the production two-stage pattern: retrieve a wide candidate
 * set cheaply (binary/hamming), then rescore just those candidates with a more
 * faithful representation. It is how you get 32× compression without paying for
 * it in quality.
 */
export function recallAtK(
  truth: Set<number>[],
  nDocs: number,
  k: number,
  approxScore: (qi: number, di: number) => number,
  rerank?: { widen: number; score: (qi: number, di: number) => number },
): number {
  let hits = 0;
  for (let qi = 0; qi < truth.length; qi++) {
    const approx = new Float64Array(nDocs);
    for (let d = 0; d < nDocs; d++) approx[d] = approxScore(qi, d);

    let cand = topK(approx, rerank ? k * rerank.widen : k);
    if (rerank) {
      const rescored = new Float64Array(nDocs).fill(-Infinity);
      for (const d of cand) rescored[d] = rerank.score(qi, d);
      cand = topK(rescored, k);
    }
    for (const c of cand) if (truth[qi].has(c)) hits++;
  }
  return hits / (truth.length * k);
}
