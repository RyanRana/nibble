/**
 * Distributed agent cache. Makes a model call disappear, not just a byte.
 *
 * Handles the four things that break caches at scale: stampedes (Lua lease),
 * synchronized expiry (TTL jitter), paraphrased prompts (semantic tier), and
 * cluster slots (hash tags). See docs/caching.md.
 */
import zlib from 'node:zlib';
import { type Client, type Reply, num, str, isErr } from './client.ts';
import { fnv1a } from './shard.ts';
import { encodeF32 } from '../lib/quantize.ts';

export interface AgentCacheOpts {
  /** Namespace. Keys become `<prefix>:e:{shard}` etc. */
  prefix: string;
  /** Expected live entries - shard count derives from it. */
  capacity: number;
  /** Base TTL in seconds. */
  ttl: number;
  /** Records per shard. See docs/tuning.md - sweep it. */
  width?: number;
  /**
   * TTL jitter as a fraction, default 0.15. An entry's real TTL is
   * `ttl × (1 ± jitter)`, so a batch written together does not expire together.
   */
  jitter?: number;
  /** zstd the cached values. Worth it above ~200 B; LLM responses always are. */
  compress?: boolean;
  /** Enable the semantic tier. Requires embeddings at get/set time. */
  semantic?: { dim: number; threshold: number; quant?: 'Q8' | 'BIN' | 'NOQUANT' };
  /** How long one worker may hold the compute lease, ms. Default 30s. */
  leaseMs?: number;
}

export type HitKind = 'exact' | 'semantic' | 'miss';

export interface CacheResult {
  kind: HitKind;
  value: Buffer | null;
  /** Similarity score, semantic hits only. */
  score?: number;
}

/** 1=hit, 2=you compute, 3=someone else is. Atomic: a client-side GET-then-SETNX races. */
const GET_OR_LEASE = `
local v = redis.call('HGET', KEYS[1], ARGV[1])
if v then return {1, v} end
if redis.call('SET', KEYS[2], ARGV[3], 'NX', 'PX', ARGV[2]) then
  return {2, ''}
end
return {3, ''}`;

/** Publish the value with its TTL, release our lease. */
const SET_AND_RELEASE = `
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
redis.call('HEXPIRE', KEYS[1], ARGV[2], 'FIELDS', 1, ARGV[1])
if redis.call('GET', KEYS[2]) == ARGV[3] then
  redis.call('DEL', KEYS[2])
end
return 1`;

export interface CacheStats {
  exact: number;
  semantic: number;
  miss: number;
  computed: number;
  waited: number;
  leaseTimeouts: number;
}

export class AgentCache {
  readonly shards: number;
  readonly stats: CacheStats = {
    exact: 0, semantic: 0, miss: 0, computed: 0, waited: 0, leaseTimeouts: 0,
  };

  private readonly r: Client;
  private readonly prefix: string;
  private readonly ttl: number;
  private readonly jitter: number;
  private readonly compress: boolean;
  private readonly leaseMs: number;
  private readonly semantic: AgentCacheOpts['semantic'];
  private getOrLease = '';
  private setRelease = '';

  constructor(r: Client, opts: AgentCacheOpts) {
    this.r = r;
    this.prefix = opts.prefix;
    this.ttl = opts.ttl;
    this.jitter = opts.jitter ?? 0.15;
    this.compress = opts.compress ?? true;
    this.leaseMs = opts.leaseMs ?? 30_000;
    this.semantic = opts.semantic;
    this.shards = Math.max(1, Math.ceil(opts.capacity / (opts.width ?? 124)));
  }

  /** Load the Lua once. Safe to call repeatedly. */
  async init(): Promise<void> {
    if (this.getOrLease) return;
    this.getOrLease = str(await this.r.cmd('SCRIPT', 'LOAD', GET_OR_LEASE));
    this.setRelease = str(await this.r.cmd('SCRIPT', 'LOAD', SET_AND_RELEASE));
  }

  // ── key layout - note every key shares the {shard} tag ──────────────
  private shardOf(key: string): number {
    return fnv1a(key) % this.shards;
  }
  private entryKey(key: string): string {
    return `${this.prefix}:e:{${this.shardOf(key)}}`;
  }
  private leaseKey(key: string): string {
    return `${this.prefix}:l:{${this.shardOf(key)}}:${fnv1a(key).toString(36)}`;
  }
  /** 8 raw bytes, not a 64-char hex digest. */
  private field(key: string): Buffer {
    const b = Buffer.allocUnsafe(8);
    b.writeUInt32BE(fnv1a(key), 0);
    b.writeUInt32BE(fnv1a(key + 'salt'), 4);
    return b;
  }
  private semanticKey(): string {
    return `${this.prefix}:v`;
  }

  /** Spread expiry so a batch written together does not expire together. */
  private jitteredTtl(): number {
    const spread = 1 + (Math.random() * 2 - 1) * this.jitter;
    return Math.max(1, Math.round(this.ttl * spread));
  }

  private pack(v: Buffer | string): Buffer {
    const b = Buffer.isBuffer(v) ? v : Buffer.from(v);
    return this.compress ? zlib.zstdCompressSync(b) : b;
  }
  private unpack(b: Buffer): Buffer {
    return this.compress ? zlib.zstdDecompressSync(b) : b;
  }

  /** Exact-key lookup only. One round trip. */
  async getExact(key: string): Promise<Buffer | null> {
    const v = (await this.r.cmd('HGET', this.entryKey(key), this.field(key))) as Buffer | null;
    return v ? this.unpack(v) : null;
  }

  /** Exact first - one round trip and it cannot be wrong. Then semantic. */
  async get(key: string, embedding?: Float32Array): Promise<CacheResult> {
    const exact = await this.getExact(key);
    if (exact) {
      this.stats.exact++;
      return { kind: 'exact', value: exact };
    }
    if (this.semantic && embedding) {
      const hit = await this.searchSemantic(embedding);
      if (hit) {
        const v = await this.getExact(hit.key);
        if (v) {
          this.stats.semantic++;
          return { kind: 'semantic', value: v, score: hit.score };
        }
        // index entry outlived the value - clean it up rather than re-hitting it
        await this.r.raw('VREM', this.semanticKey(), hit.key);
      }
    }
    this.stats.miss++;
    return { kind: 'miss', value: null };
  }

  private async searchSemantic(
    embedding: Float32Array,
  ): Promise<{ key: string; score: number } | null> {
    const res = await this.r.raw(
      'VSIM', this.semanticKey(), 'FP32', encodeF32(embedding),
      'WITHSCORES', 'COUNT', '1', 'EF', '100',
    );
    if (isErr(res) || !Array.isArray(res) || res.length < 2) return null;
    const score = num(res[1]);
    return score >= (this.semantic!.threshold) ? { key: str(res[0]), score } : null;
  }

  async set(key: string, value: Buffer | string, embedding?: Float32Array): Promise<void> {
    await this.init();
    const packed = this.pack(value);
    const cmds: (string | Buffer)[][] = [
      ['HSET', this.entryKey(key), this.field(key), packed],
      ['HEXPIRE', this.entryKey(key), String(this.jitteredTtl()), 'FIELDS', '1', this.field(key)],
    ];
    if (this.semantic && embedding) {
      cmds.push([
        'VADD', this.semanticKey(), 'FP32', encodeF32(embedding), key,
        this.semantic.quant ?? 'Q8',
      ]);
    }
    const res = await this.r.pipeline(cmds);
    for (const v of res) if (isErr(v)) throw new Error(v.err);
  }

  /**
   * Get-or-compute, exactly once across the fleet. One worker wins the lease
   * and runs `compute`; the rest wait. The lease has a TTL so a dead winner
   * doesn't wedge the key.
   */
  async fetch(
    key: string,
    compute: () => Promise<Buffer | string>,
    embedding?: Float32Array,
  ): Promise<CacheResult> {
    await this.init();

    if (this.semantic && embedding) {
      const pre = await this.get(key, embedding);
      if (pre.kind !== 'miss') return pre;
    }

    const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const res = (await this.r.cmd(
      'EVALSHA', this.getOrLease, '2',
      this.entryKey(key), this.leaseKey(key),
      this.field(key), String(this.leaseMs), token,
    )) as Reply[];

    const code = num(res[0]);
    if (code === 1) {
      this.stats.exact++;
      return { kind: 'exact', value: this.unpack(res[1] as Buffer) };
    }

    if (code === 2) {
      // we own the lease: compute, publish, release
      this.stats.miss++;
      this.stats.computed++;
      const value = await compute();
      const packed = this.pack(value);
      await this.r.cmd(
        'EVALSHA', this.setRelease, '2',
        this.entryKey(key), this.leaseKey(key),
        this.field(key), String(this.jitteredTtl()), token, packed,
      );
      if (this.semantic && embedding) {
        await this.r.raw(
          'VADD', this.semanticKey(), 'FP32', encodeF32(embedding), key,
          this.semantic.quant ?? 'Q8',
        );
      }
      return { kind: 'miss', value: packed && this.unpack(packed) };
    }

    // someone else is computing - wait for them instead of duplicating the work
    this.stats.waited++;
    const deadline = Date.now() + this.leaseMs;
    let delay = 5;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(100, Math.round(delay * 1.6)); // backoff, capped
      const v = await this.getExact(key);
      if (v) {
        this.stats.exact++;
        return { kind: 'exact', value: v };
      }
      const holder = await this.r.cmd('GET', this.leaseKey(key));
      if (holder === null) {
        // Lease gone = winner published, or winner died. Re-check the value to
        // tell them apart; skipping this makes one worker in 200 compute twice.
        const late = await this.getExact(key);
        if (late) {
          this.stats.exact++;
          return { kind: 'exact', value: late };
        }
        break; // genuinely abandoned - take over below
      }
    }
    // The holder died without publishing. Compute rather than fail the request.
    this.stats.leaseTimeouts++;
    const fallback = await compute();
    await this.set(key, fallback, embedding);
    return {
      kind: 'miss',
      value: Buffer.isBuffer(fallback) ? fallback : Buffer.from(fallback),
    };
  }

  get hitRate(): number {
    const total = this.stats.exact + this.stats.semantic + this.stats.miss;
    return total ? (this.stats.exact + this.stats.semantic) / total : 0;
  }

  /** Live entry count across every shard. */
  async size(): Promise<number> {
    const cmds: string[][] = [];
    for (let s = 0; s < this.shards; s++) cmds.push(['HLEN', `${this.prefix}:e:{${s}}`]);
    return (await this.r.pipeline(cmds)).reduce((a: number, v) => a + num(v), 0);
  }
}
