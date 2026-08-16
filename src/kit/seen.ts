/**
 * "Have I already done this?" at 9 B/id exact, or 1.4 B probabilistic.
 *
 * ExactSeen hashes to 52 bits and shards so each SET stays an intset.
 * BloomSeen is a front door for an authoritative check, never the check -
 * its "no" is always true, its "yes" usually is.
 */
import { type Client, type Reply, num, isErr } from './client.ts';

/** 52-bit: widest that stays an exact JS integer. Collisions ~n²/2^53 - ~1 in 36 at 500k ids. */
export function hash52(s: string): number {
  let h1 = 0x811c9dc5, h2 = 0x01000193;
  for (let i = 0; i < s.length; i++) {
    h1 = Math.imul(h1 ^ s.charCodeAt(i), 0x01000193);
    h2 = Math.imul(h2 ^ s.charCodeAt(i), 0x85ebca6b);
  }
  return (h1 >>> 0) * 0x100000 + (h2 >>> 12);
}

export interface SeenOpts {
  prefix: string;
  capacity: number;
  /** Must stay under set-max-intset-entries (512) or the shard becomes a hashtable. */
  width?: number;
}

export class ExactSeen {
  readonly shards: number;
  private readonly r: Client;
  private readonly prefix: string;

  constructor(r: Client, opts: SeenOpts) {
    this.r = r;
    this.prefix = opts.prefix;
    this.shards = Math.max(1, Math.ceil(opts.capacity / (opts.width ?? 244)));
  }

  private keyFor(h: number): string {
    return `${this.prefix}:{${h % this.shards}}`;
  }

  /** True if this id had NOT been seen before - i.e. you should do the work. */
  async add(id: string): Promise<boolean> {
    const h = hash52(id);
    return num(await this.r.cmd('SADD', this.keyFor(h), String(h))) === 1;
  }

  async has(id: string): Promise<boolean> {
    const h = hash52(id);
    return num(await this.r.cmd('SISMEMBER', this.keyFor(h), String(h))) === 1;
  }

  async remove(id: string): Promise<boolean> {
    const h = hash52(id);
    return num(await this.r.cmd('SREM', this.keyFor(h), String(h))) === 1;
  }

  async addMany(ids: string[]): Promise<void> {
    const buckets = new Map<string, string[]>();
    for (const id of ids) {
      const h = hash52(id);
      const k = this.keyFor(h);
      const g = buckets.get(k);
      if (g) g.push(String(h));
      else buckets.set(k, [String(h)]);
    }
    const cmds: string[][] = [];
    for (const [k, members] of buckets) cmds.push(['SADD', k, ...members]);
    const res = await this.r.pipeline(cmds);
    for (const v of res) if (isErr(v)) throw new Error(v.err);
  }

  async count(): Promise<number> {
    const cmds: string[][] = [];
    for (let s = 0; s < this.shards; s++) cmds.push(['SCARD', `${this.prefix}:{${s}}`]);
    const res = await this.r.pipeline(cmds);
    return res.reduce((a: number, v) => a + num(v), 0);
  }
}

export interface BloomOpts {
  key: string;
  capacity: number;
  /** False-positive rate. 0.01 measured 1.4 B/id; 0.001 measured 1.9 B/id. */
  errorRate?: number;
}

export class BloomSeen {
  private readonly r: Client;
  private readonly key: string;
  private readonly capacity: number;
  private readonly errorRate: number;

  constructor(r: Client, opts: BloomOpts) {
    this.r = r;
    this.key = opts.key;
    this.capacity = opts.capacity;
    this.errorRate = opts.errorRate ?? 0.01;
  }

  /** NONSCALING: a scaling filter silently chains more filters as it fills. */
  async init(): Promise<void> {
    const res = await this.r.raw(
      'BF.RESERVE', this.key, String(this.errorRate), String(this.capacity), 'NONSCALING',
    );
    if (isErr(res) && !/exists/i.test(res.err)) throw new Error(res.err);
  }

  /** True if newly added. A `false` here may be a false positive - see above. */
  async add(id: string): Promise<boolean> {
    return num(await this.r.cmd('BF.ADD', this.key, id)) === 1;
  }

  /** `false` is definitive. `true` is probable. */
  async has(id: string): Promise<boolean> {
    return num(await this.r.cmd('BF.EXISTS', this.key, id)) === 1;
  }

  async addMany(ids: string[]): Promise<boolean[]> {
    const out: boolean[] = [];
    for (let i = 0; i < ids.length; i += 1000) {
      const res = (await this.r.cmd('BF.MADD', this.key, ...ids.slice(i, i + 1000))) as Reply[];
      for (const v of res) out.push(num(v) === 1);
    }
    return out;
  }
}
