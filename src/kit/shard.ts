/**
 * Sharded records - the biggest RAM win here (15.4×).
 *
 * A Redis key costs 60–90 B before your data. Put 124 records in one hash and
 * you pay that once per 124, not once each. See docs/records.md.
 */
import { type Client, type Reply, num, isErr } from './client.ts';

/** FNV-1a over the id. Stable across processes, restarts and languages. */
export function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export interface ShardOpts {
  /** Records per shard. 124 bottomed out the sweep for ~110 B records; re-sweep for yours. */
  width?: number;
  /** Expected record count. Shard count is derived from it, rounded up to a power of two. */
  capacity: number;
  /** Namespace prefix, e.g. 'run' -> keys like `run:{7}`. */
  prefix: string;
}

/**
 * Shard counts are always powers of two so that growing is a SPLIT.
 *
 * With `h % 2^k`, incrementing k sends each key either to the shard it is
 * already in, or to `s + 2^k`. Nothing else moves. Half the keys migrate
 * instead of all of them, one shard at a time, online. Any other shard count
 * makes growth a full reshuffle, which in practice means a flush.
 */
export function shardCountFor(capacity: number, width = 124): number {
  const wanted = Math.max(1, Math.ceil(capacity / width));
  return 2 ** Math.ceil(Math.log2(wanted));
}

/** `fieldFor` is a bijection, so a field recovers the id the resharder needs. */
export function idFromField(f: Buffer): string {
  if (f.length !== 16) return f.toString('utf8');
  const h = f.toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

/** Generation marker: "k" normally, "k:prev" while a split is draining. */
export const genKeyFor = (prefix: string) => `${prefix}:gen`;

/**
  * id -> value across shared hashes. O(1) round trips; a listpack shard is
  * scanned linearly, which is fine at 124 entries and not at 10,000.
  */
export class Pouch<V> {
  /** log2 of the live shard count. */
  private k: number;
  /** Set only while a split is draining, so readers know to check both homes. */
  private prevK: number | null = null;
  private readonly r: Client;
  private readonly prefix: string;
  private readonly enc: (v: V) => Buffer | string;
  private readonly dec: (b: Buffer) => V;

  constructor(
    r: Client,
    opts: ShardOpts & { encode: (v: V) => Buffer | string; decode: (b: Buffer) => V },
  ) {
    this.r = r;
    this.prefix = opts.prefix;
    this.k = Math.log2(shardCountFor(opts.capacity, opts.width ?? 124));
    this.enc = opts.encode;
    this.dec = opts.decode;
  }

  get shards(): number {
    return 2 ** this.k;
  }

  /**
   * Read the published generation. Call at startup and periodically; a client
   * running one generation behind is still CORRECT (it writes to the old home,
   * which the drain will pick up), just slower to converge.
   */
  async syncGeneration(): Promise<void> {
    const raw = (await this.r.cmd('GET', genKeyFor(this.prefix))) as Buffer | null;
    if (!raw) return;
    const [k, prev] = raw.toString().split(':');
    this.k = Number(k);
    this.prevK = prev === undefined || prev === '' ? null : Number(prev);
  }

  /** The Redis key holding this id. Exposed so your Lua scripts can target it. */
  keyFor(id: string): string {
    return `${this.prefix}:{${fnv1a(id) % 2 ** this.k}}`;
  }

  /** The pre-split home, non-null only while a split is draining. */
  prevKeyFor(id: string): string | null {
    if (this.prevK === null) return null;
    const old = `${this.prefix}:{${fnv1a(id) % 2 ** this.prevK}}`;
    return old === this.keyFor(id) ? null : old;
  }

  /** Field name inside the shard. 16 raw bytes when the id is a UUID. */
  fieldFor(id: string): Buffer | string {
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)
      ? Buffer.from(id.replace(/-/g, ''), 'hex')
      : id;
  }

  async set(id: string, value: V): Promise<void> {
    await this.r.cmd('HSET', this.keyFor(id), this.fieldFor(id), this.enc(value));
  }

  /** New home first, then the pre-split home while a split is draining. */
  async get(id: string): Promise<V | null> {
    const f = this.fieldFor(id);
    const b = (await this.r.cmd('HGET', this.keyFor(id), f)) as Buffer | null;
    if (b) return this.dec(b);
    const old = this.prevKeyFor(id);
    if (!old) return null;
    const b2 = (await this.r.cmd('HGET', old, f)) as Buffer | null;
    return b2 ? this.dec(b2) : null;
  }

  /** Deletes must hit both homes, or a drained-later copy resurrects the record. */
  async del(id: string): Promise<boolean> {
    const f = this.fieldFor(id);
    const old = this.prevKeyFor(id);
    if (!old) return num(await this.r.cmd('HDEL', this.keyFor(id), f)) === 1;
    const res = await this.r.pipeline([['HDEL', this.keyFor(id), f], ['HDEL', old, f]]);
    return res.some((v) => num(v) === 1);
  }

  async has(id: string): Promise<boolean> {
    if (num(await this.r.cmd('HEXISTS', this.keyFor(id), this.fieldFor(id))) === 1) return true;
    const old = this.prevKeyFor(id);
    return old ? num(await this.r.cmd('HEXISTS', old, this.fieldFor(id))) === 1 : false;
  }

  /** Groups ids by shard: N ids cost min(N, shards) HMGETs, not N round trips. */
  async mget(ids: string[]): Promise<(V | null)[]> {
    const byShard = new Map<string, { id: string; at: number }[]>();
    ids.forEach((id, at) => {
      const k = this.keyFor(id);
      const g = byShard.get(k);
      if (g) g.push({ id, at });
      else byShard.set(k, [{ id, at }]);
    });

    const out: (V | null)[] = new Array(ids.length).fill(null);
    const cmds: (string | Buffer)[][] = [];
    const order: { id: string; at: number }[][] = [];
    for (const [key, group] of byShard) {
      cmds.push(['HMGET', key, ...group.map((g) => this.fieldFor(g.id))]);
      order.push(group);
    }
    const res = await this.r.pipeline(cmds);
    res.forEach((vals, i) => {
      if (isErr(vals)) throw new Error(vals.err);
      (vals as (Buffer | null)[]).forEach((b, j) => {
        if (b) out[order[i][j].at] = this.dec(b);
      });
    });
    return out;
  }

  /** Total records across every shard, including any not yet drained. */
  async count(): Promise<number> {
    const n = 2 ** Math.max(this.k, this.prevK ?? 0);
    const cmds: string[][] = [];
    for (let s = 0; s < n; s++) cmds.push(['HLEN', `${this.prefix}:{${s}}`]);
    const res = await this.r.pipeline(cmds);
    return res.reduce((a: number, v: Reply) => a + num(v), 0);
  }

  /** hash-max-listpack-entries must exceed the width or shards silently become hashtables. */
  static configFor(width = 124, maxValueBytes = 512): Record<string, string> {
    return {
      'hash-max-listpack-entries': String(Math.max(128, width * 2)),
      'hash-max-listpack-value': String(maxValueBytes),
    };
  }
}
