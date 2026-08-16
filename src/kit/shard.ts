/**
 * Sharded records — the biggest RAM win here (15.4×).
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
  /** Expected record count. Shard count is derived from it. */
  capacity: number;
  /** Namespace prefix, e.g. 'run' -> keys like `run:{7}`. */
  prefix: string;
}

/**
  * id -> value across shared hashes. O(1) round trips; a listpack shard is
  * scanned linearly, which is fine at 124 entries and not at 10,000.
  */
export class Pouch<V> {
  readonly shards: number;
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
    this.shards = Math.max(1, Math.ceil(opts.capacity / (opts.width ?? 124)));
    this.enc = opts.encode;
    this.dec = opts.decode;
  }

  /** The Redis key holding this id. Exposed so your Lua scripts can target it. */
  keyFor(id: string): string {
    return `${this.prefix}:{${fnv1a(id) % this.shards}}`;
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

  async get(id: string): Promise<V | null> {
    const b = (await this.r.cmd('HGET', this.keyFor(id), this.fieldFor(id))) as Buffer | null;
    return b ? this.dec(b) : null;
  }

  async del(id: string): Promise<boolean> {
    return num(await this.r.cmd('HDEL', this.keyFor(id), this.fieldFor(id))) === 1;
  }

  async has(id: string): Promise<boolean> {
    return num(await this.r.cmd('HEXISTS', this.keyFor(id), this.fieldFor(id))) === 1;
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

  /** Total records across every shard. One round trip per shard. */
  async count(): Promise<number> {
    const cmds: string[][] = [];
    for (let s = 0; s < this.shards; s++) cmds.push(['HLEN', `${this.prefix}:{${s}}`]);
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
