/**
 * CASE 07 - Secondary indexes (tag → runs, agent → runs, status → runs).
 *
 * If Redis is your primary store you need indexes, and indexes are pure
 * overhead: they hold no data of their own, only pointers. So their encoding
 * is the whole cost.
 *
 * The decisive variable is **id density**. Sets of opaque UUIDs are the worst
 * case. Dense integer ids unlock intsets, and - once a tag covers a meaningful
 * fraction of the id space - bitmaps, where a member costs a *bit*.
 * The crossover is measured here rather than asserted: a bitmap over a sparse
 * tag is worse than a set, and this case shows both sides of that line.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { rng, uuidLike } from '../lib/rng.ts';

const TAGS = 200;
const PER_TAG = 5_000;
const TOTAL = TAGS * PER_TAG; // 1,000,000 memberships
const ID_SPACE = 250_000; // dense integer id space: each tag covers 2% of it
const SEED = 606060;

const r0 = rng(SEED);
const UUIDS = Array.from({ length: ID_SPACE }, () => uuidLike(r0));

/** membership[t] = the dense integer ids carrying tag t */
const MEMBERSHIP: number[][] = (() => {
  const r = rng(SEED + 1);
  return Array.from({ length: TAGS }, () => {
    const s = new Set<number>();
    while (s.size < PER_TAG) s.add(Math.floor(r() * ID_SPACE));
    return [...s].sort((a, b) => a - b);
  });
})();

/** A deliberately *dense* tag set: 60% of a small id space (e.g. status=running). */
const DENSE_SPACE = 20_000;
const DENSE: number[][] = (() => {
  const r = rng(SEED + 2);
  return Array.from({ length: 50 }, () => {
    const out: number[] = [];
    for (let i = 0; i < DENSE_SPACE; i++) if (r() < 0.6) out.push(i);
    return out;
  });
})();
const DENSE_TOTAL = DENSE.reduce((a, x) => a + x.length, 0);

export const case07: BenchCase = {
  id: '07-index',
  title: `Secondary index (${TAGS} tags × ${PER_TAG.toLocaleString()} = ${TOTAL.toLocaleString()} memberships)`,
  question: 'Indexes store pointers, not data. What does one membership cost?',
  unit: 'membership',

  variants: [
    {
      name: 'A · SET of UUID strings',
      note: 'SADD tag → run-uuid. Every index entry re-stores a 36-char id.',
      load: async (r: Redis) => {
        for (let t = 0; t < TAGS; t++) {
          const mem = MEMBERSHIP[t].map((i) => UUIDS[i]);
          for (let i = 0; i < mem.length; i += 1000) {
            await r.sadd(`tag:${t}`, ...mem.slice(i, i + 1000));
          }
        }
        return TOTAL;
      },
      encodingProbes: ['tag:0'],
    },
    {
      name: 'B · ZSET of UUID strings (score = ts)',
      note: 'The "recent runs for this tag" index. Ordering is useful; the skiplist is not free.',
      load: async (r: Redis) => {
        for (let t = 0; t < TAGS; t++) {
          const cmds: any[] = [];
          for (let i = 0; i < MEMBERSHIP[t].length; i += 500) {
            const a: (string | number)[] = ['ZADD', `tag:${t}`];
            for (const id of MEMBERSHIP[t].slice(i, i + 500)) a.push(id, UUIDS[id]);
            cmds.push(a);
          }
          await pipe(r, cmds, 100);
        }
        return TOTAL;
      },
      encodingProbes: ['tag:0'],
    },
    {
      name: 'C · SET of dense integer ids',
      note: 'Same index over an internal dense int id. Integer members are stored inline.',
      load: async (r: Redis) => {
        for (let t = 0; t < TAGS; t++) {
          const mem = MEMBERSHIP[t].map(String);
          for (let i = 0; i < mem.length; i += 1000) {
            await r.sadd(`tag:${t}`, ...mem.slice(i, i + 1000));
          }
        }
        return TOTAL;
      },
      encodingProbes: ['tag:0'],
    },
    {
      name: 'D · sharded intsets of dense ids',
      note: 'Shard each tag so every shard stays under set-max-intset-entries: a packed sorted int array.',
      load: async (r: Redis) => {
        const perShard = 400;
        const cmds: any[] = [];
        for (let t = 0; t < TAGS; t++) {
          const m = MEMBERSHIP[t];
          for (let i = 0; i < m.length; i += perShard) {
            cmds.push([`SADD`, `tag:${t}:{${i / perShard}}`, ...m.slice(i, i + perShard).map(String)]);
          }
        }
        await pipe(r, cmds, 100);
        return TOTAL;
      },
      encodingProbes: ['tag:0:{0}'],
      probe: async () => ({ 'range-scannable?': 'yes, shards are sorted ranges' }),
    },
    {
      name: 'E · BITMAP over the sparse id space (2% density)',
      note: 'A bitmap costs the same 31 KiB per tag whether 2% or 100% of bits are set - it beats the plain sets here but loses to sharded intsets. This is the wrong side of the crossover.',
      load: async (r: Redis) => {
        for (let t = 0; t < TAGS; t++) {
          const cmds = MEMBERSHIP[t].map((id) => ['SETBIT', `tag:${t}`, String(id), '1']);
          await pipe(r, cmds as any, 1000);
        }
        return TOTAL;
      },
      encodingProbes: ['tag:0'],
      probe: async (r) => ({
        'bytes per tag bitmap': await r.strlen('tag:0'),
        'density': `${((PER_TAG / ID_SPACE) * 100).toFixed(1)}%`,
      }),
    },
    {
      caveat: 'different corpus - dense ids, not comparable to rows A-E',
      name: 'F · BITMAP over a dense id space (60% density)',
      note: `The other side of the crossover: 50 tags × ~60% of ${DENSE_SPACE.toLocaleString()} ids. A membership costs a bit.`,
      load: async (r: Redis) => {
        for (let t = 0; t < DENSE.length; t++) {
          // one SETBIT at the top bound allocates the whole bitmap once
          await r.setbit(`dense:${t}`, DENSE_SPACE - 1, 0);
          const cmds = DENSE[t].map((id) => ['SETBIT', `dense:${t}`, String(id), '1']);
          await pipe(r, cmds as any, 2000);
        }
        return DENSE_TOTAL;
      },
      probe: async (r) => ({
        'bytes per tag bitmap': await r.strlen('dense:0'),
        'density': '60%',
        'set ops': 'BITOP AND/OR/XOR for tag intersection, server-side',
      }),
    },
  ],
};
