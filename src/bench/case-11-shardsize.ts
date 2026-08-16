/**
 * CASE 11 - Shard sizing, or: the 11% nobody tunes.
 *
 * Case 08 showed the allocator charges you by size class, not by byte. Case 01
 * showed sharding records into shared hashes is the biggest single RAM win.
 * This case is what happens where those two facts meet, and it is the least
 * obvious result in the repo.
 *
 * A sharded hash's listpack is one allocation whose size is roughly
 * `shard_size × bytes_per_entry`. Pick 128 entries per shard because it is a
 * round number and that allocation may land just *over* a jemalloc size class,
 * so every shard silently carries up to a full class of dead space. Pick 120
 * and it lands just under.
 *
 * Same data. Same encoding. Same commands. Only the shard width changes.
 *
 * The effect is not small and it is not noise - it is periodic, because size
 * classes are periodic. It also explains a result that confused this benchmark
 * earlier: per-field TTLs (HEXPIRE) looked like they cost ~32 bytes per field,
 * when the intrinsic cost in `listpackex` is ~10 bytes. The other ~22 was the
 * shard's listpack crossing a 4 KiB boundary. At an aligned shard width the
 * same TTLs cost almost nothing, because the allocation was going to round up
 * to that class regardless.
 *
 * Practical rule: after you choose a layout, sweep the shard width and keep the
 * cheapest. It is a one-line constant and it is worth ~10%.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase, Variant } from '../lib/measure.ts';
import { rng, uuidLike, text } from '../lib/rng.ts';

const N = 120_000;
const SEED = 5150;

const ENTRIES = (() => {
  const r = rng(SEED);
  return Array.from({ length: N }, () => ({
    f: Buffer.from(uuidLike(r).replace(/-/g, ''), 'hex'),
    v: text(r, 14),
  }));
})();

const WIDTHS = [96, 112, 120, 124, 128, 144, 160, 176, 192, 224, 240, 256];

function variantFor(width: number, ttl: boolean): Variant {
  const shards = Math.ceil(N / width);
  return {
    name: `${ttl ? 'TTL  ' : 'plain'} · ${String(width).padStart(3)} fields/shard`,
    note: '',
    config: { 'hash-max-listpack-entries': 512, 'hash-max-listpack-value': 256 },
    load: async (r: Redis) => {
      const cmds: any[] = [];
      for (let i = 0; i < N; i++) {
        const key = `tool:{${i % shards}}`;
        cmds.push(['HSET', key, ENTRIES[i].f, ENTRIES[i].v]);
        if (ttl) cmds.push(['HEXPIRE', key, '3600', 'FIELDS', '1', ENTRIES[i].f]);
      }
      await pipe(r, cmds, 1000);
      return N;
    },
    encodingProbes: ['tool:{0}'],
    probe: async (r) => ({
      'shard listpack (B)': (await r.memory('USAGE', 'tool:{0}')) as number,
      'fields in shard 0': await r.hlen('tool:{0}'),
    }),
  };
}

export const case11: BenchCase = {
  id: '11-shard-size',
  title: `Shard width vs allocator size class (${N.toLocaleString()} entries)`,
  question: 'Same layout, same data - how much does the shard width alone cost you?',
  unit: 'entry',
  kind: 'sweep',
  variants: [
    ...WIDTHS.map((w) => variantFor(w, false)),
    ...WIDTHS.map((w) => variantFor(w, true)),
  ],
};
