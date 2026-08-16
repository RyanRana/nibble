/**
 * CASE 05 — Idempotency / dedup / "have I already done this?"
 *
 * Every serious agent platform keeps a large set of seen-ids: idempotency keys,
 * processed webhook ids, visited URLs, tool-call fingerprints. The set is huge,
 * each member is tiny, and the only question ever asked of it is "is this in
 * there?". That is the exact shape where storing the members verbatim is the
 * most expensive possible choice.
 *
 * Two families of answer:
 *
 *   exact & sharded  — hash the id to a 52-bit int, shard so each SET stays
 *                      under set-max-intset-entries and keeps the intset
 *                      encoding: ~8 bytes/member, no false positives, and
 *                      deletion still works.
 *   probabilistic    — a Bloom or Cuckoo filter: ~1–2 bytes/member, false
 *                      positives at a rate you choose, no deletion (Bloom).
 *
 * Both are measured, and the Bloom filter's *empirical* false-positive rate is
 * measured too, not quoted from the config.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { rng, uuidLike } from '../lib/rng.ts';

const N = 500_000;
const PROBE = 50_000; // ids that were never inserted, for FP-rate measurement
const SEED = 13371337;

const { IDS, ABSENT } = (() => {
  const r = rng(SEED);
  const ids = Array.from({ length: N }, () => uuidLike(r));
  const absent = Array.from({ length: PROBE }, () => uuidLike(r));
  return { IDS: ids, ABSENT: absent };
})();

/** 52-bit FNV-1a — stays inside JS's exact-integer range and Redis's int64 intset. */
function hash52(s: string): number {
  let h1 = 0x811c9dc5, h2 = 0x01000193;
  for (let i = 0; i < s.length; i++) {
    h1 = Math.imul(h1 ^ s.charCodeAt(i), 0x01000193);
    h2 = Math.imul(h2 ^ s.charCodeAt(i), 0x85ebca6b);
  }
  return (h1 >>> 0) * 0x100000 + (h2 >>> 12);
}

const SHARDS = 2048; // 500k / 2048 ≈ 244 members per shard → stays an intset

export const case05: BenchCase = {
  id: '05-dedup',
  title: `Idempotency / seen-set (${N.toLocaleString()} ids)`,
  question: 'A set whose only operation is membership. What is the floor?',
  unit: 'id',

  variants: [
    {
      name: 'A · SET of 36-char UUID strings',
      note: 'SADD the id as you received it. Correct, obvious, and the most expensive option available.',
      load: async (r: Redis) => {
        for (let i = 0; i < N; i += 1000) {
          await r.sadd('seen', ...IDS.slice(i, i + 1000));
        }
        return N;
      },
      encodingProbes: ['seen'],
    },
    {
      name: 'B · one STRING key per id (TTL pattern)',
      note: 'SET id 1 EX 86400 NX — the textbook idempotency lock. Pays full key overhead per id.',
      load: async (r: Redis) => {
        await pipe(r, IDS.map((id) => ['SET', `idem:${id}`, '1', 'EX', '86400', 'NX']) as any, 2000);
        return N;
      },
    },
    {
      name: 'C · SET of 52-bit int hashes',
      note: 'Hash first. Redis stores integer members inline instead of as SDS strings.',
      load: async (r: Redis) => {
        const h = IDS.map((id) => String(hash52(id)));
        for (let i = 0; i < N; i += 1000) await r.sadd('seen', ...h.slice(i, i + 1000));
        return N;
      },
      encodingProbes: ['seen'],
    },
    {
      name: 'D · sharded SETs of int hashes (intset)',
      note: `${SHARDS} shards × ~${Math.round(N / SHARDS)} members keeps every shard under set-max-intset-entries: a sorted packed int64 array.`,
      load: async (r: Redis) => {
        const buckets: number[][] = Array.from({ length: SHARDS }, () => []);
        for (const id of IDS) {
          const h = hash52(id);
          buckets[h % SHARDS].push(h);
        }
        const cmds: string[][] = [];
        for (let s = 0; s < SHARDS; s++) {
          // insert ascending: intset appends at the tail instead of memmoving
          buckets[s].sort((a, b) => a - b);
          for (let i = 0; i < buckets[s].length; i += 400) {
            cmds.push(['SADD', `seen:{${s}}`, ...buckets[s].slice(i, i + 400).map(String)]);
          }
        }
        await pipe(r, cmds as any, 200);
        return N;
      },
      encodingProbes: ['seen:{0}'],
      probe: async (r) => ({
        'exact?': 'yes — deletable, no false positives',
        'hash collisions expected': (((N * (N - 1)) / 2 / 2 ** 52).toFixed(4)),
        'members in shard 0': await r.scard('seen:{0}'),
      }),
    },
    {
      name: 'E · Bloom filter, 1% FP',
      note: 'BF.RESERVE 0.01. Membership only: no iteration, no deletion, no listing.',
      caveat: 'probabilistic — 1% false positives',
      load: async (r: Redis) => {
        await r.call('BF.RESERVE', 'seen', '0.01', String(N), 'NONSCALING');
        for (let i = 0; i < N; i += 1000) {
          await r.call('BF.MADD', 'seen', ...IDS.slice(i, i + 1000));
        }
        return N;
      },
      probe: async (r) => measureFp(r, 'seen'),
    },
    {
      name: 'F · Bloom filter, 0.1% FP',
      note: 'Ten times stricter, ~1.5× the bytes. The knob is yours.',
      caveat: 'probabilistic — 0.1% false positives',
      load: async (r: Redis) => {
        await r.call('BF.RESERVE', 'seen', '0.001', String(N), 'NONSCALING');
        for (let i = 0; i < N; i += 1000) {
          await r.call('BF.MADD', 'seen', ...IDS.slice(i, i + 1000));
        }
        return N;
      },
      probe: async (r) => measureFp(r, 'seen'),
    },
    {
      name: 'G · Cuckoo filter, 0.1%-class',
      note: 'CF.RESERVE — costs a bit more than Bloom but supports deletion, which idempotency keys often need.',
      caveat: 'probabilistic, but deletable',
      load: async (r: Redis) => {
        await r.call('CF.RESERVE', 'seen', String(N), 'BUCKETSIZE', '2', 'EXPANSION', '1');
        for (let i = 0; i < N; i += 1000) {
          await r.call('CF.INSERT', 'seen', 'ITEMS', ...IDS.slice(i, i + 1000));
        }
        return N;
      },
      probe: async (r) => {
        let fp = 0;
        for (let i = 0; i < PROBE; i += 1000) {
          const res = (await r.call('CF.MEXISTS', 'seen', ...ABSENT.slice(i, i + 1000))) as number[];
          fp += res.reduce((a: number, b: number) => a + b, 0);
        }
        return {
          'measured FP rate': `${((fp / PROBE) * 100).toFixed(3)}%`,
          'deletable?': 'yes (CF.DEL)',
        };
      },
    },
    {
      name: 'H · HyperLogLog (cardinality only)',
      note: 'Different question, different price. PFADD answers "how many distinct?" in a fixed 12 KiB — for any N.',
      caveat: 'answers cardinality, NOT membership',
      load: async (r: Redis) => {
        for (let i = 0; i < N; i += 1000) await r.pfadd('seen', ...IDS.slice(i, i + 1000));
        return N;
      },
      probe: async (r) => {
        const est = await r.pfcount('seen');
        return {
          'estimated distinct': est,
          'error': `${(((est - N) / N) * 100).toFixed(2)}%`,
          'answers membership?': 'no — cardinality only',
        };
      },
    },
  ],
};

async function measureFp(r: Redis, key: string): Promise<Record<string, string | number>> {
  let fp = 0;
  for (let i = 0; i < PROBE; i += 1000) {
    const res = (await r.call('BF.MEXISTS', key, ...ABSENT.slice(i, i + 1000))) as number[];
    fp += res.reduce((a: number, b: number) => a + b, 0);
  }
  return {
    'measured FP rate': `${((fp / PROBE) * 100).toFixed(3)}%`,
    'deletable?': 'no',
  };
}
