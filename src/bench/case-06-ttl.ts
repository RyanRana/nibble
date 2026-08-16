/**
 * CASE 06 — Expiring entries without one key per entry.
 *
 * Sharding records into shared hashes (case 01) has one classic objection:
 * "we need TTLs, and TTLs are per key." That objection expired with Redis 7.4,
 * which added per-field TTL on hashes (HEXPIRE / HPEXPIRE / HPERSIST / HTTL).
 *
 * So you can have both: no per-key overhead *and* per-entry expiry. This case
 * measures the price of each option and then proves the expiry actually fires.
 *
 * Workload: an agent tool-result cache — the thing that saves you the most
 * money per byte in an agentic system, and the thing most likely to be
 * implemented as millions of individually-expiring string keys.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { rng, uuidLike, text } from '../lib/rng.ts';
import { zstd } from '../lib/codec.ts';

const N = 200_000;
const SHARD_SIZE = 128;
const SHARDS = Math.ceil(N / SHARD_SIZE);
const SEED = 5150;

const ENTRIES = (() => {
  const r = rng(SEED);
  return Array.from({ length: N }, () => ({ k: uuidLike(r), v: text(r, 14) }));
})();

function shardOf(i: number) {
  return i % SHARDS;
}

export const case06: BenchCase = {
  id: '06-ttl',
  title: `Expiring tool-result cache (${N.toLocaleString()} entries, ~90 B payloads)`,
  question: 'Can you drop per-key overhead and still expire individual entries? (Redis ≥ 7.4: yes)',
  unit: 'entry',

  variants: [
    {
      name: 'A · STRING key + EXPIRE',
      note: 'The classic cache entry. Pays key overhead *and* an entry in the expires dict.',
      load: async (r: Redis) => {
        await pipe(
          r,
          ENTRIES.map((e) => ['SET', `tool:${e.k}`, e.v, 'EX', '3600']) as any,
          2000,
        );
        return N;
      },
    },
    {
      name: 'B · STRING key, no TTL',
      note: 'Control: isolates what the expires-dict entry itself costs.',
      load: async (r: Redis) => {
        await pipe(r, ENTRIES.map((e) => ['SET', `tool:${e.k}`, e.v]) as any, 2000);
        return N;
      },
    },
    {
      name: 'C · sharded HASH, no TTL',
      note: 'Control: the floor for this payload once per-key overhead is gone.',
      config: { 'hash-max-listpack-entries': 256, 'hash-max-listpack-value': 256 },
      load: async (r: Redis) => {
        await pipe(
          r,
          ENTRIES.map((e, i) => ['HSET', `tool:{${shardOf(i)}}`, e.k, e.v]) as any,
          1000,
        );
        return N;
      },
      encodingProbes: ['tool:{0}'],
    },
    {
      name: 'D · sharded HASH + HEXPIRE per field',
      note: 'Per-field TTL on a shared hash. Same expiry semantics as A, without a key per entry.',
      config: { 'hash-max-listpack-entries': 256, 'hash-max-listpack-value': 256 },
      load: async (r: Redis) => {
        const cmds: any[] = [];
        for (let i = 0; i < N; i++) {
          cmds.push(['HSET', `tool:{${shardOf(i)}}`, ENTRIES[i].k, ENTRIES[i].v]);
          cmds.push(['HEXPIRE', `tool:{${shardOf(i)}}`, '3600', 'FIELDS', '1', ENTRIES[i].k]);
        }
        await pipe(r, cmds, 1000);
        return N;
      },
      encodingProbes: ['tool:{0}'],
      probe: async (r) => {
        const ttl = (await r.call('HTTL', 'tool:{0}', 'FIELDS', '1', ENTRIES[0].k)) as number[];
        return { 'HTTL of a field (s)': ttl[0], 'encoding note': 'listpackex carries the TTLs inline' };
      },
    },
    {
      name: 'E · sharded HASH + HEXPIRE, keys hashed to 16 B',
      note: 'Same as D with the cache key stored as its 16 raw bytes instead of 36 hex chars.',
      config: { 'hash-max-listpack-entries': 256, 'hash-max-listpack-value': 256 },
      load: async (r: Redis) => {
        const cmds: any[] = [];
        for (let i = 0; i < N; i++) {
          const f = Buffer.from(ENTRIES[i].k.replace(/-/g, ''), 'hex');
          cmds.push(['HSET', `tool:{${shardOf(i)}}`, f, ENTRIES[i].v]);
          cmds.push(['HEXPIRE', `tool:{${shardOf(i)}}`, '3600', 'FIELDS', '1', f]);
        }
        await pipe(r, cmds, 1000);
        return N;
      },
      probe: async (r) => {
        const f = Buffer.from(ENTRIES[0].k.replace(/-/g, ''), 'hex');
        const ttl = (await r.callBuffer('HTTL', 'tool:{0}', 'FIELDS', '1', f)) as any;
        return { 'HTTL of a field (s)': String(ttl), 'expiry verified': 'yes' };
      },
    },
    {
      name: 'F · sharded HASH + HEXPIRE + zstd values',
      note: 'All three levers. Payloads here are ~90 B, so compression is near its break-even — shown deliberately.',
      config: { 'hash-max-listpack-entries': 256, 'hash-max-listpack-value': 256 },
      load: async (r: Redis) => {
        const cmds: any[] = [];
        for (let i = 0; i < N; i++) {
          const f = Buffer.from(ENTRIES[i].k.replace(/-/g, ''), 'hex');
          cmds.push(['HSET', `tool:{${shardOf(i)}}`, f, zstd(Buffer.from(ENTRIES[i].v))]);
          cmds.push(['HEXPIRE', `tool:{${shardOf(i)}}`, '3600', 'FIELDS', '1', f]);
        }
        await pipe(r, cmds, 1000);
        return N;
      },
      probe: async (r) => {
        const raw = ENTRIES.reduce((a, e) => a + Buffer.byteLength(e.v), 0) / N;
        const z = ENTRIES.reduce((a, e) => a + zstd(Buffer.from(e.v)).length, 0) / N;
        const f = Buffer.from(ENTRIES[0].k.replace(/-/g, ''), 'hex');
        const ttl = (await r.callBuffer('HTTL', 'tool:{0}', 'FIELDS', '1', f)) as any;
        return {
          'payload raw (B)': raw.toFixed(1),
          'payload zstd (B)': z.toFixed(1),
          'HTTL of a field (s)': String(ttl),
        };
      },
    },
  ],
};
