/**
 * CASE 01 - Agent run state: how you lay out one record.
 *
 * Every agentic framework stores a "run" / "session" / "checkpoint" row with a
 * few dozen fields. The naive layouts are the ones that come naturally from an
 * ORM mindset (a key per field) or a REST mindset (a JSON blob). Both are
 * expensive for reasons that have nothing to do with your data.
 *
 * A Redis top-level key is not free: dictEntry + robj + the SDS of the key name
 * + hashtable bucket slack ≈ 50–100 B *before the value*. Storing 20 fields as
 * 20 keys pays that 20 times per run.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { makeRun, rng, type AgentRun } from '../lib/rng.ts';
import {
  json, packRun, RUN_SHORT_FIELDS, uuidToBytes,
  makeDictionary, deflateDict, zstd,
} from '../lib/codec.ts';
import { msgpack } from '../lib/msgpack.ts';

const N = 5_000;
const SEED = 20260816;

function runs(): AgentRun[] {
  const r = rng(SEED);
  return Array.from({ length: N }, (_, i) => makeRun(r, i));
}

const DATA = runs();
/** Dictionaries are trained per encoding - a dictionary is a corpus, not a setting. */
const DICT_JSON = makeDictionary(DATA.slice(0, 120).map((x) => json(x)));
const DICT_PACKED = makeDictionary(DATA.slice(0, 120).map((x) => packRun(x)));

/** Shard router: which hash holds this run, given `shards` buckets. */
function shardOf(runId: string, shards: number): number {
  // FNV-1a over the id's first 8 hex chars - cheap and stable
  let h = 0x811c9dc5;
  for (let i = 0; i < 8; i++) {
    h ^= runId.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0) % shards;
}

export const case01: BenchCase = {
  id: '01-run-state',
  title: 'Agent run state (20 fields × 5,000 runs)',
  question: 'What does one agent-run record actually cost, and how much of that is layout tax?',
  unit: 'run',

  variants: [
    {
      name: 'A · key-per-field',
      note: 'run:{id}:{field} → 20 top-level keys per run. The ORM-shaped layout.',
      encodingProbes: [],
      load: async (r: Redis) => {
        const cmds: string[][] = [];
        for (const run of DATA) {
          for (const [k, v] of Object.entries(run)) {
            cmds.push(['SET', `run:${run.run_id}:${k}`, String(v)]);
          }
        }
        await pipe(r, cmds);
        return N;
      },
    },
    {
      name: 'B · HASH, long names, hashtable',
      note: 'One hash per run but forced past the listpack threshold - what you get at scale if you never tune.',
      config: { 'hash-max-listpack-entries': 0 },
      load: async (r: Redis) => {
        const cmds: string[][] = [];
        for (const run of DATA) {
          const a: string[] = ['HSET', `run:${run.run_id}`];
          for (const [k, v] of Object.entries(run)) a.push(k, String(v));
          cmds.push(a);
        }
        await pipe(r, cmds, 500);
        return N;
      },
      encodingProbes: [`run:${DATA[0].run_id}`],
    },
    {
      name: 'C · HASH, long names, listpack',
      note: 'Same data, same API - only the encoding threshold changed. No code change.',
      load: async (r: Redis) => {
        const cmds: string[][] = [];
        for (const run of DATA) {
          const a: string[] = ['HSET', `run:${run.run_id}`];
          for (const [k, v] of Object.entries(run)) a.push(k, String(v));
          cmds.push(a);
        }
        await pipe(r, cmds, 500);
        return N;
      },
      encodingProbes: [`run:${DATA[0].run_id}`],
    },
    {
      name: 'C2 · HASH, long names, template-listpack',
      note: 'Redis 8.10 compact hashes. Hashes that share a schema store their field names ONCE, in a shared template. Config-only: same code, same readable field names.',
      config: { 'hash-min-template-entries': 4, 'hash-max-template-entries': 128 },
      load: async (r: Redis) => {
        const cmds: string[][] = [];
        for (const run of DATA) {
          const a: string[] = ['HSET', `run:${run.run_id}`];
          for (const [k, v] of Object.entries(run)) a.push(k, String(v));
          cmds.push(a);
        }
        await pipe(r, cmds, 500);
        return N;
      },
      encodingProbes: [`run:${DATA[0].run_id}`],
      probe: async () => ({
        'requires': 'Redis ≥ 8.10',
        'app changes': 'none - CONFIG SET hash-min-template-entries',
      }),
    },
    {
      name: 'D · HASH, short names, listpack',
      note: 'Field names are stored once per record, not once per schema. 1-char names.',
      load: async (r: Redis) => {
        const cmds: string[][] = [];
        for (const run of DATA) {
          const a: string[] = ['HSET', `run:${run.run_id}`];
          for (const [k, v] of Object.entries(run)) {
            a.push(RUN_SHORT_FIELDS[k as keyof AgentRun], String(v));
          }
          cmds.push(a);
        }
        await pipe(r, cmds, 500);
        return N;
      },
      encodingProbes: [`run:${DATA[0].run_id}`],
    },
    {
      name: 'D2 · HASH, short names + template',
      note: 'Both field-name tricks together. Shows they overlap: the template already removed the repetition that short names were fighting.',
      config: { 'hash-min-template-entries': 4, 'hash-max-template-entries': 128 },
      load: async (r: Redis) => {
        const cmds: string[][] = [];
        for (const run of DATA) {
          const a: string[] = ['HSET', `run:${run.run_id}`];
          for (const [k, v] of Object.entries(run)) {
            a.push(RUN_SHORT_FIELDS[k as keyof AgentRun], String(v));
          }
          cmds.push(a);
        }
        await pipe(r, cmds, 500);
        return N;
      },
      encodingProbes: [`run:${DATA[0].run_id}`],
    },
    {
      name: 'E · STRING, JSON blob',
      note: 'The "just put JSON in Redis" baseline.',
      load: async (r: Redis) => {
        await pipe(r, DATA.map((run) => ['SET', `run:${run.run_id}`, json(run)]) as any);
        return N;
      },
      encodingProbes: [`run:${DATA[0].run_id}`],
    },
    {
      name: 'F · STRING, MessagePack',
      note: 'Same object graph, self-describing binary. Zero schema work.',
      load: async (r: Redis) => {
        await pipe(r, DATA.map((run) => ['SET', `run:${run.run_id}`, msgpack(run)]) as any);
        return N;
      },
      encodingProbes: [`run:${DATA[0].run_id}`],
    },
    {
      name: 'G · STRING, schema-packed',
      note: 'UUIDs as 16 raw bytes, enums as 1 byte, timestamps as deltas, ints as varints.',
      load: async (r: Redis) => {
        await pipe(r, DATA.map((run) => ['SET', `run:${run.run_id}`, packRun(run)]) as any);
        return N;
      },
      encodingProbes: [`run:${DATA[0].run_id}`],
    },
    {
      name: 'H · STRING, JSON + zstd',
      note: 'General-purpose compression on a ~577 B record: some win, but no shared history to exploit.',
      load: async (r: Redis) => {
        await pipe(r, DATA.map((run) => ['SET', `run:${run.run_id}`, zstd(json(run))]) as any);
        return N;
      },
    },
    {
      name: 'I · STRING, JSON + dict-deflate',
      note: 'Preset-dictionary deflate trained on 120 records. Recovers most of the packing win with zero schema work.',
      load: async (r: Redis) => {
        await pipe(
          r,
          DATA.map((run) => ['SET', `run:${run.run_id}`, deflateDict(json(run), DICT_JSON)]) as any,
        );
        return N;
      },
    },
    {
      name: 'I2 · STRING, packed + dict-deflate',
      note: 'Negative result, kept on purpose: schema packing already removed the redundancy, so compressing again buys nothing.',
      load: async (r: Redis) => {
        await pipe(
          r,
          DATA.map((run) => [
            'SET', `run:${run.run_id}`, deflateDict(packRun(run), DICT_PACKED),
          ]) as any,
        );
        return N;
      },
    },
    {
      name: 'J · sharded HASH (128/shard, listpack)',
      note: 'Runs bucketed into shared hashes; field = 16-byte run id, value = packed record. Kills per-key overhead.',
      config: { 'hash-max-listpack-entries': 256, 'hash-max-listpack-value': 512 },
      load: async (r: Redis) => {
        const shards = Math.ceil(N / 128);
        const cmds: (string | Buffer)[][] = [];
        for (const run of DATA) {
          cmds.push([
            'HSET',
            `runs:{${shardOf(run.run_id, shards)}}`,
            uuidToBytes(run.run_id),
            packRun(run),
          ]);
        }
        await pipe(r, cmds as any, 500);
        return N;
      },
      encodingProbes: ['runs:{0}'],
    },
    {
      name: 'K · sharded HASH (1024/shard, hashtable)',
      note: 'Bigger shards: O(1) field lookup instead of listpack scan, still ~1 key per 1024 runs.',
      load: async (r: Redis) => {
        const shards = Math.ceil(N / 1024);
        const cmds: (string | Buffer)[][] = [];
        for (const run of DATA) {
          cmds.push([
            'HSET',
            `runs:{${shardOf(run.run_id, shards)}}`,
            uuidToBytes(run.run_id),
            packRun(run),
          ]);
        }
        await pipe(r, cmds as any, 500);
        return N;
      },
      encodingProbes: ['runs:{0}'],
    },
    {
      name: 'L · sharded HASH + JSON dict-deflate',
      note: 'Sharding plus compression while keeping plain JSON as the wire format - no schema code anywhere.',
      config: { 'hash-max-listpack-entries': 256, 'hash-max-listpack-value': 512 },
      load: async (r: Redis) => {
        const shards = Math.ceil(N / 128);
        const cmds: (string | Buffer)[][] = [];
        for (const run of DATA) {
          cmds.push([
            'HSET',
            `runs:{${shardOf(run.run_id, shards)}}`,
            uuidToBytes(run.run_id),
            deflateDict(json(run), DICT_JSON),
          ]);
        }
        await pipe(r, cmds as any, 500);
        return N;
      },
      encodingProbes: ['runs:{0}'],
      probe: async () => {
        const avg = (f: (x: AgentRun) => number) => DATA.reduce((a, x) => a + f(x), 0) / N;
        return {
          'payload JSON (B)': avg((x) => json(x).length).toFixed(1),
          'payload msgpack (B)': avg((x) => msgpack(x).length).toFixed(1),
          'payload packed (B)': avg((x) => packRun(x).length).toFixed(1),
          'payload JSON+dict (B)': avg((x) => deflateDict(json(x), DICT_JSON).length).toFixed(1),
          'payload packed+dict (B)': avg((x) => deflateDict(packRun(x), DICT_PACKED).length).toFixed(1),
        };
      },
    },
  ],
};
