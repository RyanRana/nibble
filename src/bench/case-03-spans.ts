/**
 * CASE 03 - Tool-call spans / step traces.
 *
 * This is the highest-cardinality record class in an agentic system. A single
 * long-horizon run emits hundreds of spans; a busy platform emits tens of
 * millions a day. At that volume the per-record framing tax *is* the bill:
 * a 24-byte saving per span is 1.2 GB/day at 50M spans/day.
 *
 * The last variant is the one people skip: retention. No encoding beats not
 * storing the record. Cost is reported per *ingested* span so a capped stream
 * is comparable to an uncapped one.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { makeSpan, rng, uuidLike, type ToolSpan } from '../lib/rng.ts';
import { json, packSpan, uuidToBytes, makeDictionary, deflateDict } from '../lib/codec.ts';
import { msgpack } from '../lib/msgpack.ts';

const RUNS = 1_500;
const SPANS_PER_RUN = 100;
const TOTAL = RUNS * SPANS_PER_RUN;
const BASE_TS = 1_760_000_000_000;
const SEED = 990011;

const RUN_IDS: string[] = [];
const SPANS: ToolSpan[][] = (() => {
  const r = rng(SEED);
  return Array.from({ length: RUNS }, () => {
    const id = uuidLike(r);
    RUN_IDS.push(id);
    return Array.from({ length: SPANS_PER_RUN }, (_, s) => makeSpan(r, id, s));
  });
})();

const DICT = makeDictionary(SPANS.slice(0, 40).flat().map((s) => json(s)));

export const case03: BenchCase = {
  id: '03-spans',
  title: `Tool-call spans (${RUNS.toLocaleString()} runs × ${SPANS_PER_RUN} spans = ${TOTAL.toLocaleString()})`,
  question: 'At tens of millions of spans a day, what does the framing tax cost - and what does retention save?',
  unit: 'span',

  variants: [
    {
      name: 'A · HASH per span',
      note: 'One key + 12 fields per span. The layout you get from "a span is a row".',
      load: async (r: Redis) => {
        for (const thread of SPANS) {
          const cmds = thread.map((s) => {
            const a: string[] = ['HSET', `span:${s.span_id}`];
            for (const [k, v] of Object.entries(s)) a.push(k, String(v));
            return a;
          });
          await pipe(r, cmds as any, 500);
        }
        return TOTAL;
      },
      encodingProbes: [`span:${SPANS[0][0].span_id}`],
    },
    {
      name: 'B · LIST of JSON spans per run',
      note: 'Collapses to one key per run, but every span still carries its field names.',
      load: async (r: Redis) => {
        for (let i = 0; i < RUNS; i++) {
          await pipe(r, SPANS[i].map((s) => ['RPUSH', `spans:${RUN_IDS[i]}`, json(s)]) as any, 500);
        }
        return TOTAL;
      },
      encodingProbes: [`spans:${RUN_IDS[0]}`],
    },
    {
      name: 'C · STREAM, field-per-attribute',
      note: 'Idiomatic XADD. Consumer groups and time ranges for free; field names repeat per entry.',
      load: async (r: Redis) => {
        for (let i = 0; i < RUNS; i++) {
          const cmds = SPANS[i].map((s, j) => {
            const a: string[] = ['XADD', `trace:${RUN_IDS[i]}`, `${BASE_TS + j}-0`];
            for (const [k, v] of Object.entries(s)) a.push(k, String(v));
            return a;
          });
          await pipe(r, cmds as any, 500);
        }
        return TOTAL;
      },
      encodingProbes: [`trace:${RUN_IDS[0]}`],
    },
    {
      name: 'D · STREAM, MessagePack in one field',
      note: 'One `d` field per entry. Stream semantics kept, field-name repetition removed.',
      load: async (r: Redis) => {
        for (let i = 0; i < RUNS; i++) {
          const cmds = SPANS[i].map((s, j) => [
            'XADD', `trace:${RUN_IDS[i]}`, `${BASE_TS + j}-0`, 'd', msgpack(s),
          ]);
          await pipe(r, cmds as any, 500);
        }
        return TOTAL;
      },
    },
    {
      name: 'E · STREAM, schema-packed in one field',
      note: 'span_id as 16 raw bytes, tool as 1 byte, ok/cache_hit/retry folded into one byte, ts as delta.',
      load: async (r: Redis) => {
        for (let i = 0; i < RUNS; i++) {
          const cmds = SPANS[i].map((s, j) => [
            'XADD', `trace:${RUN_IDS[i]}`, `${BASE_TS + j}-0`, 'd', packSpan(s, BASE_TS),
          ]);
          await pipe(r, cmds as any, 500);
        }
        return TOTAL;
      },
      encodingProbes: [`trace:${RUN_IDS[0]}`],
    },
    {
      name: 'F · LIST of schema-packed spans',
      note: 'Same payload as E without the stream entry-id and radix-tree overhead. No consumer groups.',
      load: async (r: Redis) => {
        for (let i = 0; i < RUNS; i++) {
          await pipe(
            r,
            SPANS[i].map((s) => ['RPUSH', `spans:${RUN_IDS[i]}`, packSpan(s, BASE_TS)]) as any,
            500,
          );
        }
        return TOTAL;
      },
      encodingProbes: [`spans:${RUN_IDS[0]}`],
    },
    {
      name: 'G · sharded HASH, packed, span_id key',
      note: 'Direct span lookup by id without a key per span. field = 16-byte span id.',
      config: { 'hash-max-listpack-entries': 256, 'hash-max-listpack-value': 256 },
      load: async (r: Redis) => {
        const shards = Math.ceil(TOTAL / 128);
        let n = 0;
        for (let i = 0; i < RUNS; i++) {
          const cmds = SPANS[i].map((s) => [
            'HSET', `spans:{${n++ % shards}}`, uuidToBytes(s.span_id), packSpan(s, BASE_TS),
          ]);
          await pipe(r, cmds as any, 500);
        }
        return TOTAL;
      },
      encodingProbes: ['spans:{0}'],
    },
    {
      caveat: 'lossy - retains 20 of 100 spans/run',
      name: 'H · STREAM packed + MAXLEN 20 (retention)',
      note: 'Same 150,000 spans ingested, only the last 20 per run retained. Cost is per span *ingested*. Exact MAXLEN, not `~`: approximate trimming only fires at macro-node boundaries and would not trim a 100-entry stream at all.',
      load: async (r: Redis) => {
        for (let i = 0; i < RUNS; i++) {
          const cmds = SPANS[i].map((s, j) => [
            'XADD', `trace:${RUN_IDS[i]}`, 'MAXLEN', '20', `${BASE_TS + j}-0`,
            'd', packSpan(s, BASE_TS),
          ]);
          await pipe(r, cmds as any, 500);
        }
        return TOTAL;
      },
      probe: async (r) => ({
        'retained entries/run': await r.xlen(`trace:${RUN_IDS[0]}`),
        'note': 'lossy by design - pair with an object-store archive',
      }),
    },
    {
      caveat: 'lossy - retains 20 of 100 spans/run',
      name: 'I · STREAM packed, MAXLEN 20, dict-deflate',
      note: 'Retention + compression together: the realistic hot-tier configuration.',
      load: async (r: Redis) => {
        for (let i = 0; i < RUNS; i++) {
          const cmds = SPANS[i].map((s, j) => [
            'XADD', `trace:${RUN_IDS[i]}`, 'MAXLEN', '20', `${BASE_TS + j}-0`,
            'd', deflateDict(json(s), DICT),
          ]);
          await pipe(r, cmds as any, 500);
        }
        return TOTAL;
      },
      probe: async () => {
        const flat = SPANS.flat();
        const avg = (f: (s: ToolSpan) => number) => flat.reduce((a, s) => a + f(s), 0) / flat.length;
        return {
          'payload JSON (B)': avg((s) => json(s).length).toFixed(1),
          'payload msgpack (B)': avg((s) => msgpack(s).length).toFixed(1),
          'payload packed (B)': avg((s) => packSpan(s, BASE_TS).length).toFixed(1),
          'payload JSON+dict (B)': avg((s) => deflateDict(json(s), DICT).length).toFixed(1),
        };
      },
    },
  ],
};
