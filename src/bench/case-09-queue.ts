/**
 * CASE 09 — The agent task queue, and the PEL you forgot about.
 *
 * Two findings live here.
 *
 * The first is the usual encoding story: a queue entry is a small record and
 * pays the same framing tax as any other.
 *
 * The second is the one that actually pages people at 3am. A stream consumer
 * group keeps a Pending Entries List — one entry per delivered-but-unacked
 * message, holding the id, the consumer, a delivery timestamp and a counter.
 * If an agent crashes mid-tool-call, or a worker forgets to XACK, or you never
 * XAUTOCLAIM the dead ones, the PEL grows without bound *and it is not covered
 * by MAXLEN*. Trimming the stream does not trim the PEL. This case measures
 * that cost directly so you can budget for it.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { rng, uuidLike, pick, intBetween } from '../lib/rng.ts';
import { json, uuidToBytes, Writer } from '../lib/codec.ts';
import { msgpack } from '../lib/msgpack.ts';

const N = 200_000;
const SEED = 8899;
const KINDS = ['plan', 'tool_call', 'summarize', 'reflect', 'handoff', 'verify'];

interface Task {
  task_id: string;
  run_id: string;
  kind: string;
  priority: number;
  attempts: number;
  not_before: number;
  arg: string;
}

const TASKS: Task[] = (() => {
  const r = rng(SEED);
  return Array.from({ length: N }, () => ({
    task_id: uuidLike(r),
    run_id: uuidLike(r),
    kind: pick(r, KINDS),
    priority: intBetween(r, 0, 9),
    attempts: intBetween(r, 0, 3),
    not_before: 1_760_000_000 + intBetween(r, 0, 86_400),
    arg: uuidLike(r),
  }));
})();

function packTask(t: Task): Buffer {
  const w = new Writer(80);
  w.bytes(uuidToBytes(t.task_id));
  w.bytes(uuidToBytes(t.run_id));
  w.bytes(uuidToBytes(t.arg));
  w.u8(KINDS.indexOf(t.kind));
  w.u8((t.priority & 0xf) | ((t.attempts & 0xf) << 4));
  w.varint(t.not_before - 1_760_000_000);
  return w.done();
}

export const case09: BenchCase = {
  id: '09-queue',
  title: `Agent task queue (${N.toLocaleString()} tasks)`,
  question: 'What does a queued task cost — and what does an un-acked one cost on top?',
  unit: 'task',

  variants: [
    {
      name: 'A · LIST of JSON tasks',
      note: 'RPUSH/BLPOP. Simple, durable enough, no delivery tracking.',
      load: async (r: Redis) => {
        await pipe(r, TASKS.map((t) => ['RPUSH', 'q', json(t)]) as any, 1000);
        return N;
      },
      encodingProbes: ['q'],
    },
    {
      name: 'B · LIST of MessagePack tasks',
      note: 'Same structure, binary framing.',
      load: async (r: Redis) => {
        await pipe(r, TASKS.map((t) => ['RPUSH', 'q', msgpack(t)]) as any, 1000);
        return N;
      },
    },
    {
      name: 'C · LIST of schema-packed tasks',
      note: '3 raw UUIDs + one packed byte for priority/attempts + a varint deadline = 52 B.',
      load: async (r: Redis) => {
        await pipe(r, TASKS.map((t) => ['RPUSH', 'q', packTask(t)]) as any, 1000);
        return N;
      },
      probe: async () => ({
        'payload JSON (B)': (TASKS.reduce((a, t) => a + json(t).length, 0) / N).toFixed(1),
        'payload packed (B)': (TASKS.reduce((a, t) => a + packTask(t).length, 0) / N).toFixed(1),
      }),
    },
    {
      name: 'D · STREAM, field-per-attribute',
      note: 'XADD with 7 fields. Gets you consumer groups, at-least-once delivery and replay.',
      load: async (r: Redis) => {
        const cmds = TASKS.map((t, i) => [
          'XADD', 'q', `${1_760_000_000_000 + i}-0`,
          'task_id', t.task_id, 'run_id', t.run_id, 'kind', t.kind,
          'priority', String(t.priority), 'attempts', String(t.attempts),
          'not_before', String(t.not_before), 'arg', t.arg,
        ]);
        await pipe(r, cmds as any, 1000);
        return N;
      },
      encodingProbes: ['q'],
    },
    {
      name: 'E · STREAM, packed single field',
      note: 'Same delivery guarantees, one `d` field.',
      load: async (r: Redis) => {
        const cmds = TASKS.map((t, i) => [
          'XADD', 'q', `${1_760_000_000_000 + i}-0`, 'd', packTask(t),
        ]);
        await pipe(r, cmds as any, 1000);
        return N;
      },
    },
    {
      name: 'F · STREAM packed + full PEL (nothing acked)',
      note: 'Identical data to E, then every entry delivered and left un-acked. The delta is the PEL.',
      load: async (r: Redis) => {
        const cmds = TASKS.map((t, i) => [
          'XADD', 'q', `${1_760_000_000_000 + i}-0`, 'd', packTask(t),
        ]);
        await pipe(r, cmds as any, 1000);
        await r.xgroup('CREATE', 'q', 'g', '0');
        for (let i = 0; i < N; i += 5000) {
          await r.xreadgroup('GROUP', 'g', 'worker-1', 'COUNT', 5000, 'STREAMS', 'q', '>');
        }
        return N;
      },
      probe: async (r) => {
        const p = (await r.xpending('q', 'g')) as any[];
        return {
          'pending entries': p[0],
          'covered by MAXLEN?': 'no — XTRIM does not shrink the PEL',
          'fix': 'XACK on success; XAUTOCLAIM + XACK for abandoned work',
        };
      },
    },
    {
      name: 'G · STREAM packed + PEL, all acked',
      note: 'Same delivery, then XACK. Proves the PEL cost is recoverable, not structural.',
      load: async (r: Redis) => {
        const cmds = TASKS.map((t, i) => [
          'XADD', 'q', `${1_760_000_000_000 + i}-0`, 'd', packTask(t),
        ]);
        await pipe(r, cmds as any, 1000);
        await r.xgroup('CREATE', 'q', 'g', '0');
        for (let i = 0; i < N; i += 5000) {
          const res = (await r.xreadgroup(
            'GROUP', 'g', 'worker-1', 'COUNT', 5000, 'STREAMS', 'q', '>',
          )) as any;
          const ids = res[0][1].map((e: any[]) => e[0]);
          for (let j = 0; j < ids.length; j += 1000) {
            await r.xack('q', 'g', ...ids.slice(j, j + 1000));
          }
        }
        return N;
      },
      probe: async (r) => ({ 'pending entries': ((await r.xpending('q', 'g')) as any[])[0] }),
    },
    {
      name: 'H · ZSET delayed queue, packed member',
      note: 'score = not_before, member = packed task. Scheduling for free; skiplist overhead is the price.',
      load: async (r: Redis) => {
        const cmds: any[] = [];
        for (let i = 0; i < N; i += 500) {
          const a: any[] = ['ZADD', 'q'];
          for (const t of TASKS.slice(i, i + 500)) a.push(String(t.not_before), packTask(t));
          cmds.push(a);
        }
        await pipe(r, cmds, 100);
        return N;
      },
      encodingProbes: ['q'],
    },
  ],
};
