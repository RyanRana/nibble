/**
 * CASE 02 - Conversation transcripts.
 *
 * The bulk of an agentic system's bytes is prose: user turns, assistant turns,
 * tool results. Prose is the one thing in this repo that compresses well, and
 * it is usually stored uncompressed as JSON in a LIST.
 *
 * The interesting structural idea here is the **hot tail / cold chunk** split:
 * an agent only ever reads the last few turns at full fidelity, and rereads the
 * older ones rarely and in bulk. So keep a small uncompressed tail for the
 * write path, and seal older turns into compressed chunks. You get list-like
 * append semantics with archive-like density.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { makeTurn, rng, type Turn } from '../lib/rng.ts';
import { json, zstd, makeDictionary, deflateDict } from '../lib/codec.ts';
import { msgpack } from '../lib/msgpack.ts';

const RUNS = 800;
const TURNS = 40;
const WORDS = 180; // ≈ 1.1 KiB of content per turn
const SEED = 424242;

const THREADS: Turn[][] = (() => {
  const r = rng(SEED);
  return Array.from({ length: RUNS }, () =>
    Array.from({ length: TURNS }, (_, i) => makeTurn(r, i, WORDS)),
  );
})();

const TOTAL = RUNS * TURNS;
const DICT = makeDictionary(THREADS.slice(0, 8).flat().map((t) => json(t)));
const CHUNK = 16;

/** Seal every CHUNK turns into one compressed blob; keep the remainder raw. */
function chunkThread(turns: Turn[], compress: (b: Buffer) => Buffer) {
  const sealed: Buffer[] = [];
  let i = 0;
  for (; i + CHUNK <= turns.length; i += CHUNK) {
    sealed.push(compress(json(turns.slice(i, i + CHUNK))));
  }
  const tail = turns.slice(i).map((t) => json(t));
  return { sealed, tail };
}

export const case02: BenchCase = {
  id: '02-transcript',
  title: `Conversation transcripts (${RUNS} threads × ${TURNS} turns ≈ 1.1 KiB each)`,
  question: 'Prose is the biggest byte pool and the most compressible. How much is left on the table?',
  unit: 'turn',

  variants: [
    {
      name: 'A · LIST of JSON turns',
      note: 'The default in nearly every agent framework: RPUSH a JSON turn per message.',
      load: async (r: Redis) => {
        for (let t = 0; t < RUNS; t++) {
          await pipe(r, THREADS[t].map((x) => ['RPUSH', `thread:${t}`, json(x)]) as any, 200);
        }
        return TOTAL;
      },
      encodingProbes: ['thread:0'],
    },
    {
      name: 'B · LIST of JSON + list-compress-depth 1',
      note: 'Config-only. Redis LZF-compresses interior quicklist nodes; head and tail stay hot. Zero code change.',
      config: { 'list-compress-depth': 1 },
      load: async (r: Redis) => {
        for (let t = 0; t < RUNS; t++) {
          await pipe(r, THREADS[t].map((x) => ['RPUSH', `thread:${t}`, json(x)]) as any, 200);
        }
        return TOTAL;
      },
      encodingProbes: ['thread:0'],
    },
    {
      name: 'C · LIST of MessagePack turns',
      note: 'Binary framing only. Prose dominates, so the win is small - worth knowing before you refactor.',
      load: async (r: Redis) => {
        for (let t = 0; t < RUNS; t++) {
          await pipe(r, THREADS[t].map((x) => ['RPUSH', `thread:${t}`, msgpack(x)]) as any, 200);
        }
        return TOTAL;
      },
    },
    {
      name: 'D · LIST of per-turn zstd',
      note: 'Compress each turn independently. Preserves random access to any single turn.',
      load: async (r: Redis) => {
        for (let t = 0; t < RUNS; t++) {
          await pipe(r, THREADS[t].map((x) => ['RPUSH', `thread:${t}`, zstd(json(x))]) as any, 200);
        }
        return TOTAL;
      },
    },
    {
      name: 'E · LIST of per-turn dict-deflate',
      note: 'Per-turn compression with a dictionary trained on 8 sample threads.',
      load: async (r: Redis) => {
        for (let t = 0; t < RUNS; t++) {
          await pipe(
            r,
            THREADS[t].map((x) => ['RPUSH', `thread:${t}`, deflateDict(json(x), DICT)]) as any,
            200,
          );
        }
        return TOTAL;
      },
    },
    {
      name: 'F · STREAM, field-per-attribute',
      note: 'XADD with 7 fields. Streams are the natural append-only log, but field names repeat per entry.',
      load: async (r: Redis) => {
        for (let t = 0; t < RUNS; t++) {
          const cmds = THREADS[t].map((x, i) => [
            'XADD', `thread:${t}`, `${1_760_000_000_000 + i}-0`,
            'role', x.role, 'content', x.content, 'model', x.model,
            'ts', String(x.ts), 'in_tok', String(x.in_tok),
            'out_tok', String(x.out_tok), 'stop', x.stop,
          ]);
          await pipe(r, cmds as any, 200);
        }
        return TOTAL;
      },
      encodingProbes: ['thread:0'],
    },
    {
      name: 'G · STREAM, one compressed field',
      note: 'XADD with a single `d` field holding the zstd-compressed turn. Keeps stream semantics, drops the field-name tax.',
      load: async (r: Redis) => {
        for (let t = 0; t < RUNS; t++) {
          const cmds = THREADS[t].map((x, i) => [
            'XADD', `thread:${t}`, `${1_760_000_000_000 + i}-0`, 'd', zstd(json(x)),
          ]);
          await pipe(r, cmds as any, 200);
        }
        return TOTAL;
      },
    },
    {
      name: 'H · chunked: 16-turn zstd blobs + raw tail',
      note: 'Hot tail stays raw for cheap appends; sealed 16-turn chunks compress against each other.',
      load: async (r: Redis) => {
        for (let t = 0; t < RUNS; t++) {
          const { sealed, tail } = chunkThread(THREADS[t], (b) => zstd(b));
          const cmds: any[] = sealed.map((b) => ['RPUSH', `thread:${t}:cold`, b]);
          for (const x of tail) cmds.push(['RPUSH', `thread:${t}:hot`, x]);
          await pipe(r, cmds, 200);
        }
        return TOTAL;
      },
      probe: async () => {
        const raw = THREADS.flat().reduce((a, x) => a + json(x).length, 0) / TOTAL;
        const perTurnZ = THREADS.flat().reduce((a, x) => a + zstd(json(x)).length, 0) / TOTAL;
        let chunked = 0;
        for (const th of THREADS) {
          const { sealed, tail } = chunkThread(th, (b) => zstd(b));
          chunked += sealed.reduce((a, b) => a + b.length, 0) + tail.reduce((a, b) => a + b.length, 0);
        }
        return {
          'raw JSON turn (B)': raw.toFixed(1),
          'per-turn zstd (B)': perTurnZ.toFixed(1),
          'chunked zstd (B/turn)': (chunked / TOTAL).toFixed(1),
          'chunk ratio': (raw / (chunked / TOTAL)).toFixed(2) + '×',
        };
      },
    },
    {
      name: 'I · chunked + dict-deflate blobs + raw tail',
      note: 'Same structure, dictionary-primed. Best density that still supports O(1) appends.',
      load: async (r: Redis) => {
        for (let t = 0; t < RUNS; t++) {
          const { sealed, tail } = chunkThread(THREADS[t], (b) => deflateDict(b, DICT));
          const cmds: any[] = sealed.map((b) => ['RPUSH', `thread:${t}:cold`, b]);
          for (const x of tail) cmds.push(['RPUSH', `thread:${t}:hot`, x]);
          await pipe(r, cmds, 200);
        }
        return TOTAL;
      },
    },
  ],
};
