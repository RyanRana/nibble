/**
 * CASE 12 — tool-latency percentiles.
 *
 * A t-digest allocates at CREATE and never grows, so it only beats TimeSeries
 * above ~4,700 samples/series. And agent traffic is bimodal: a 0.5% timeout
 * spike puts p99 on a near-vertical CDF. Both measured below.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { rng, gaussianFactory } from '../lib/rng.ts';

const SERIES = 200;
const SAMPLES = 5_000;
const TOTAL = SERIES * SAMPLES;
const T0 = 1_760_000_000_000;
const STEP_MS = 1_000;
const SEED = 606011;

/**
 * Lognormal tool latency — the distribution real RPC/LLM calls actually have.
 * A normal distribution would flatter every sketch; a uniform one would flatter
 * none of them.
 */
function makeLatencies(seed: number, timeoutFraction: number): number[][] {
  const r = rng(seed);
  const g = gaussianFactory(r);
  return Array.from({ length: SERIES }, () => {
    const out: number[] = [];
    for (let i = 0; i < SAMPLES; i++) {
      if (timeoutFraction > 0 && r() < timeoutFraction) {
        out.push(30_000); // hit the tool timeout
      } else {
        out.push(Math.max(1, Math.round(120 * Math.exp(g() * 0.9))));
      }
    }
    return out;
  });
}

const CLEAN = makeLatencies(SEED, 0);
const BIMODAL = makeLatencies(SEED, 0.005); // 0.5% of calls time out

/** Exact percentile from the raw samples — the ground truth every row is scored against. */
function exactQuantile(sorted: number[], q: number): number {
  const i = Math.min(sorted.length - 1, Math.floor(q * sorted.length));
  return sorted[i];
}

const SORTED_CLEAN = CLEAN.map((s) => [...s].sort((a, b) => a - b));
const SORTED_BIMODAL = BIMODAL.map((s) => [...s].sort((a, b) => a - b));

/** Mean |relative error| of TDIGEST.QUANTILE against exact, across all series. */
async function digestError(
  r: Redis,
  keyOf: (s: number) => string,
  sorted: number[][],
): Promise<Record<string, string>> {
  const qs = [0.5, 0.95, 0.99, 0.999];
  const err: number[][] = qs.map(() => []);
  for (let s = 0; s < SERIES; s++) {
    const got = (await r.call('TDIGEST.QUANTILE', keyOf(s), ...qs.map(String))) as string[];
    qs.forEach((q, j) => {
      const truth = exactQuantile(sorted[s], q);
      const est = Number(got[j]);
      if (Number.isFinite(est) && truth > 0) err[j].push(Math.abs(est - truth) / truth);
    });
  }
  const pct = (a: number[]) =>
    a.length ? `${((a.reduce((x, y) => x + y, 0) / a.length) * 100).toFixed(2)}%` : 'n/a';
  return {
    'p50 error': pct(err[0]),
    'p95 error': pct(err[1]),
    'p99 error': pct(err[2]),
    'p99.9 error': pct(err[3]),
  };
}

function loadDigests(data: number[][], compression: number, keyOf: (s: number) => string) {
  return async (r: Redis) => {
    await pipe(
      r,
      Array.from({ length: SERIES }, (_, s) => [
        'TDIGEST.CREATE', keyOf(s), 'COMPRESSION', String(compression),
      ]),
      100,
    );
    for (let s = 0; s < SERIES; s++) {
      const cmds: (string | number)[][] = [];
      // batch at 500 values per ADD — larger batches are rejected in Lua and
      // slow here for no benefit
      for (let i = 0; i < SAMPLES; i += 500) {
        cmds.push(['TDIGEST.ADD', keyOf(s), ...data[s].slice(i, i + 500).map(String)]);
      }
      await pipe(r, cmds as any, 50);
    }
    return TOTAL;
  };
}

const k = (s: number) => `lat:${s}`;

export const case12: BenchCase = {
  id: '12-percentiles',
  title: `Tool-latency percentiles (${SERIES} series × ${SAMPLES.toLocaleString()} = ${TOTAL.toLocaleString()} samples)`,
  question: 'Sketches promise huge savings on percentiles. Where is the break-even, and where do they lie to you?',
  unit: 'sample',

  variants: [
    {
      name: 'A · ZSET of raw samples',
      note: 'Keep everything, sort on read. Exact, queryable, and the most expensive option available.',
      load: async (r: Redis) => {
        for (let s = 0; s < SERIES; s++) {
          const cmds: any[] = [];
          for (let i = 0; i < SAMPLES; i += 500) {
            const a: any[] = ['ZADD', k(s)];
            for (let j = i; j < Math.min(i + 500, SAMPLES); j++) {
              a.push(String(T0 + j * STEP_MS), `${T0 + j * STEP_MS}:${CLEAN[s][j]}`);
            }
            cmds.push(a);
          }
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
      encodingProbes: ['lat:0'],
      probe: async () => ({ 'exact?': 'yes', 'also gives you': 'the raw samples, for anything else you need later' }),
    },
    {
      name: 'B · TIMESERIES COMPRESSED (integer ms)',
      note: 'Purpose-built, exact, and the row most people skip straight past on their way to a sketch.',
      load: async (r: Redis) => {
        await pipe(
          r,
          Array.from({ length: SERIES }, (_, s) => [
            'TS.CREATE', k(s), 'ENCODING', 'COMPRESSED', 'CHUNK_SIZE', '4096',
          ]),
          100,
        );
        for (let s = 0; s < SERIES; s++) {
          const cmds: any[] = [];
          for (let i = 0; i < SAMPLES; i += 500) {
            const a: any[] = ['TS.MADD'];
            for (let j = i; j < Math.min(i + 500, SAMPLES); j++) {
              a.push(k(s), String(T0 + j * STEP_MS), String(CLEAN[s][j]));
            }
            cmds.push(a);
          }
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
      probe: async () => ({
        'exact?': 'yes',
        'percentiles': 'computed client-side from TS.RANGE, or via a rollup rule',
      }),
    },
    {
      name: 'C · T-DIGEST, compression 100 (default)',
      note: 'Fixed 9,864 B per series REGARDLESS of sample count. Cheap per sample only if you feed it enough.',
      load: loadDigests(CLEAN, 100, k),
      probe: async (r) => ({
        ...(await digestError(r, k, SORTED_CLEAN)),
        'bytes/series': 9_864,
        'exact?': 'no — and it has no proven error bound',
      }),
    },
    {
      name: 'D · T-DIGEST, compression 200',
      note: 'Double the RAM, several times the accuracy. The defensible default for anything you alert on.',
      load: loadDigests(CLEAN, 200, k),
      probe: async (r) => ({ ...(await digestError(r, k, SORTED_CLEAN)), 'bytes/series': 19_464 }),
    },
    {
      name: 'E · T-DIGEST, compression 500',
      note: 'Diminishing returns on clean data — but see variant G for where it earns its keep.',
      load: loadDigests(CLEAN, 500, k),
      probe: async (r) => ({ ...(await digestError(r, k, SORTED_CLEAN)), 'bytes/series': 48_264 }),
    },
    {
      name: 'F · T-DIGEST c=100, archived as DUMP blobs',
      note: 'Cold rollups do not need to be live keys. DUMP persists only the merged centroids, not the input buffer.',
      caveat: 'not queryable until RESTOREd',
      load: async (r: Redis) => {
        await loadDigests(CLEAN, 100, k)(r);
        const blobs: Buffer[] = [];
        for (let s = 0; s < SERIES; s++) {
          blobs.push((await r.callBuffer('DUMP', k(s))) as Buffer);
        }
        await r.del(...Array.from({ length: SERIES }, (_, s) => k(s)));
        await pipe(
          r,
          blobs.map((b, s) => ['HSET', `cold:{${s % 4}}`, String(s), b]) as any,
          100,
        );
        return TOTAL;
      },
      probe: async (r) => ({
        'archive is': 'RESTORE-able back into a live digest on demand',
        'shards': 4,
      }),
    },
    {
      name: 'G · T-DIGEST c=100, BIMODAL (0.5% timeouts)',
      note: 'Identical sketch, realistic agent data: a body of normal latencies plus calls that hit the 30 s timeout.',
      caveat: 'different input data — compare its error to C, not its size',
      load: loadDigests(BIMODAL, 100, k),
      probe: async (r) => ({
        ...(await digestError(r, k, SORTED_BIMODAL)),
        'why': 'p99 sits on the cliff between the body and the timeout spike, where the CDF is near-vertical',
      }),
    },
    {
      name: 'H · BIMODAL, timeouts excluded + exact counter',
      note: 'The fix: digest only completed calls, count timeouts exactly. Both answers get better, and the count is exact.',
      caveat: 'different input data — compare its error to G',
      load: async (r: Redis) => {
        await pipe(
          r,
          Array.from({ length: SERIES }, (_, s) => ['TDIGEST.CREATE', k(s), 'COMPRESSION', '100']),
          100,
        );
        for (let s = 0; s < SERIES; s++) {
          const completed = BIMODAL[s].filter((v) => v < 30_000);
          const timeouts = SAMPLES - completed.length;
          const cmds: any[] = [];
          for (let i = 0; i < completed.length; i += 500) {
            cmds.push(['TDIGEST.ADD', k(s), ...completed.slice(i, i + 500).map(String)]);
          }
          cmds.push(['HSET', `timeouts:{${s % 4}}`, String(s), String(timeouts)]);
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
      probe: async (r) => {
        const sortedCompleted = SORTED_BIMODAL.map((s) => s.filter((v) => v < 30_000));
        return {
          ...(await digestError(r, k, sortedCompleted)),
          'timeout count': 'exact, in a sharded hash',
          'note': 'error is now measured against the completed-call distribution, which is the one you can act on',
        };
      },
    },
  ],
};
