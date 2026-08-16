/**
 * CASE 10 - Per-agent telemetry: latency, token spend, cost.
 *
 * Agentic platforms emit a lot of numbers-over-time: p95 tool latency per
 * agent, tokens per minute per tenant, cost per run. Stored as a ZSET or a
 * stream, each sample costs 50–100 bytes to hold 8 bytes of information.
 *
 * Redis's time-series type stores a timestamp/value pair with Gorilla-style
 * double-delta + XOR compression, which for the smoothly-varying signals real
 * telemetry produces lands around 1–2 bytes per sample.
 *
 * The signal here is a random walk, not uniform noise. That matters: uniform
 * random floats are incompressible and would understate the compressed
 * encoding, while a constant series would flatter it. A random walk is what
 * latency and cost metrics actually look like.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase } from '../lib/measure.ts';
import { rng } from '../lib/rng.ts';

const SERIES = 400;
const SAMPLES = 2_500;
const TOTAL = SERIES * SAMPLES; // 1,000,000 samples
const STEP_MS = 10_000;
const T0 = 1_760_000_000_000;
const SEED = 31415;

/**
 * Random-walk latency signal per series: autocorrelated, like real telemetry.
 *
 * Two versions of the *same* walk, because the difference between them turns
 * out to be a real optimization. Gorilla-style value compression XORs
 * consecutive float64s and stores only the differing mantissa window. An
 * integer millisecond count has a short, stable mantissa; the same value with
 * two decimal places (123.45) has a long noisy one. Rounding your metric to an
 * integer before you store it is free and measurably cheaper.
 */
const DATA: Float64Array[] = (() => {
  const r = rng(SEED);
  return Array.from({ length: SERIES }, () => {
    const out = new Float64Array(SAMPLES);
    let v = 120 + r() * 400;
    for (let i = 0; i < SAMPLES; i++) {
      v = Math.max(5, v + (r() - 0.5) * 18);
      out[i] = Math.round(v);
    }
    return out;
  });
})();

/** Identical walk, stored with two decimal places. */
const DATA_FLOAT: Float64Array[] = (() => {
  const r = rng(SEED);
  return Array.from({ length: SERIES }, () => {
    const out = new Float64Array(SAMPLES);
    let v = 120 + r() * 400;
    for (let i = 0; i < SAMPLES; i++) {
      v = Math.max(5, v + (r() - 0.5) * 18);
      out[i] = Math.round(v * 100) / 100;
    }
    return out;
  });
})();

/** Same series resampled to pure noise - the adversarial input for compression. */
const NOISE: Float64Array[] = (() => {
  const r = rng(SEED + 9);
  return Array.from({ length: SERIES }, () => {
    const out = new Float64Array(SAMPLES);
    for (let i = 0; i < SAMPLES; i++) out[i] = Math.round(r() * 1e9) / 1000;
    return out;
  });
})();

export const case10: BenchCase = {
  id: '10-metrics',
  title: `Agent telemetry (${SERIES} series × ${SAMPLES.toLocaleString()} = ${TOTAL.toLocaleString()} samples)`,
  question: 'A sample is 16 bytes of information. What are the containers charging for it?',
  unit: 'sample',

  variants: [
    {
      name: 'A · ZSET, member "ts:value"',
      note: 'The common hand-rolled time series. ZRANGEBYSCORE works; the skiplist and the string member do not come cheap.',
      load: async (r: Redis) => {
        for (let s = 0; s < SERIES; s++) {
          const cmds: any[] = [];
          for (let i = 0; i < SAMPLES; i += 500) {
            const a: any[] = ['ZADD', `lat:${s}`];
            for (let j = i; j < Math.min(i + 500, SAMPLES); j++) {
              a.push(String(T0 + j * STEP_MS), `${T0 + j * STEP_MS}:${DATA[s][j]}`);
            }
            cmds.push(a);
          }
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
      encodingProbes: ['lat:0'],
    },
    {
      name: 'B · STREAM, ts + value fields',
      note: 'Idiomatic, replayable, and still stores two field names per sample.',
      load: async (r: Redis) => {
        for (let s = 0; s < SERIES; s++) {
          const cmds = Array.from({ length: SAMPLES }, (_, j) => [
            'XADD', `lat:${s}`, `${T0 + j * STEP_MS}-0`, 'v', String(DATA[s][j]),
          ]);
          await pipe(r, cmds as any, 1000);
        }
        return TOTAL;
      },
      encodingProbes: ['lat:0'],
    },
    {
      name: 'C · sharded HASH, ts → value',
      note: 'Cheapest plain-datatype option, but loses range queries entirely.',
      config: { 'hash-max-listpack-entries': 0 },
      load: async (r: Redis) => {
        for (let s = 0; s < SERIES; s++) {
          const cmds: any[] = [];
          for (let i = 0; i < SAMPLES; i += 500) {
            const a: any[] = ['HSET', `lat:${s}`];
            for (let j = i; j < Math.min(i + 500, SAMPLES); j++) {
              a.push(String(j), String(DATA[s][j]));
            }
            cmds.push(a);
          }
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
    },
    {
      name: 'D · TIMESERIES, UNCOMPRESSED',
      note: 'Purpose-built type, compression off: a flat 16 bytes per sample plus chunk headers.',
      load: async (r: Redis) => {
        for (let s = 0; s < SERIES; s++) {
          await r.call('TS.CREATE', `lat:${s}`, 'ENCODING', 'UNCOMPRESSED', 'CHUNK_SIZE', '4096');
        }
        for (let s = 0; s < SERIES; s++) {
          const cmds: any[] = [];
          for (let i = 0; i < SAMPLES; i += 500) {
            const a: any[] = ['TS.MADD'];
            for (let j = i; j < Math.min(i + 500, SAMPLES); j++) {
              a.push(`lat:${s}`, String(T0 + j * STEP_MS), String(DATA[s][j]));
            }
            cmds.push(a);
          }
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
    },
    {
      name: 'E · TIMESERIES, COMPRESSED (integer ms)',
      note: 'Double-delta timestamps + XOR-compressed values, realistic telemetry signal rounded to whole milliseconds.',
      load: async (r: Redis) => {
        for (let s = 0; s < SERIES; s++) {
          await r.call('TS.CREATE', `lat:${s}`, 'ENCODING', 'COMPRESSED', 'CHUNK_SIZE', '4096',
            'LABELS', 'metric', 'tool_latency_ms', 'agent', String(s));
        }
        for (let s = 0; s < SERIES; s++) {
          const cmds: any[] = [];
          for (let i = 0; i < SAMPLES; i += 500) {
            const a: any[] = ['TS.MADD'];
            for (let j = i; j < Math.min(i + 500, SAMPLES); j++) {
              a.push(`lat:${s}`, String(T0 + j * STEP_MS), String(DATA[s][j]));
            }
            cmds.push(a);
          }
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
      probe: async (r) => {
        const info = (await r.call('TS.INFO', 'lat:0')) as any[];
        const o: Record<string, any> = {};
        for (let i = 0; i < info.length; i += 2) o[String(info[i])] = info[i + 1];
        return {
          'chunks': o.chunkCount,
          'samples': o.totalSamples,
          'memory (B)': o.memoryUsage,
          'queryable': 'TS.RANGE / TS.MRANGE by label',
        };
      },
    },
    {
      name: 'F · TIMESERIES, COMPRESSED (same walk, 2 decimals)',
      note: 'The identical signal with two decimal places. The delta against E is what float precision costs you per sample.',
      load: async (r: Redis) => {
        for (let s = 0; s < SERIES; s++) {
          await r.call('TS.CREATE', `lat:${s}`, 'ENCODING', 'COMPRESSED', 'CHUNK_SIZE', '4096');
        }
        for (let s = 0; s < SERIES; s++) {
          const cmds: any[] = [];
          for (let i = 0; i < SAMPLES; i += 500) {
            const a: any[] = ['TS.MADD'];
            for (let j = i; j < Math.min(i + 500, SAMPLES); j++) {
              a.push(`lat:${s}`, String(T0 + j * STEP_MS), String(DATA_FLOAT[s][j]));
            }
            cmds.push(a);
          }
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
    },
    {
      name: 'F2 · TIMESERIES, COMPRESSED (pure noise)',
      note: 'Adversarial control: incompressible values. The honest worst case - nothing here is autocorrelated.',
      load: async (r: Redis) => {
        for (let s = 0; s < SERIES; s++) {
          await r.call('TS.CREATE', `lat:${s}`, 'ENCODING', 'COMPRESSED', 'CHUNK_SIZE', '4096');
        }
        for (let s = 0; s < SERIES; s++) {
          const cmds: any[] = [];
          for (let i = 0; i < SAMPLES; i += 500) {
            const a: any[] = ['TS.MADD'];
            for (let j = i; j < Math.min(i + 500, SAMPLES); j++) {
              a.push(`lat:${s}`, String(T0 + j * STEP_MS), String(NOISE[s][j]));
            }
            cmds.push(a);
          }
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
    },
    {
      caveat: 'lossy - raw retained 24h, then hourly averages only',
      name: 'G · TIMESERIES compressed + 1h downsample rule',
      note: 'Raw kept 24h, hourly aggregates kept a year. The retention lever, expressed as config.',
      load: async (r: Redis) => {
        for (let s = 0; s < SERIES; s++) {
          await r.call('TS.CREATE', `lat:${s}`, 'ENCODING', 'COMPRESSED', 'RETENTION', String(86_400_000));
          await r.call('TS.CREATE', `lat:${s}:1h`, 'ENCODING', 'COMPRESSED', 'RETENTION', String(31_536_000_000));
          await r.call('TS.CREATERULE', `lat:${s}`, `lat:${s}:1h`, 'AGGREGATION', 'avg', String(3_600_000));
        }
        for (let s = 0; s < SERIES; s++) {
          const cmds: any[] = [];
          for (let i = 0; i < SAMPLES; i += 500) {
            const a: any[] = ['TS.MADD'];
            for (let j = i; j < Math.min(i + 500, SAMPLES); j++) {
              a.push(`lat:${s}`, String(T0 + j * STEP_MS), String(DATA[s][j]));
            }
            cmds.push(a);
          }
          await pipe(r, cmds, 50);
        }
        return TOTAL;
      },
      probe: async (r) => ({
        'raw samples retained': ((await r.call('TS.INFO', 'lat:0')) as any[])[
          ((await r.call('TS.INFO', 'lat:0')) as any[]).indexOf('totalSamples') + 1
        ] as any,
        'rollup series': `lat:*:1h (avg over 1h)`,
      }),
    },
  ],
};
