/**
 * CASE 08 — The allocator's staircase.
 *
 * Redis does not charge you for the bytes you stored; it charges you for the
 * jemalloc size class your allocation landed in. Size classes go
 * 8,16,32,48,64,80,96,112,128,160,192,224,256,320,… so a 129-byte value costs
 * the same as a 160-byte one, and a 257-byte value costs the same as 320.
 *
 * This is why "shave 3 bytes off the record" is sometimes worth 20% and
 * usually worth nothing: the only savings that count are the ones that cross a
 * class boundary. Measure the staircase, then design your records to land just
 * under a step.
 *
 * There is also a Redis-specific cliff, and it is NOT the "44-byte embstr
 * limit" that everyone (this file included, at first) repeats. In Redis 8 the
 * decision in `kvobjSet` is:
 *
 *     16 + (keylen + 3) + (4 + vallen) <= 64
 *
 * Key length and value length share ONE 64-byte budget. A short key buys you a
 * longer embedded value and vice versa, so where your cliff falls depends on
 * your key naming — which is a second, independent reason to keep key names
 * short. These rows use a fixed 8-character key so the value axis is clean.
 */
import type Redis from 'ioredis';
import { pipe } from '../lib/redis.ts';
import type { BenchCase, Variant } from '../lib/measure.ts';

const N = 50_000;
const SIZES = [
  8, 16, 24, 32, 40, 44, 45, 48, 56, 64, 96, 112, 128, 129, 160, 192, 256, 257, 384, 512,
  // the large-object regime, where a 1536-d float32 vector (6,144 B) lives
  1024, 2048, 4096, 5120, 6144, 6145, 7168, 8192,
];

function variantFor(size: number): Variant {
  return {
    name: `${String(size).padStart(3)} B value`,
    note:
      size === 45 ? 'past the shared key+value embedding budget'
      : size === 129 || size === 257 || size === 6145 ? 'one byte past a size-class boundary'
      : size === 6144 ? 'a 1536-d float32 vector — see case 04'
      : '',
    load: async (r: Redis) => {
      const val = Buffer.alloc(size, 0x61);
      const cmds: any[] = [];
      for (let i = 0; i < N; i++) cmds.push(['SET', `k:${String(i).padStart(6, '0')}`, val]);
      await pipe(r, cmds, 2000);
      return N;
    },
    encodingProbes: ['k:000000'],
    probe: async (r) => ({
      'MEMORY USAGE (B)': (await r.memory('USAGE', 'k:000000')) as number,
      'payload (B)': size,
    }),
  };
}

export const case08: BenchCase = {
  id: '08-size-class',
  title: `Allocator size classes (${N.toLocaleString()} keys per value size, fixed 8-char key)`,
  question: 'Which byte savings actually reduce the bill, and which are free but pointless?',
  unit: 'key',
  kind: 'sweep',
  variants: SIZES.map(variantFor),
};
