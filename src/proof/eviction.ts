/**
 * PROOF 03 — "Redis will silently delete my data when it fills up."
 *
 * This one is real, it is the scariest failure mode Redis has, and almost every
 * team that hits it hits it the same way: they ran Redis as a cache, set
 * `maxmemory-policy allkeys-lru` because that is what every cache tutorial
 * says, then started keeping something important in the same instance. Redis
 * did exactly what it was told and threw the important thing away. No error, no
 * log line at the write that caused it, no way to tell what went.
 *
 * The fix is not clever, it is just *deliberate*: eviction is a per-key
 * property in disguise, and `volatile-*` policies expose it. A key with no TTL
 * is never a candidate under a `volatile-*` policy. So:
 *
 *     primary records  →  written WITHOUT a TTL   →  never evictable
 *     cache entries    →  written WITH a TTL      →  evictable, by design
 *
 * and one instance safely holds both. When memory runs out, Redis evicts the
 * cache and starts refusing writes with an OOM error — loudly, at the moment it
 * happens, to the client that caused it — instead of quietly dropping state.
 *
 * This file demonstrates the data loss first, then the fix, then verifies the
 * failure is loud. It also measures the thing people forget: `maxmemory`
 * accounts for the *whole* instance, so replication buffers and client buffers
 * push you into eviction even when your dataset hasn't grown.
 */
import fs from 'node:fs';
import path from 'node:path';
import type Redis from 'ioredis';
import { startRedis, rmContainer, waitReady, Report } from './harness.ts';

const PORT = 6397;
const NAME = 'redops-evict';
const MAXMEM = 64 * 1024 * 1024; // 64 MiB — small enough to fill quickly

const PRIMARY = 20_000; // durable agent-run records; MUST survive
const PAYLOAD = Buffer.alloc(400, 0x41);

const report = new Report(
  'Eviction safety',
  'Redis will silently delete my data when it fills up.',
);

/** Write the primary records, then flood with cache traffic until full. */
async function fill(r: Redis, ttlOnCache: boolean) {
  for (let i = 0; i < PRIMARY; i += 500) {
    const p = r.pipeline();
    for (let j = i; j < Math.min(i + 500, PRIMARY); j++) p.set(`run:${j}`, PAYLOAD);
    await p.exec();
  }
  const seeded = await r.dbsize();

  let oomErrors = 0;
  let cacheWrites = 0;
  for (let i = 0; i < 400_000; i += 500) {
    const p = r.pipeline();
    for (let j = i; j < i + 500; j++) {
      if (ttlOnCache) p.set(`cache:${j}`, PAYLOAD, 'EX', 3600);
      else p.set(`cache:${j}`, PAYLOAD);
    }
    const res = await p.exec();
    for (const [err] of res!) {
      if (err) oomErrors++;
      else cacheWrites++;
    }
    if (oomErrors > 2000) break; // it is clearly refusing; stop hammering
  }
  return { seeded, cacheWrites, oomErrors };
}

async function survivors(r: Redis): Promise<number> {
  let alive = 0;
  for (let i = 0; i < PRIMARY; i += 1000) {
    const p = r.pipeline();
    for (let j = i; j < Math.min(i + 1000, PRIMARY); j++) p.exists(`run:${j}`);
    const res = await p.exec();
    for (const [, v] of res!) alive += Number(v);
  }
  return alive;
}

const outcomes: Record<string, any> = {};

async function scenario(
  label: string,
  policy: string,
  ttlOnCache: boolean,
): Promise<{ alive: number; oomErrors: number; cacheWrites: number; evicted: number }> {
  startRedis({
    name: NAME, port: PORT,
    args: [
      '--appendonly', 'no', '--save', '',
      '--maxmemory', String(MAXMEM),
      '--maxmemory-policy', policy,
    ],
  });
  const r = await waitReady(PORT);
  const { cacheWrites, oomErrors } = await fill(r, ttlOnCache);
  const alive = await survivors(r);
  const evicted = Number(/evicted_keys:(\d+)/.exec(await r.info('stats'))![1]);
  r.disconnect();
  rmContainer(NAME);
  outcomes[label] = { policy, ttlOnCache, alive, lost: PRIMARY - alive, oomErrors, cacheWrites, evicted };
  return { alive, oomErrors, cacheWrites, evicted };
}

// ── 1 · The failure: the cache policy everyone copies ─────────────────────

const lru = await scenario('allkeys-lru (the trap)', 'allkeys-lru', true);
report.assert(
  `allkeys-lru destroyed ${(PRIMARY - lru.alive).toLocaleString()} of ${PRIMARY.toLocaleString()} durable records ` +
    `(${(((PRIMARY - lru.alive) / PRIMARY) * 100).toFixed(1)}%), with ${lru.oomErrors} errors raised`,
  lru.alive < PRIMARY,
  `Redis evicted ${lru.evicted.toLocaleString()} keys and reported success on every single write. ` +
    'The data is gone and nothing told the application. This is the objection, and it is completely valid.',
);

// ── 2 · The same trap with allkeys-random, to show it is the *policy* ──────

const rnd = await scenario('allkeys-random', 'allkeys-random', true);
report.assert(
  `allkeys-random likewise destroyed ${(PRIMARY - rnd.alive).toLocaleString()} durable records`,
  rnd.alive < PRIMARY,
  'every allkeys-* policy treats your primary records as cache. The word "allkeys" is not decoration.',
);

// ── 3 · The fix: volatile-ttl + TTL as the eviction opt-in ────────────────

const vol = await scenario('volatile-ttl + TTL-tagged cache', 'volatile-ttl', true);
report.assert(
  `volatile-ttl preserved all ${vol.alive.toLocaleString()}/${PRIMARY.toLocaleString()} durable records ` +
    `while evicting ${vol.evicted.toLocaleString()} cache keys`,
  vol.alive === PRIMARY,
  'keys written without a TTL are not eviction candidates under any volatile-* policy — ' +
    'so "is this row durable?" becomes "did I set a TTL?", which is a decision you make per write',
);

// ── 4 · And the failure is loud, not silent ──────────────────────────────

const strict = await scenario('noeviction (strictest)', 'noeviction', false);
report.assert(
  `noeviction preserved all ${strict.alive.toLocaleString()} records and raised ${strict.oomErrors.toLocaleString()} OOM errors instead of deleting anything`,
  strict.alive === PRIMARY && strict.oomErrors > 0,
  'the write fails, synchronously, on the connection that caused it — an incident you can page on, ' +
    'rather than a silent corruption you discover next quarter',
);

// ── 5 · What the OOM error actually looks like ───────────────────────────

startRedis({
  name: NAME, port: PORT,
  args: ['--appendonly', 'no', '--save', '', '--maxmemory', String(4 * 1024 * 1024), '--maxmemory-policy', 'noeviction'],
});
const rr = await waitReady(PORT);
let oomMessage = '';
try {
  for (let i = 0; i < 200_000; i++) await rr.set(`x:${i}`, PAYLOAD);
} catch (e: any) {
  oomMessage = String(e?.message ?? '');
}
report.assert(
  `the error is explicit and machine-readable: "${oomMessage.slice(0, 78)}…"`,
  /OOM/.test(oomMessage),
  'clients can detect the OOM prefix and shed load, rather than silently succeeding against a shrinking dataset',
);

// ── 6 · maxmemory is instance-wide, not dataset-wide ─────────────────────

const before = await rr.info('memory');
const overheadPct =
  (Number(/used_memory_overhead:(\d+)/.exec(before)![1]) /
    Number(/used_memory:(\d+)/.exec(before)![1])) * 100;
report.info(
  `at the eviction boundary, ${overheadPct.toFixed(1)}% of used_memory was overhead (key dict, client buffers, ` +
    'replication backlog) rather than values — budget maxmemory against the whole instance, not your record count',
);
report.info(
  'set maxmemory-clients (default 0 = unlimited) or a single slow consumer\'s output buffer can push a ' +
    'healthy dataset into eviction without your data growing at all',
);

rr.disconnect();
rmContainer(NAME);

const summary = report.summary();
const dir = path.resolve(import.meta.dirname, '../../results');
fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(
  path.join(dir, 'proof-eviction.json'),
  JSON.stringify({ ...summary, scenarios: outcomes, oomMessage, overheadPct: +overheadPct.toFixed(1) }, null, 2),
);
console.log('  → results/proof-eviction.json');
process.exit(summary.passed ? 0 : 1);
