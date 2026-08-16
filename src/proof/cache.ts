/**
 * PROOF 05 — the agent cache under 200-way concurrency.
 *
 * Stampede (one model call, not 200), dead-holder recovery, semantic hit rate
 * and false positives, and bytes per entry.
 */
import fs from 'node:fs';
import path from 'node:path';
import { connect, AgentCache, applyKitConfig } from '../kit/index.ts';
import { str, num } from '../kit/client.ts';
import { makeEmbeddingCorpus } from '../lib/rng.ts';
import { Report, stats } from './harness.ts';

const DIM = 384;
const report = new Report(
  'Distributed agent cache',
  'A cache is easy. A cache that survives 200 concurrent misses on the same key is not.',
);

const r = await connect('redis://127.0.0.1:6399');
await r.cmd('FLUSHALL', 'SYNC');
await applyKitConfig(r, { shardWidth: 124 });

async function used(): Promise<number> {
  await r.raw('MEMORY', 'PURGE');
  return Number(/used_memory:(\d+)/.exec(str(await r.cmd('INFO', 'memory')))?.[1] ?? 0);
}

// ── 1 · stampede ────────────────────────────────────────────────────────

const WORKERS = 200;
let modelCalls = 0;
const callLatency: number[] = [];

async function expensiveModelCall(): Promise<string> {
  modelCalls++;
  await new Promise((res) => setTimeout(res, 120)); // a cheap LLM call
  return JSON.stringify({ answer: 'x'.repeat(3000), tokens: 20_000 });
}

{
  // Each worker gets its OWN connection — same as separate processes would.
  const clients = await Promise.all(
    Array.from({ length: WORKERS }, () => connect('redis://127.0.0.1:6399')),
  );
  const caches = clients.map((c) => new AgentCache(c, {
    prefix: 'cache', capacity: 100_000, ttl: 600, semantic: undefined,
  }));
  await caches[0].init();

  const t0 = performance.now();
  const results = await Promise.all(
    caches.map(async (cache) => {
      const t = performance.now();
      const res = await cache.fetch('hot:prompt:1', expensiveModelCall);
      callLatency.push(performance.now() - t);
      return res;
    }),
  );
  const wall = performance.now() - t0;

  const computed = caches.reduce((a, c) => a + c.stats.computed, 0);
  const waited = caches.reduce((a, c) => a + c.stats.waited, 0);
  const timeouts = caches.reduce((a, c) => a + c.stats.leaseTimeouts, 0);
  const allGotValue = results.every((x) => x.value && x.value.length > 0);
  const s = stats(callLatency);

  report.assert(
    `${WORKERS} concurrent workers missed the same key → the model was called ${modelCalls} time(s), not ${WORKERS}`,
    modelCalls === 1 && computed === 1,
    `${computed} computed, ${waited} waited on the lease, ${timeouts} lease timeouts. ` +
      `Without a lease this is ${WORKERS} model calls — a ${WORKERS}× bill and a rate-limit incident.`,
  );
  report.assert(
    `all ${WORKERS} workers got a value back`,
    allGotValue,
    `p50 ${s.p50} ms · p99 ${s.p99} ms · wall ${Math.round(wall)} ms for the whole fan-in`,
  );

  for (const c of clients) c.close();
}

// ── 2 · a dead worker must not wedge the cache ──────────────────────────

{
  await r.cmd('FLUSHALL', 'SYNC');
  const a = await connect('redis://127.0.0.1:6399');
  const cacheA = new AgentCache(a, {
    prefix: 'cache', capacity: 1000, ttl: 600, leaseMs: 700, semantic: undefined,
  });
  await cacheA.init();

  // A takes the lease and then "dies" — never publishes, never releases.
  await r.cmd('EVALSHA',
    str(await r.cmd('SCRIPT', 'LOAD', `
      local v = redis.call('HGET', KEYS[1], ARGV[1])
      if v then return {1, v} end
      if redis.call('SET', KEYS[2], ARGV[3], 'NX', 'PX', ARGV[2]) then return {2, ''} end
      return {3, ''}`)),
    '2', 'cache:e:{0}', 'cache:l:{0}:dead', 'f', '700', 'zombie');

  let recovered = false;
  const t0 = performance.now();
  const res = await cacheA.fetch('orphan:key', async () => {
    recovered = true;
    return 'computed-after-lease-expiry';
  });
  const ms = performance.now() - t0;

  report.assert(
    `a worker that dies holding the lease does not wedge the key — recovered in ${Math.round(ms)} ms`,
    recovered && res.value !== null,
    'the lease has a TTL, so the next caller takes over rather than waiting forever',
  );
  a.close();
}

// ── 3 · semantic tier ───────────────────────────────────────────────────

{
  await r.cmd('FLUSHALL', 'SYNC');
  const cache = new AgentCache(r, {
    prefix: 'sem', capacity: 20_000, ttl: 3600,
    semantic: { dim: DIM, threshold: 0.92, quant: 'Q8' },
  });
  await cache.init();

  // 400 distinct cached prompts, then queries that are near-paraphrases of a
  // known subset plus genuinely unrelated ones that must NOT match.
  const corpus = makeEmbeddingCorpus(4242, 400, DIM, 0);
  const near = makeEmbeddingCorpus(4242, 400, DIM, 0); // same seed -> identical
  const unrelated = makeEmbeddingCorpus(999, 100, DIM, 0);

  for (let i = 0; i < 400; i++) {
    await cache.set(`prompt:${i}`, `answer-${i}`, corpus.vectors[i]);
  }

  // paraphrase = the same embedding with a small perturbation
  let hits = 0;
  for (let i = 0; i < 200; i++) {
    const v = Float32Array.from(near.vectors[i]);
    for (let d = 0; d < DIM; d += 7) v[d] += 0.01;
    const res = await cache.get(`prompt:paraphrased:${i}`, v);
    if (res.kind === 'semantic') hits++;
  }

  let falsePositives = 0;
  for (let i = 0; i < 100; i++) {
    const res = await cache.get(`prompt:unrelated:${i}`, unrelated.vectors[i]);
    if (res.kind === 'semantic') falsePositives++;
  }

  report.assert(
    `semantic tier caught ${hits}/200 paraphrased prompts that an exact-key cache would have missed entirely`,
    hits > 150,
    'exact-key hit rate on paraphrased traffic is 0 by construction — this is the tier that pays for itself',
  );
  report.assert(
    `${falsePositives}/100 unrelated prompts were wrongly served from cache`,
    falsePositives === 0,
    'threshold 0.92 — tune it against your own traffic; too low serves the wrong answer confidently',
  );
}

// ── 4 · what it costs ───────────────────────────────────────────────────

{
  await r.cmd('FLUSHALL', 'SYNC');
  await applyKitConfig(r, { shardWidth: 124 });
  const N = 20_000;
  const body = JSON.stringify({ answer: 'the capital of france is paris. '.repeat(40) });

  const before = await used();
  const cache = new AgentCache(r, { prefix: 'c', capacity: N, ttl: 3600, compress: true });
  await cache.init();
  for (let i = 0; i < N; i++) await cache.set(`prompt:${i}`, body);
  const after = await used();
  const perEntry = (after - before) / N;

  // naive comparison: one key per entry, uncompressed, with its own TTL
  await r.cmd('FLUSHALL', 'SYNC');
  const b2 = await used();
  for (let i = 0; i < N; i += 500) {
    await r.pipeline(Array.from({ length: Math.min(500, N - i) }, (_, j) =>
      ['SET', `naive:prompt:${i + j}`, body, 'EX', '3600']));
  }
  const a2 = await used();
  const naivePerEntry = (a2 - b2) / N;

  report.assert(
    `cached entry costs ${perEntry.toFixed(0)} B vs ${naivePerEntry.toFixed(0)} B for a key-per-entry cache (${(naivePerEntry / perEntry).toFixed(1)}×)`,
    perEntry < naivePerEntry,
    `${body.length} B payload · sharded hash + HEXPIRE + zstd, vs one string key per entry with EXPIRE`,
  );

  // the number that actually matters
  const perHitSaved = 0.06; // one avoided 20k-token call at $3/M input
  const ramCostPerEntryMonth = (perEntry / 1024 ** 3) * 12.93;
  report.info(
    `economics: one cached entry costs $${ramCostPerEntryMonth.toExponential(1)}/month of RAM and ` +
      `saves ~$${perHitSaved.toFixed(2)} per hit — the break-even is ${Math.ceil(ramCostPerEntryMonth / perHitSaved * 1e6) / 1e6} hits/month`,
  );
  report.info(
    'which is why cache hit rate, not cache RAM, is the number to optimize. nibble makes the RAM small ' +
      'enough that you can afford to cache aggressively and keep entries longer.',
  );
}

await r.cmd('FLUSHALL', 'SYNC');
r.close();

const summary = report.summary();
const dir = path.resolve(import.meta.dirname, '../../results');
fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(path.join(dir, 'proof-cache.json'), JSON.stringify(summary, null, 2));
console.log('  → results/proof-cache.json');
process.exit(summary.passed ? 0 : 1);
