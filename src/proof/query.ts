/**
 * PROOF 04 - "You can only look things up by key, so it can't be a primary DB."
 *
 * This was true in 2015. Redis 8 ships the Query Engine in the open-source
 * build: secondary indexes over HASH and JSON, with filtering, numeric ranges,
 * full-text, sorting, aggregation and vector search, queried with FT.SEARCH /
 * FT.AGGREGATE.
 *
 * But there is a real tension with everything else in this repo, and pretending
 * otherwise would be dishonest:
 *
 *   The Query Engine indexes KEYS MATCHING A PREFIX. The sharded-hash layout
 *   that produced the biggest RAM win in case 01 stores 128 runs inside ONE
 *   key - and the Query Engine cannot see inside that. Sharding and
 *   FT.SEARCH are, straightforwardly, incompatible.
 *
 * So this proof measures the actual trade rather than hiding it, comparing
 * three designs on the same 100k runs:
 *
 *   A. Sharded hashes + hand-rolled indexes. Cheapest RAM. You write the
 *      index maintenance yourself, inside the same Lua script that does the
 *      state transition (proof 02), so it stays consistent.
 *   B. One hash per run + FT index. Full query language, much more RAM.
 *   C. HYBRID - the recommendation. Fat record in a sharded hash; a small
 *      index-only document per run holding just the queryable fields. You pay
 *      for indexing the columns you actually filter on, not for indexing the
 *      whole record.
 *
 * All three answer the same four queries, and the answers are checked against
 * a ground truth computed in JS so a fast-but-wrong index cannot win.
 */
import fs from 'node:fs';
import path from 'node:path';
import type Redis from 'ioredis';
import { connect, pipe, configSet, DEFAULT_ENCODING_CONFIG } from '../lib/redis.ts';
import { sample } from '../lib/measure.ts';
import { makeRun, rng, type AgentRun } from '../lib/rng.ts';
import { packRun, uuidToBytes } from '../lib/codec.ts';
import { Report, stats } from './harness.ts';

const N = 100_000;
const SHARD = 128;
const SHARDS = Math.ceil(N / SHARD);
const SEED = 777333;

const DATA: AgentRun[] = (() => {
  const r = rng(SEED);
  const runs = Array.from({ length: N }, (_, i) => makeRun(r, i));
  // makeRun gives every run a fresh tenant uuid, which would make the
  // tenant filter match exactly one row and prove nothing. Real platforms have
  // far more runs than tenants, so redistribute across a realistic pool.
  const pool = runs.slice(0, 40).map((x) => x.tenant_id);
  runs.forEach((x, i) => { x.tenant_id = pool[i % pool.length]; });
  return runs;
})();

const T0 = DATA[0].tenant_id;

/** Ground truth, computed in JS. Every index must reproduce these exactly. */
const TRUTH = {
  runningCount: DATA.filter((d) => d.status === 'running').length,
  tenantFailed: DATA.filter((d) => d.tenant_id === T0 && d.status === 'failed').length,
  expensive: DATA.filter((d) => d.cost_micros > 8_000_000).length,
  opusRunning: DATA.filter((d) => d.model === 'claude-opus-5' && d.status === 'running').length,
};

const report = new Report(
  'Querying without a key',
  'You can only look things up by key, so Redis cannot be a primary database.',
);

const r = connect();
const results: Record<string, any> = {};

async function reset() {
  await r.flushall('SYNC');
  await configSet(r, DEFAULT_ENCODING_CONFIG);
}

async function timed(fn: () => Promise<number>, reps: number): Promise<{ ms: any; n: number }> {
  let n = 0;
  const lat: number[] = [];
  for (let i = 0; i < reps; i++) {
    const t = performance.now();
    n = await fn();
    lat.push(performance.now() - t);
  }
  return { ms: stats(lat), n };
}

// ── A · sharded hashes + hand-rolled inverted indexes ────────────────────

await reset();
await configSet(r, { 'hash-max-listpack-entries': 256, 'hash-max-listpack-value': 512 });
let before = await sample(r);
{
  const cmds: any[] = [];
  DATA.forEach((run, i) => {
    const s = i % SHARDS;
    cmds.push(['HSET', `run:{${s}}`, uuidToBytes(run.run_id), packRun(run)]);
    // hand-rolled indexes, co-located in the same slot as their shard
    cmds.push(['SADD', `ix:{${s}}:st:${run.status}`, String(i)]);
    cmds.push(['SADD', `ix:{${s}}:md:${run.model}`, String(i)]);
    cmds.push(['SADD', `ix:{${s}}:tn:${run.tenant_id}`, String(i)]);
    cmds.push(['ZADD', `ix:{${s}}:cost`, String(run.cost_micros), String(i)]);
  });
  await pipe(r, cmds, 1000);
}
let after = await sample(r);
const aBytes = after.attributable - before.attributable;

const aRunning = await timed(async () => {
  let n = 0;
  const p = r.pipeline();
  for (let s = 0; s < SHARDS; s++) p.scard(`ix:{${s}}:st:running`);
  for (const [, v] of (await p.exec())!) n += Number(v);
  return n;
}, 5);

const aTenantFailed = await timed(async () => {
  let n = 0;
  const p = r.pipeline();
  for (let s = 0; s < SHARDS; s++) p.sintercard(2, `ix:{${s}}:tn:${T0}`, `ix:{${s}}:st:failed`);
  for (const [, v] of (await p.exec())!) n += Number(v);
  return n;
}, 5);

const aExpensive = await timed(async () => {
  let n = 0;
  const p = r.pipeline();
  for (let s = 0; s < SHARDS; s++) p.zcount(`ix:{${s}}:cost`, '(8000000', '+inf');
  for (const [, v] of (await p.exec())!) n += Number(v);
  return n;
}, 5);

const aOpusRunning = await timed(async () => {
  let n = 0;
  const p = r.pipeline();
  for (let s = 0; s < SHARDS; s++) {
    p.sintercard(2, `ix:{${s}}:md:claude-opus-5`, `ix:{${s}}:st:running`);
  }
  for (const [, v] of (await p.exec())!) n += Number(v);
  return n;
}, 5);

results.A = {
  design: 'sharded hash + hand-rolled indexes',
  bytesPerRun: aBytes / N,
  queries: { running: aRunning, tenantFailed: aTenantFailed, expensive: aExpensive, opusRunning: aOpusRunning },
};

report.assert(
  `hand-rolled indexes answer all four queries correctly at ${(aBytes / N).toFixed(0)} B/run ` +
    `(status count p50 ${aRunning.ms.p50} ms, two-way intersect p50 ${aOpusRunning.ms.p50} ms)`,
  aRunning.n === TRUTH.runningCount &&
    aTenantFailed.n === TRUTH.tenantFailed &&
    aExpensive.n === TRUTH.expensive &&
    aOpusRunning.n === TRUTH.opusRunning,
  'SINTERCARD and ZCOUNT do the set algebra server-side; the fan-out across shards pipelines into one round trip',
);

// ── B · one hash per run + Query Engine index ────────────────────────────

await reset();
before = await sample(r);
{
  try {
    await r.call('FT.DROPINDEX', 'idx:runs');
  } catch { /* not there yet */ }
  await r.call(
    'FT.CREATE', 'idx:runs', 'ON', 'HASH', 'PREFIX', '1', 'run:', 'SCHEMA',
    'status', 'TAG',
    'model', 'TAG',
    'tenant_id', 'TAG',
    'cost_micros', 'NUMERIC', 'SORTABLE',
    'created_at', 'NUMERIC', 'SORTABLE',
  );
  const cmds: any[] = [];
  for (const run of DATA) {
    const a: any[] = ['HSET', `run:${run.run_id}`];
    for (const [k, v] of Object.entries(run)) a.push(k, String(v));
    cmds.push(a);
  }
  await pipe(r, cmds, 500);
  // wait for background indexing to drain
  for (;;) {
    const info = (await r.call('FT.INFO', 'idx:runs')) as any[];
    const o: Record<string, any> = {};
    for (let i = 0; i < info.length; i += 2) o[String(info[i])] = info[i + 1];
    if (Number(o.indexing) === 0 && Number(o.num_docs) >= N) break;
    await new Promise((s) => setTimeout(s, 100));
  }
}
after = await sample(r);
const bBytes = after.attributable - before.attributable;

const ftCount = async (q: string) => {
  const res = (await r.call('FT.SEARCH', 'idx:runs', q, 'LIMIT', '0', '0')) as any[];
  return Number(res[0]);
};

const bRunning = await timed(() => ftCount('@status:{running}'), 5);
const bTenantFailed = await timed(() => ftCount(`@tenant_id:{${T0.replace(/-/g, '\\-')}} @status:{failed}`), 5);
const bExpensive = await timed(() => ftCount('@cost_micros:[(8000000 +inf]'), 5);
const bOpusRunning = await timed(() => ftCount('@model:{claude\\-opus\\-5} @status:{running}'), 5);

const ftInfo = (await r.call('FT.INFO', 'idx:runs')) as any[];
const fi: Record<string, any> = {};
for (let i = 0; i < ftInfo.length; i += 2) fi[String(ftInfo[i])] = ftInfo[i + 1];

results.B = {
  design: 'hash per run + FT.SEARCH index',
  bytesPerRun: bBytes / N,
  indexMemoryMb: fi.total_index_memory_sz_mb ?? fi.inverted_sz_mb,
  queries: { running: bRunning, tenantFailed: bTenantFailed, expensive: bExpensive, opusRunning: bOpusRunning },
};

report.assert(
  `the Query Engine answers the same four queries correctly at ${(bBytes / N).toFixed(0)} B/run ` +
    `(status count p50 ${bRunning.ms.p50} ms, two-tag intersect p50 ${bOpusRunning.ms.p50} ms)`,
  bRunning.n === TRUTH.runningCount &&
    bTenantFailed.n === TRUTH.tenantFailed &&
    bExpensive.n === TRUTH.expensive &&
    bOpusRunning.n === TRUTH.opusRunning,
  'FT.SEARCH gives you a real query language - TAG filters, NUMERIC ranges, sorting, aggregation - over ordinary hashes',
);

// ── C · hybrid: fat record sharded, thin index document per run ──────────

await reset();
await configSet(r, { 'hash-max-listpack-entries': 256, 'hash-max-listpack-value': 512 });
before = await sample(r);
{
  try {
    await r.call('FT.DROPINDEX', 'idx:ix');
  } catch { /* fine */ }
  await r.call(
    'FT.CREATE', 'idx:ix', 'ON', 'HASH', 'PREFIX', '1', 'ix:', 'SCHEMA',
    's', 'TAG', 'm', 'TAG', 't', 'TAG', 'c', 'NUMERIC', 'SORTABLE',
  );
  const cmds: any[] = [];
  DATA.forEach((run, i) => {
    // the fat, packed record lives in a shared shard - invisible to the index
    cmds.push(['HSET', `run:{${i % SHARDS}}`, uuidToBytes(run.run_id), packRun(run)]);
    // …and a tiny document carries ONLY the columns we filter on
    cmds.push([
      'HSET', `ix:${i}`,
      's', run.status, 'm', run.model, 't', run.tenant_id, 'c', String(run.cost_micros),
    ]);
  });
  await pipe(r, cmds, 500);
  for (;;) {
    const info = (await r.call('FT.INFO', 'idx:ix')) as any[];
    const o: Record<string, any> = {};
    for (let i = 0; i < info.length; i += 2) o[String(info[i])] = info[i + 1];
    if (Number(o.indexing) === 0 && Number(o.num_docs) >= N) break;
    await new Promise((s) => setTimeout(s, 100));
  }
}
after = await sample(r);
const cBytes = after.attributable - before.attributable;

const ixCount = async (q: string) => {
  const res = (await r.call('FT.SEARCH', 'idx:ix', q, 'LIMIT', '0', '0')) as any[];
  return Number(res[0]);
};
const cRunning = await timed(() => ixCount('@s:{running}'), 5);
const cTenantFailed = await timed(() => ixCount(`@t:{${T0.replace(/-/g, '\\-')}} @s:{failed}`), 5);
const cExpensive = await timed(() => ixCount('@c:[(8000000 +inf]'), 5);
const cOpusRunning = await timed(() => ixCount('@m:{claude\\-opus\\-5} @s:{running}'), 5);

results.C = {
  design: 'hybrid: sharded packed record + thin index doc',
  bytesPerRun: cBytes / N,
  queries: { running: cRunning, tenantFailed: cTenantFailed, expensive: cExpensive, opusRunning: cOpusRunning },
};

report.assert(
  `hybrid keeps the full query language at ${(cBytes / N).toFixed(0)} B/run vs ${(bBytes / N).toFixed(0)} B/run for the plain FT design ` +
    `(${(bBytes / cBytes).toFixed(2)}× cheaper)`,
  cRunning.n === TRUTH.runningCount &&
    cTenantFailed.n === TRUTH.tenantFailed &&
    cExpensive.n === TRUTH.expensive &&
    cOpusRunning.n === TRUTH.opusRunning &&
    cBytes < bBytes,
  'index the four columns you filter on, not the twenty you store - the record itself stays packed and sharded',
);

report.assert(
  `all three designs return identical, correct answers (running=${TRUTH.runningCount.toLocaleString()}, ` +
    `tenant+failed=${TRUTH.tenantFailed}, cost>8M=${TRUTH.expensive.toLocaleString()}, opus+running=${TRUTH.opusRunning.toLocaleString()})`,
  aRunning.n === bRunning.n && bRunning.n === cRunning.n && cRunning.n === TRUTH.runningCount,
  'checked against ground truth computed in JS, so a fast-but-wrong index cannot win this comparison',
);

report.info(
  `RAM per run - hand-rolled ${(aBytes / N).toFixed(0)} B · hybrid ${(cBytes / N).toFixed(0)} B · full FT index ${(bBytes / N).toFixed(0)} B`,
);
report.info(
  'the honest limitation: FT.SEARCH indexes keys by prefix, so it cannot see inside a sharded hash. ' +
    'If you want both the query language and the sharding win, the hybrid layout is how you get them.',
);

await r.flushall('SYNC');
await r.quit();

const summary = report.summary();
const dir = path.resolve(import.meta.dirname, '../../results');
fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(
  path.join(dir, 'proof-query.json'),
  JSON.stringify({ ...summary, truth: TRUTH, designs: results }, null, 2),
);
console.log('  → results/proof-query.json');
process.exit(summary.passed ? 0 : 1);
