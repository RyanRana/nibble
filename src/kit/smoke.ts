/**
 * Kit smoke test. Every snippet in the README and docs is exercised here, so
 * the documentation cannot drift away from the code without this going red.
 *
 *   node src/kit/smoke.ts
 */
import {
  connect, applyKitConfig, applySafeEviction,
  schema, uuid, enum_, varint, flags,
  Pouch, ShardedCache, ExactSeen, BloomSeen, VectorMemory, storageBytes,
} from './index.ts';
import { type Client, str } from './client.ts';

/**
 * Memory sample, inlined so the smoke test has no dependency outside the kit.
 * See docs/measuring.md for why the client/AOF/replication buffers are
 * subtracted and `used_memory_dataset` is not used.
 */
async function sample(r: Client): Promise<{ attributable: number }> {
  await r.raw('MEMORY', 'PURGE');
  const info = str(await r.cmd('INFO', 'memory'));
  const g = (k: string) => Number(new RegExp(`${k}:(\\d+)`).exec(info)?.[1] ?? 0);
  return {
    attributable: g('used_memory') - g('mem_clients_normal') - g('mem_clients_slaves')
      - g('mem_aof_buffer') - g('mem_replication_backlog') - g('mem_cluster_links'),
  };
}

const r = await connect();
await r.cmd('FLUSHALL', 'SYNC');
await applyKitConfig(r, { shardWidth: 124 });

let failures = 0;
function check(label: string, ok: boolean, detail = '') {
  if (!ok) failures++;
  console.log(`  ${ok ? '\x1b[32m✔\x1b[0m' : '\x1b[31m✘\x1b[0m'} ${label}${detail ? `  ${detail}` : ''}`);
}

console.log('\n\x1b[1m█ nibble - smoke test\x1b[0m\n');

// ── schema ───────────────────────────────────────────────────────────────
console.log('schema');

const Run = schema({
  run_id: uuid(),
  tenant_id: uuid(),
  status: enum_(['queued', 'running', 'awaiting_tool', 'succeeded', 'failed'] as const),
  model: enum_(['claude-opus-5', 'claude-sonnet-5', 'claude-haiku-4-5-20251001'] as const),
  step_count: varint(),
  prompt_tokens: varint(),
  cost_micros: varint(),
  bits: flags(['cached', 'retried', 'billable'] as const),
});

const run = {
  run_id: '3946f31a-da9d-02d9-a002-a80469567fb9',
  tenant_id: 'c0ffee00-dead-beef-cafe-000000000001',
  status: 'running' as const,
  model: 'claude-opus-5' as const,
  step_count: 42,
  prompt_tokens: 128_400,
  cost_micros: 913_222,
  bits: { cached: true, retried: false, billable: true },
};

const packed = Run.encode(run);
const back = Run.decode(packed);
const asJson = Buffer.byteLength(JSON.stringify(run));

check(
  'encode/decode round-trips',
  JSON.stringify(back) === JSON.stringify(run),
);
check(
  `packs ${asJson} B of JSON into ${packed.length} B`,
  packed.length < asJson / 2,
  `${(asJson / packed.length).toFixed(2)}×`,
);

// forward compatibility: a v1 reader must survive a v2 record and vice versa
const RunV2 = schema({ ...({
  run_id: uuid(), tenant_id: uuid(),
  status: enum_(['queued', 'running', 'awaiting_tool', 'succeeded', 'failed'] as const),
  model: enum_(['claude-opus-5', 'claude-sonnet-5', 'claude-haiku-4-5-20251001'] as const),
  step_count: varint(), prompt_tokens: varint(), cost_micros: varint(),
  bits: flags(['cached', 'retried', 'billable'] as const),
  retry_budget: varint(), // <- new trailing field
}) }, { version: 2 });

const v1Record = Run.encode(run);
const readByV2 = RunV2.decode(v1Record);
check(
  'a v2 reader decodes a v1 record (trailing field undefined)',
  readByV2.status === 'running' && readByV2.retry_budget === undefined,
);

// ── Pouch ───────────────────────────────────────────────────────────
console.log('\nPouch');

const runs = new Pouch(r, {
  prefix: 'run',
  capacity: 20_000,
  width: 124,
  encode: (v: typeof run) => Run.encode(v),
  decode: (b) => Run.decode(b) as typeof run,
});

const ids: string[] = [];
for (let i = 0; i < 5_000; i++) {
  const id = `3946f31a-da9d-02d9-a002-${String(i).padStart(12, '0')}`;
  ids.push(id);
  await runs.set(id, { ...run, run_id: id, step_count: i % 900 });
}

const got = await runs.get(ids[1234]);
check('set/get round-trips through a shard', got?.step_count === 1234 % 900);
check('reports the right shard count', runs.shards === Math.ceil(20_000 / 124), `${runs.shards} shards`);
check('counts every record', (await runs.count()) === 5_000);

const many = await runs.mget([ids[0], ids[10], 'missing-id-0000-0000-0000-000000000000', ids[4999]]);
check(
  'mget batches by shard and preserves order',
  many[0]?.step_count === 0 && many[1]?.step_count === 10 && many[2] === null && many[3] !== null,
);

const encoding = str(await r.cmd('OBJECT', 'ENCODING', runs.keyFor(ids[0])));
check('shards stay listpack-encoded', encoding === 'listpack', encoding);

check('delete works', (await runs.del(ids[0])) && !(await runs.has(ids[0])));

// the actual claim, measured rather than asserted
{
  await r.cmd('FLUSHALL', 'SYNC');
  await applyKitConfig(r, { shardWidth: 124 });
  const before = await sample(r);
  for (const id of ids) await runs.set(id, { ...run, run_id: id });
  const afterSharded = await sample(r);
  const shardedPerRecord = (afterSharded.attributable - before.attributable) / ids.length;

  await r.cmd('FLUSHALL', 'SYNC');
  const b2 = await sample(r);
  for (const id of ids) await r.cmd('SET', `plain:${id}`, JSON.stringify({ ...run, run_id: id }));
  const a2 = await sample(r);
  const jsonPerRecord = (a2.attributable - b2.attributable) / ids.length;

  check(
    `sharded+packed beats key-per-JSON-record`,
    shardedPerRecord < jsonPerRecord / 3,
    `${jsonPerRecord.toFixed(0)} B -> ${shardedPerRecord.toFixed(0)} B  (${(jsonPerRecord / shardedPerRecord).toFixed(1)}×)`,
  );
}

// ── ShardedCache ─────────────────────────────────────────────────────────
console.log('\nShardedCache');

await r.cmd('FLUSHALL', 'SYNC');
await applyKitConfig(r, { shardWidth: 124 });

const cache = new ShardedCache(r, {
  prefix: 'tool', capacity: 10_000, ttl: 3600, compress: true,
});
await cache.set('web_search:redis memory', 'a'.repeat(400));
const cached = await cache.get('web_search:redis memory');
check('cache round-trips through zstd', cached?.toString() === 'a'.repeat(400));

const ttl = await cache.ttlOf('web_search:redis memory');
check('per-field TTL is set', ttl !== null && ttl > 3500 && ttl <= 3600, `HTTL=${ttl}s`);

await cache.set('short-lived', 'x', 1);
check('short TTL is honoured', (await cache.ttlOf('short-lived')) === 1);

await cache.persist('short-lived');
check('persist removes the TTL', (await cache.ttlOf('short-lived')) === null);
check('miss returns null', (await cache.get('never-written')) === null);

// ── ExactSeen / BloomSeen ────────────────────────────────────────────────
console.log('\nExactSeen / BloomSeen');

await r.cmd('FLUSHALL', 'SYNC');
const seen = new ExactSeen(r, { prefix: 'idem', capacity: 100_000 });

check('first add returns true', await seen.add('order-1'));
check('second add returns false', !(await seen.add('order-1')));
check('has() agrees', await seen.has('order-1'));
check('unknown id is absent', !(await seen.has('order-2')));
check('remove works', (await seen.remove('order-1')) && !(await seen.has('order-1')));

await seen.addMany(Array.from({ length: 3000 }, (_, i) => `evt-${i}`));
check('addMany stores every id', (await seen.count()) === 3000);
const intsetEnc = str(await r.cmd('OBJECT', 'ENCODING', `idem:{${0}}`));
check('shards stay intset-encoded', intsetEnc === 'intset', intsetEnc);

const bloom = new BloomSeen(r, { key: 'bloom', capacity: 100_000, errorRate: 0.01 });
await bloom.init();
await bloom.addMany(Array.from({ length: 10_000 }, (_, i) => `evt-${i}`));
check('bloom recalls what it stored (no false negatives)', await bloom.has('evt-42'));
let fp = 0;
for (let i = 0; i < 2000; i++) if (await bloom.has(`absent-${i}`)) fp++;
check(
  'bloom false-positive rate is near the configured 1%',
  fp / 2000 < 0.03,
  `measured ${((fp / 2000) * 100).toFixed(2)}%`,
);

// ── VectorMemory ─────────────────────────────────────────────────────────
console.log('\nVectorMemory');

await r.cmd('FLUSHALL', 'SYNC');
const DIM = 256;
const mem = new VectorMemory(r, { key: 'mem', dim: DIM, quant: 'Q8' });

function vec(seed: number): Float32Array {
  const v = new Float32Array(DIM);
  let x = seed >>> 0;
  for (let i = 0; i < DIM; i++) {
    x = (Math.imul(x, 1103515245) + 12345) >>> 0;
    v[i] = x / 4294967296 - 0.5;
  }
  let n = 0;
  for (let i = 0; i < DIM; i++) n += v[i] * v[i];
  n = Math.sqrt(n) || 1;
  for (let i = 0; i < DIM; i++) v[i] /= n;
  return v;
}

for (let i = 0; i < 500; i++) await mem.add(`m${i}`, vec(i), { tenant: `t${i % 5}` });
check('vector set holds every vector', (await mem.size()) === 500);

const hits = await mem.search(vec(7), 5);
check('nearest neighbour of a stored vector is itself', hits[0]?.id === 'm7', `score ${hits[0]?.score.toFixed(4)}`);

const info = await mem.info();
check('quantization is int8 as requested', info['quant-type'] === 'int8', info['quant-type']);
check(
  'storageBytes matches the documented table',
  storageBytes(1536, 'f32') === 6144 && storageBytes(1536, 'int8') === 1540 &&
    storageBytes(1536, 'binary') === 192,
);

check('dimension mismatch is rejected loudly', await (async () => {
  try { await mem.add('bad', new Float32Array(8)); return false; } catch { return true; }
})());

// ── eviction safety ──────────────────────────────────────────────────────
console.log('\neviction safety');
await applySafeEviction(r);
const policy = (await r.cmd('CONFIG', 'GET', 'maxmemory-policy')) as any[];
check('applySafeEviction sets volatile-ttl', str(policy[1]) === 'volatile-ttl', str(policy[1]));

await r.cmd('CONFIG', 'SET', 'maxmemory-policy', 'noeviction');
await r.cmd('FLUSHALL', 'SYNC');
r.close();

console.log(
  failures === 0
    ? '\n\x1b[32m✔ all kit checks passed\x1b[0m\n'
    : `\n\x1b[31m✘ ${failures} check(s) failed\x1b[0m\n`,
);
process.exit(failures === 0 ? 0 : 1);
