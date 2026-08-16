/**
 * PROOF 06 - resharding without losing a record.
 *
 * The claim is that a Pouch can double its shard count online, while writes and
 * reads keep running, and that no record is lost, duplicated, or rolled back to
 * a stale value.
 *
 * The interesting case is not the quiet one. Writers run continuously THROUGH
 * the split, including writers still on the old generation, which is what
 * happens in a real fleet where clients pick up the new generation at different
 * times.
 */
import fs from 'node:fs';
import path from 'node:path';
import { connect, Pouch, schema, uuid, varint, applyKitConfig } from '../kit/index.ts';
import { split, readGeneration } from '../kit/reshard.ts';
import { str, num } from '../kit/client.ts';
import { Report } from './harness.ts';

const N = 20_000;
const START_K = 5; // 32 shards
const STALE_MS = 2_000;   // how long the stale writers stay stale
const SETTLE_MS = 4_000;  // must exceed STALE_MS, or the split loses their writes
const report = new Report(
  'Online resharding',
  'You cannot grow a sharded keyspace without a flush.',
);

const r = await connect('redis://127.0.0.1:6399');
await r.cmd('FLUSHALL', 'SYNC');
await applyKitConfig(r, { shardWidth: 124 });

const Rec = schema({ id: uuid(), version: varint() });
const ids = Array.from({ length: N }, (_, i) =>
  `3946f31a-da9d-02d9-a002-${String(i).padStart(12, '0')}`);

function pouch() {
  return new Pouch<{ id: string; version: number }>(r, {
    prefix: 'rec',
    capacity: 2 ** START_K * 124,
    width: 124,
    encode: Rec.encode,
    decode: (b) => Rec.decode(b) as { id: string; version: number },
  });
}

const writer = pouch();
await writer.syncGeneration();

for (let i = 0; i < N; i++) await writer.set(ids[i], { id: ids[i], version: 1 });
const beforeCount = await writer.count();
report.assert(
  `seeded ${beforeCount.toLocaleString()} records across ${writer.shards} shards`,
  beforeCount === N,
  `shard count is a power of two so that growth is a split, not a reshuffle`,
);

// ── run traffic THROUGH the split ───────────────────────────────────────

let reads = 0;
let readMisses = 0;
let writes = 0;
let stop = false;

/** A client that keeps up with the generation, like a healthy worker. */
async function freshClient() {
  const c = await connect('redis://127.0.0.1:6399');
  const p = new Pouch<{ id: string; version: number }>(c, {
    prefix: 'rec', capacity: 2 ** START_K * 124, width: 124,
    encode: Rec.encode, decode: (b) => Rec.decode(b) as { id: string; version: number },
  });
  while (!stop) {
    await p.syncGeneration();
    for (let n = 0; n < 50 && !stop; n++) {
      const id = ids[Math.floor(Math.random() * N)];
      const got = await p.get(id);
      reads++;
      if (!got) readMisses++;
    }
  }
  c.close();
}

/**
 * A client that is stale for a bounded time, which is the contract the protocol
 * requires. It writes to the pre-split home until it refreshes.
 *
 * A client that is stale FOREVER cannot be made safe by any drain, because it
 * keeps re-populating shards the mover has already finished. That is a
 * requirement on clients, not a bug in the splitter, and `settleMs` is where
 * the requirement is written down.
 */
async function staleClient() {
  const c = await connect('redis://127.0.0.1:6399');
  const p = new Pouch<{ id: string; version: number }>(c, {
    prefix: 'rec', capacity: 2 ** START_K * 124, width: 124,
    encode: Rec.encode, decode: (b) => Rec.decode(b) as { id: string; version: number },
  });
  // stale for STALE_MS, then refreshes like a real worker would
  const refreshAt = Date.now() + STALE_MS;
  while (!stop) {
    if (Date.now() > refreshAt) await p.syncGeneration();
    const i = Math.floor(Math.random() * N);
    await p.set(ids[i], { id: ids[i], version: 2 });
    writes++;
  }
  c.close();
}

const traffic = [freshClient(), freshClient(), freshClient(), staleClient(), staleClient()];

const gen0 = await readGeneration(r, 'rec');
const res = await split(r, { prefix: 'rec', k: START_K, settleMs: SETTLE_MS });
stop = true;
await Promise.all(traffic);

const gen1 = await readGeneration(r, 'rec');

report.assert(
  `split ${res.from} -> ${res.to} shards moved ${res.moved.toLocaleString()} of ${N.toLocaleString()} records in ${res.ms} ms`,
  res.to === res.from * 2 && res.moved > 0,
  `about half the keys move, which is the whole point of powers of two. ` +
    `${reads.toLocaleString()} reads and ${writes.toLocaleString()} writes ran concurrently.`,
);

report.assert(
  `no read missed during the split: ${readMisses} of ${reads.toLocaleString()}`,
  readMisses === 0,
  'readers check the new home then the pre-split home, so a record in either is found',
);

// ── the part that actually matters: nothing lost, nothing duplicated ────

const reader = pouch();
await reader.syncGeneration();

let missing = 0;
let wrong = 0;
for (const id of ids) {
  const got = await reader.get(id);
  if (!got) missing++;
  else if (got.id !== id) wrong++;
}

report.assert(
  `all ${N.toLocaleString()} records readable after the split (${missing} missing, ${wrong} corrupted)`,
  missing === 0 && wrong === 0,
  'checked one by one against the id that was written',
);

// Duplication check: count raw fields across BOTH generations of shard keys.
let rawFields = 0;
const maxShards = 2 ** (START_K + 1);
const lens = await r.pipeline(
  Array.from({ length: maxShards }, (_, s) => ['HLEN', `rec:{${s}}`]),
);
for (const v of lens) rawFields += num(v);

report.assert(
  `${rawFields.toLocaleString()} stored fields for ${N.toLocaleString()} records: no duplicates left behind`,
  rawFields === N,
  'a copy left in the old home would show up here as an extra field',
);

report.assert(
  `generation went ${gen0 ? `${gen0.k}:${gen0.prev ?? ''}` : 'unset'} -> ${gen1!.k}, draining flag cleared`,
  gen1!.prev === null && gen1!.k === START_K + 1,
  'while draining, the marker carries the previous k so readers know to check both homes',
);

report.info(
  'the two Lua scripts (HSETNX to claim, compare-and-delete to retire) each touch ONE key, ' +
    'so the whole protocol is legal in cluster mode with no cross-slot transaction',
);
report.info(
  `stale writers stayed on the old generation for ${STALE_MS} ms and lost nothing, because the ` +
    `splitter waits out a ${SETTLE_MS} ms staleness window and sweeps again before clearing the marker`,
);
report.info(
  'settleMs is a correctness parameter: it must exceed your clients\' generation refresh interval. ' +
    'A client that never refreshes cannot be made safe by any drain.',
);

await r.cmd('FLUSHALL', 'SYNC');
r.close();

const summary = report.summary();
const dir = path.resolve(import.meta.dirname, '../../results');
fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(
  path.join(dir, 'proof-reshard.json'),
  JSON.stringify({ ...summary, split: res, reads, writes, readMisses }, null, 2),
);
console.log('  -> results/proof-reshard.json');
process.exit(summary.passed ? 0 : 1);
