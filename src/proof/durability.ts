/**
 * PROOF 01 - "Redis loses acknowledged writes when it crashes."
 *
 * This is the first objection and the fairest one, because with Redis's
 * *default* configuration it is true. Redis Open Source ships with
 * `appendonly no`: durability comes from periodic RDB snapshots, so a crash
 * discards everything written since the last one. Anybody who has been burned
 * by that is right to distrust it.
 *
 * It is also fixable, and this file proves it - while being exact about the
 * one thing an AOF cannot fix.
 *
 * ── The distinction that matters ─────────────────────────────────────────
 *
 * There are two different crashes and they have different answers:
 *
 *   PROCESS LOSS   Redis segfaults, the OOM killer takes it, the container is
 *                  killed, the pod is evicted. The kernel survives. Anything
 *                  Redis has `write(2)`-ed is already in the page cache and the
 *                  OS will still flush it. `appendfsync everysec` loses NOTHING
 *                  here - the everysec window is an *fsync* window, not a
 *                  write() window.
 *
 *   MACHINE LOSS   Power failure, kernel panic, an EC2 instance vanishing. The
 *                  page cache goes with it. Now the fsync window is real, and
 *                  `everysec` can cost you writes.
 *
 *                  How many? The docs say "1 second". The source says otherwise:
 *                  when an fsync is already in flight, `flushAppendOnlyFile()`
 *                  postpones the *write()* too, for up to 2000 ms
 *                  (`src/aof.c`, `aof_flush_postponed_start`). So the honest
 *                  bound is ~2 s, and longer under disk stall. Watch the
 *                  `aof_delayed_fsync` counter in INFO if you care.
 *
 * `docker kill -s KILL` reproduces PROCESS LOSS exactly and faithfully. It
 * cannot reproduce MACHINE LOSS - nothing running inside a healthy kernel can.
 * So this proof does two things:
 *
 *   1. Empirically tests process loss across four durability configurations,
 *      including the default, and counts survivors of acknowledged writes.
 *
 *   2. *Measures* the machine-loss exposure window instead of asserting it, by
 *      timing `WAITAOF` - which returns only once the write is fsynced to disk.
 *      The latency of WAITAOF after a write is, by definition, how long that
 *      write was exposed to machine loss. That turns a hand-wave into a number.
 *
 * Finally it measures recovery: how long a restart takes to reload the dataset,
 * which is objection #7 and the thing that decides your real RTO.
 */
import fs from 'node:fs';
import path from 'node:path';
import type Redis from 'ioredis';
import {
  startRedis, startExisting, killRedis, rmContainer, waitReady, sh, Report, stats,
} from './harness.ts';

/**
 * Destroy the data volume so each scenario starts from a genuinely empty disk.
 *
 * Order matters: `docker volume rm` fails while any container still references
 * the volume, and it fails *quietly* here. Removing the container first is the
 * difference between a clean run and silently reloading the previous
 * scenario's dataset - which is exactly the bug that once made this file report
 * that 4,000 writes survived a crash with persistence disabled.
 */
function resetVolume(): void {
  rmContainer(NAME);
  sh('docker', ['volume', 'rm', '-f', VOLUME], true);
}

const PORT = 6398;
const NAME = 'redops-durable';
const VOLUME = 'redops-durable-data';
const WRITES = 20_000;

interface Config {
  label: string;
  args: string[];
  /** What the reader should expect before we run it. */
  expectation: string;
  expectSurvivalPct: number;
}

const CONFIGS: Config[] = [
  {
    label: 'default (appendonly no, no save)',
    args: ['--appendonly', 'no', '--save', ''],
    expectation: 'total loss - nothing is persisted at all',
    expectSurvivalPct: 0,
  },
  {
    label: 'RDB snapshots only (save 60 1000)',
    args: ['--appendonly', 'no', '--save', '60 1000'],
    expectation: 'loses everything since the last snapshot',
    expectSurvivalPct: 0,
  },
  {
    label: 'AOF, appendfsync everysec',
    args: ['--appendonly', 'yes', '--appendfsync', 'everysec'],
    expectation: 'zero loss on process crash; ~2s exposure to machine loss',
    expectSurvivalPct: 100,
  },
  {
    label: 'AOF, appendfsync always',
    args: ['--appendonly', 'yes', '--appendfsync', 'always'],
    expectation: 'zero loss on process crash AND on machine loss',
    expectSurvivalPct: 100,
  },
];

const report = new Report(
  'Durability under crash',
  'Redis loses acknowledged writes when it crashes.',
);

const results: Record<string, any> = {};

for (const cfg of CONFIGS) {
  resetVolume();
  startRedis({ name: NAME, port: PORT, volume: VOLUME, args: cfg.args });
  let r = await waitReady(PORT);

  // Write and *wait for the reply* before counting a write as acknowledged.
  // This is the whole point: we only hold Redis to writes it told us it took.
  let acked = 0;
  for (let i = 0; i < WRITES; i += 500) {
    const p = r.pipeline();
    for (let j = i; j < Math.min(i + 500, WRITES); j++) p.set(`d:${j}`, `v${j}`);
    const res = await p.exec();
    for (const [err] of res!) if (!err) acked++;
  }
  const before = await r.dbsize();
  r.disconnect();

  // SIGKILL: no SHUTDOWN, no atexit, no final fsync.
  killRedis(NAME);
  startExisting(NAME);
  r = await waitReady(PORT);

  const t0 = performance.now();
  const survived = await r.dbsize();
  const readMs = performance.now() - t0;

  const survivalPct = (survived / acked) * 100;
  const ok = Math.abs(survivalPct - cfg.expectSurvivalPct) < 0.5;

  report.assert(
    `${cfg.label} → ${survived.toLocaleString()}/${acked.toLocaleString()} acknowledged writes survived SIGKILL (${survivalPct.toFixed(2)}%)`,
    ok,
    `expected: ${cfg.expectation}`,
  );
  results[cfg.label] = { acked, before, survived, survivalPct: +survivalPct.toFixed(2), readMs };

  r.disconnect();
  rmContainer(NAME);
}

// ─── Measure the machine-loss window instead of asserting it ──────────────

report.info('measuring the fsync exposure window with WAITAOF (the machine-loss risk)');

async function measureWaitAof(args: string[], samples: number): Promise<number[]> {
  resetVolume();
  startRedis({ name: NAME, port: PORT, volume: VOLUME, args });
  const r = await waitReady(PORT);
  const lat: number[] = [];
  for (let i = 0; i < samples; i++) {
    await r.set(`w:${i}`, 'x');
    const t = performance.now();
    // WAITAOF <numlocal> <numreplicas> <timeout-ms>: returns once fsynced locally.
    await r.call('WAITAOF', '1', '0', '5000');
    lat.push(performance.now() - t);
  }
  r.disconnect();
  rmContainer(NAME);
  return lat;
}

const everysecWait = await measureWaitAof(['--appendonly', 'yes', '--appendfsync', 'everysec'], 300);
const alwaysWait = await measureWaitAof(['--appendonly', 'yes', '--appendfsync', 'always'], 300);

const esw = stats(everysecWait);
const asw = stats(alwaysWait);

report.assert(
  `everysec: a write is exposed to machine loss for p50 ${esw.p50} ms / p99 ${esw.p99} ms / max ${esw.max} ms`,
  esw.max < 2100,
  'WAITAOF blocks until the write is fsynced but does NOT trigger an fsync, so this latency is exactly the wait for ' +
    'the next scheduled one - i.e. the exposure window, measured rather than assumed. Note the bound is ~2 s, not the ' +
    '1 s the docs state: aof.c postpones the write() by up to 2000 ms when an fsync is already in flight.',
);
report.assert(
  `always: exposure collapses to p50 ${asw.p50} ms / p99 ${asw.p99} ms / max ${asw.max} ms`,
  asw.p99 < esw.p99,
  'every write is fsynced before the reply, so there is effectively no window',
);

// ─── What durability costs in throughput ─────────────────────────────────

report.info('measuring what each durability level costs in write throughput');

async function throughputOnce(args: string[], n: number, waitaof: boolean): Promise<number> {
  resetVolume();
  startRedis({ name: NAME, port: PORT, volume: VOLUME, args });
  const r = await waitReady(PORT);
  const t0 = performance.now();
  for (let i = 0; i < n; i += 200) {
    const p = r.pipeline();
    for (let j = i; j < Math.min(i + 200, n); j++) p.set(`t:${j}`, 'payload-of-modest-size-0123456789');
    if (waitaof) p.call('WAITAOF', '1', '0', '5000');
    await p.exec();
  }
  const ops = n / ((performance.now() - t0) / 1000);
  r.disconnect();
  rmContainer(NAME);
  return Math.round(ops);
}

/**
 * Best-of-N.
 *
 * The first measurement of the session is systematically slow: a cold container,
 * a cold page cache and a cold docker overlay all land on run #1. Taking the
 * best of several runs removes that bias - otherwise whichever config happens to
 * be measured first looks worst, which is how an earlier version of this file
 * "proved" that enabling the AOF made Redis faster.
 */
async function throughput(args: string[], n: number, waitaof = false, reps = 3): Promise<number> {
  let best = 0;
  for (let i = 0; i < reps; i++) best = Math.max(best, await throughputOnce(args, n, waitaof));
  return best;
}

/**
 * Enough operations that the measurement is dominated by the work rather than
 * by connection setup. At ~400k ops/s a 60,000-op run finishes in 0.15 s, which
 * is how an earlier version of this file "measured" the AOF making Redis
 * faster. 600,000 ops takes ~2 s and the ordering becomes stable.
 */
const N = 600_000;
await throughputOnce(['--appendonly', 'no', '--save', ''], 5_000, false); // discarded warmup
const tpNone = await throughput(['--appendonly', 'no', '--save', ''], N);
const tpEverysec = await throughput(['--appendonly', 'yes', '--appendfsync', 'everysec'], N);
const tpAlways = await throughput(['--appendonly', 'yes', '--appendfsync', 'always'], N);
// WAITAOF against `always`: under `everysec` each WAITAOF waits for the next
// scheduled fsync (~1 s), so batching it there collapses to ~1 batch/second.
const tpWaitAof = await throughput(['--appendonly', 'yes', '--appendfsync', 'always'], N, true, 2);
const tpWaitAofEverysec = await throughputOnce(
  ['--appendonly', 'yes', '--appendfsync', 'everysec'], 4_000, true,
);

const pctSlower = (x: number) => `${(((tpNone - x) / tpNone) * 100).toFixed(1)}%`;
const delta = ((tpNone - tpEverysec) / tpNone) * 100;
// Within ±5% these two are indistinguishable on this hardware, and saying
// "everysec is 3% faster" would be as wrong as saying it is 3% slower.
const everysecVerdict =
  Math.abs(delta) < 5
    ? `no measurable throughput cost (${tpNone.toLocaleString()} vs ${tpEverysec.toLocaleString()} ops/s - within run-to-run noise)`
    : delta > 0
      ? `costs ${delta.toFixed(1)}% throughput (${tpNone.toLocaleString()} → ${tpEverysec.toLocaleString()} ops/s)`
      : `measured ${(-delta).toFixed(1)}% FASTER than no persistence, which means the difference is below this rig's noise floor`;
report.assert(
  `everysec vs no persistence: ${everysecVerdict}`,
  tpEverysec > tpNone * 0.5,
  'expected: AOF writes are buffered and flushed once per event-loop iteration, and the fsync runs on a ' +
    'background thread - so everysec should barely touch the write path. That is what makes it the sane default.',
);
report.assert(
  `appendfsync always costs ${pctSlower(tpAlways)} (${tpAlways.toLocaleString()} ops/s) - far less than folklore suggests, because Redis group-commits`,
  tpAlways > tpNone * 0.25,
  'the AOF is fsynced once per event-loop iteration for a whole batch of clients, not once per command',
);
report.info(
  `always + per-batch WAITAOF: ${tpWaitAof.toLocaleString()} ops/s - the "confirm durability where it matters" pattern`,
);
report.info(
  `everysec + per-batch WAITAOF: ${tpWaitAofEverysec.toLocaleString()} ops/s - ` +
    'a trap. WAITAOF does not trigger an fsync, so under everysec each call waits for the next scheduled one. ' +
    'If you want per-write durability confirmation, you need appendfsync always.',
);

// ─── Recovery: how long is your RTO, really ──────────────────────────────

report.info('measuring restart/reload time - objection #7');

resetVolume();
startRedis({
  name: NAME, port: PORT, volume: VOLUME,
  args: ['--appendonly', 'yes', '--appendfsync', 'everysec'],
});
let rr: Redis = await waitReady(PORT);

const BIG = 500_000;
for (let i = 0; i < BIG; i += 1000) {
  const p = rr.pipeline();
  for (let j = i; j < Math.min(i + 1000, BIG); j++) {
    p.hset(`run:{${j % 4000}}`, String(j), 'packed-agent-run-record-of-about-110-bytes-0123456789012345678901234567890123456789012345');
  }
  await p.exec();
}
const memBefore = Number(/used_memory:(\d+)/.exec(await rr.info('memory'))![1]);
try {
  await rr.call('BGREWRITEAOF');
} catch (e: any) {
  // an automatic rewrite may already be in flight - that is fine, we just wait
  if (!/already in progress/i.test(String(e?.message))) throw e;
}
for (;;) {
  const inf = await rr.info('persistence');
  if (/aof_rewrite_in_progress:0/.test(inf)) break;
  await new Promise((s) => setTimeout(s, 100));
}
rr.disconnect();

killRedis(NAME);
const tRestart = performance.now();
startExisting(NAME);
rr = await waitReady(PORT, 120_000);
const restartMs = performance.now() - tRestart;
const keysAfter = await rr.dbsize();
const memAfter = Number(/used_memory:(\d+)/.exec(await rr.info('memory'))![1]);

report.assert(
  `recovery: ${BIG.toLocaleString()} records across ${keysAfter.toLocaleString()} keys reloaded from AOF in ${(restartMs / 1000).toFixed(2)} s`,
  keysAfter === 4000,
  `≈ ${Math.round(BIG / (restartMs / 1000)).toLocaleString()} records/s reload; RAM ${(memBefore / 1024 ** 2).toFixed(1)} MiB → ${(memAfter / 1024 ** 2).toFixed(1)} MiB`,
);
report.info(
  'reload is single-threaded and roughly linear in dataset size - budget RTO from this rate, ' +
    'and use a replica for failover if that RTO is too slow.',
);

rr.disconnect();
resetVolume();

const summary = report.summary();
const out = {
  ...summary,
  crashMatrix: results,
  fsyncWindowMs: { everysec: esw, always: asw },
  throughputOpsPerSec: { none: tpNone, everysec: tpEverysec, always: tpAlways, everysecWaitAof: tpWaitAof },
  recovery: { records: BIG, keys: keysAfter, restartMs: Math.round(restartMs), memBefore, memAfter },
};
const dir = path.resolve(import.meta.dirname, '../../results');
fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(path.join(dir, 'proof-durability.json'), JSON.stringify(out, null, 2));
console.log('  → results/proof-durability.json');
process.exit(summary.passed ? 0 : 1);
