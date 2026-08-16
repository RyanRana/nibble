/**
 * PROOF 02 — "Redis has no transactions, so you can't maintain invariants."
 *
 * The objection is half right, and the half that's right is worth stating
 * plainly before knocking down the half that isn't:
 *
 *   TRUE.  MULTI/EXEC is not a transaction in the database sense. It gives you
 *          batching and isolation, but NOT rollback: if a command inside the
 *          block fails at runtime, the commands around it still apply. This
 *          file demonstrates that failure live rather than taking anyone's word
 *          for it.
 *
 *   FALSE. "…therefore you cannot maintain invariants." Redis executes a Lua
 *          script as a single unit against a single-threaded core. No other
 *          command interleaves. That is strictly *stronger* isolation than the
 *          READ COMMITTED most people actually run Postgres at — it is
 *          effectively serializable, for the keys the script touches.
 *
 * So the correct statement is: Redis has no *interactive* transactions, and
 * instead gives you atomic server-side procedures. If you write your state
 * transitions as scripts rather than as read-modify-write round trips, your
 * invariants hold under arbitrary concurrency.
 *
 * This proof runs a real agent-run state machine under 64 concurrent workers
 * deliberately racing on the same runs, three ways:
 *
 *   A. read-modify-write   the natural client-side implementation
 *   B. WATCH/MULTI/EXEC    optimistic concurrency control
 *   C. Lua (EVALSHA)       atomic server-side transition
 *
 * and then checks four invariants that a corrupted store cannot satisfy.
 *
 * ── The layout synergy worth noticing ────────────────────────────────────
 *
 * The script mutates a *schema-packed binary record in place* — it patches the
 * status byte and the version counter with string.byte/string.char without ever
 * decoding the other ~105 bytes. So the RAM optimization from case 01 does not
 * cost you server-side logic; it enables it, because there is no round trip.
 *
 * And because every key for a shard shares one `{hash tag}`, the whole script
 * touches a single cluster slot — so this stays correct on Redis Cluster, where
 * cross-slot multi-key operations are rejected.
 */
import fs from 'node:fs';
import path from 'node:path';
import Redis from 'ioredis';
import { connect } from '../lib/redis.ts';
import { Report, stats } from './harness.ts';

const RUNS = 400;
const WORKERS = 64;
const ATTEMPTS_PER_WORKER = 300;
const SHARDS = 8;

/** Status codes stored as the first byte of the packed record. */
const QUEUED = 0, RUNNING = 1, AWAITING = 2, SUCCEEDED = 3, FAILED = 4, CANCELLED = 5;
const NAMES = ['queued', 'running', 'awaiting_tool', 'succeeded', 'failed', 'cancelled'];
const TERMINAL = new Set([SUCCEEDED, FAILED, CANCELLED]);

/** The only transitions the business logic permits. */
const ALLOWED: Record<number, number[]> = {
  [QUEUED]: [RUNNING, CANCELLED],
  [RUNNING]: [AWAITING, SUCCEEDED, FAILED, CANCELLED],
  [AWAITING]: [RUNNING, FAILED, CANCELLED],
  [SUCCEEDED]: [],
  [FAILED]: [],
  [CANCELLED]: [],
};

const shardOf = (i: number) => i % SHARDS;
const shardKey = (i: number) => `runs:{s${shardOf(i)}}`;
const idxKey = (i: number, st: number) => `idx:{s${shardOf(i)}}:${st}`;
const evtKey = (i: number) => `evt:{s${shardOf(i)}}`;

/** record = [status:1][version:4 BE][payload:107] — same shape the codec emits. */
function packed(status: number, version: number): Buffer {
  const b = Buffer.alloc(112);
  b[0] = status;
  b.writeUInt32BE(version, 1);
  b.fill(0x5a, 5);
  return b;
}

// ─────────────────────────── the Lua procedure ───────────────────────────
//
// KEYS[1] shard hash · KEYS[2] index set for the OLD status
// KEYS[3] index set for the NEW status · KEYS[4] event stream
// ARGV[1] run id · ARGV[2] expected status · ARGV[3] new status
//
// Returns 1 on success, 0 if the transition was not legal from the *current*
// status. The legality table lives in the script, so an out-of-date or buggy
// client physically cannot write an illegal state.
const TRANSITION_LUA = `
local rec = redis.call('HGET', KEYS[1], ARGV[1])
if not rec then return -1 end

local cur = string.byte(rec, 1)
local want = tonumber(ARGV[2])
local nxt  = tonumber(ARGV[3])

-- compare-and-set: someone else moved it first, so this attempt is stale
if cur ~= want then return 0 end

-- server-side legality table: the client cannot bypass this
local legal = {
  [0] = {[1]=true, [5]=true},
  [1] = {[2]=true, [3]=true, [4]=true, [5]=true},
  [2] = {[1]=true, [4]=true, [5]=true},
  [3] = {}, [4] = {}, [5] = {}
}
if not legal[cur] or not legal[cur][nxt] then return 0 end

-- bump the 4-byte big-endian version counter in place
local v = string.byte(rec,2)*16777216 + string.byte(rec,3)*65536
        + string.byte(rec,4)*256 + string.byte(rec,5) + 1

local patched = string.char(nxt)
  .. string.char(math.floor(v/16777216) % 256)
  .. string.char(math.floor(v/65536) % 256)
  .. string.char(math.floor(v/256) % 256)
  .. string.char(v % 256)
  .. string.sub(rec, 6)               -- the other 107 bytes are never decoded

redis.call('HSET', KEYS[1], ARGV[1], patched)
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[1])
redis.call('XADD', KEYS[4], '*', 'r', ARGV[1], 'f', ARGV[2], 't', ARGV[3])
return 1
`;

const report = new Report(
  'Atomicity, isolation and server-side validation',
  'Redis has no transactions, so you cannot maintain invariants.',
);

const main = connect();

// ── First: demonstrate the real MULTI/EXEC semantics, honestly ────────────

await main.flushall('SYNC');
await main.set('counter', 'not-a-number');
const multi = main.multi();
multi.set('before', '1');
multi.incr('counter'); // fails at runtime — wrong type
multi.set('after', '1');
const execRes = await multi.exec();
const failedCmds = execRes!.filter(([e]) => e).length;
const beforeSet = await main.get('before');
const afterSet = await main.get('after');

report.assert(
  `MULTI/EXEC does NOT roll back: 1 command failed, yet 'before'=${beforeSet} and 'after'=${afterSet} both applied`,
  failedCmds === 1 && beforeSet === '1' && afterSet === '1',
  'This is the objection, and it is correct. MULTI is batching + isolation, not a transaction. Do not build invariants on it.',
);

// ── Setup: seed the runs ─────────────────────────────────────────────────

async function seed(r: Redis) {
  await r.flushall('SYNC');
  const p = r.pipeline();
  for (let i = 0; i < RUNS; i++) {
    p.hset(shardKey(i), String(i), packed(QUEUED, 0));
    p.sadd(idxKey(i, QUEUED), String(i));
  }
  await p.exec();
}

interface Outcome {
  applied: number;
  rejected: number;
  errors: number;
  retries: number;
  latencies: number[];
}

/**
 * Deterministic attempt plan, identical for all three strategies so they face
 * exactly the same race.
 *
 * Two properties matter for this to be a real test:
 *
 *  - Contention. Run ids are drawn from a skewed distribution so workers pile
 *    onto the same few runs, the way a real fan-out/fan-in agent graph does.
 *  - Longevity. Terminal transitions are rare (2%). An earlier version picked a
 *    terminal state 1 attempt in 3, so every run died within the first few
 *    hundred attempts and the remaining 19,000 were no-ops against frozen rows
 *    — which made the contention window far too short to be convincing.
 */
function plan(worker: number): { run: number; to: number }[] {
  const out: { run: number; to: number }[] = [];
  let x = (worker * 2654435761) >>> 0;
  const next = () => ((x = (x * 1103515245 + 12345) >>> 0) / 4294967296);
  for (let i = 0; i < ATTEMPTS_PER_WORKER; i++) {
    const run = Math.floor(Math.pow(next(), 2) * RUNS); // skewed toward low ids
    const roll = next();
    const to =
      roll < 0.49 ? RUNNING
      : roll < 0.98 ? AWAITING
      : roll < 0.99 ? SUCCEEDED
      : FAILED;
    out.push({ run, to });
  }
  return out;
}

// ── A · read-modify-write (the natural client-side implementation) ────────

async function strategyRMW(): Promise<Outcome> {
  const conns = Array.from({ length: WORKERS }, () => connect());
  await seed(conns[0]);
  const o: Outcome = { applied: 0, rejected: 0, errors: 0, retries: 0, latencies: [] };

  await Promise.all(
    conns.map(async (c, w) => {
      for (const { run, to } of plan(w)) {
        const t0 = performance.now();
        try {
          const rec = await c.hgetBuffer(shardKey(run), String(run));
          if (!rec) { o.errors++; continue; }
          const cur = rec[0];
          if (!ALLOWED[cur]?.includes(to)) { o.rejected++; continue; }
          const ver = rec.readUInt32BE(1) + 1;
          const patched = Buffer.from(rec);
          patched[0] = to;
          patched.writeUInt32BE(ver, 1);
          await c.hset(shardKey(run), String(run), patched);
          await c.srem(idxKey(run, cur), String(run));
          await c.sadd(idxKey(run, to), String(run));
          await c.xadd(evtKey(run), '*', 'r', String(run), 'f', String(cur), 't', String(to));
          o.applied++;
        } catch { o.errors++; }
        o.latencies.push(performance.now() - t0);
      }
    }),
  );
  for (const c of conns) c.disconnect();
  return o;
}

// ── B · WATCH / MULTI / EXEC (optimistic concurrency control) ─────────────

async function strategyWatch(): Promise<Outcome> {
  const conns = Array.from({ length: WORKERS }, () => connect());
  await seed(conns[0]);
  const o: Outcome = { applied: 0, rejected: 0, errors: 0, retries: 0, latencies: [] };

  await Promise.all(
    conns.map(async (c, w) => {
      for (const { run, to } of plan(w)) {
        const t0 = performance.now();
        let done = false;
        for (let attempt = 0; attempt < 20 && !done; attempt++) {
          try {
            await c.watch(shardKey(run));
            const rec = await c.hgetBuffer(shardKey(run), String(run));
            if (!rec) { await c.unwatch(); o.errors++; break; }
            const cur = rec[0];
            if (!ALLOWED[cur]?.includes(to)) { await c.unwatch(); o.rejected++; break; }
            const patched = Buffer.from(rec);
            patched[0] = to;
            patched.writeUInt32BE(rec.readUInt32BE(1) + 1, 1);
            const res = await c.multi()
              .hset(shardKey(run), String(run), patched)
              .srem(idxKey(run, cur), String(run))
              .sadd(idxKey(run, to), String(run))
              .xadd(evtKey(run), '*', 'r', String(run), 'f', String(cur), 't', String(to))
              .exec();
            if (res === null) { o.retries++; continue; } // WATCH tripped — retry
            o.applied++;
            done = true;
          } catch { o.errors++; break; }
        }
        o.latencies.push(performance.now() - t0);
      }
    }),
  );
  for (const c of conns) c.disconnect();
  return o;
}

// ── C · Lua (atomic server-side procedure) ───────────────────────────────

async function strategyLua(): Promise<Outcome> {
  const conns = Array.from({ length: WORKERS }, () => connect());
  await seed(conns[0]);
  const sha = (await conns[0].script('LOAD', TRANSITION_LUA)) as string;
  const o: Outcome = { applied: 0, rejected: 0, errors: 0, retries: 0, latencies: [] };

  await Promise.all(
    conns.map(async (c, w) => {
      for (const { run, to } of plan(w)) {
        const t0 = performance.now();
        try {
          // We must pass the status we believe it is; the script CASes on it.
          const rec = await c.hgetBuffer(shardKey(run), String(run));
          const cur = rec ? rec[0] : QUEUED;
          const res = (await c.evalsha(
            sha, 4,
            shardKey(run), idxKey(run, cur), idxKey(run, to), evtKey(run),
            String(run), String(cur), String(to),
          )) as number;
          if (res === 1) o.applied++;
          else o.rejected++;
        } catch { o.errors++; }
        o.latencies.push(performance.now() - t0);
      }
    }),
  );
  for (const c of conns) c.disconnect();
  return o;
}

// ── Invariant checks ─────────────────────────────────────────────────────

interface Violations {
  illegalStates: number;
  indexMismatch: number;
  duplicateIndex: number;
  versionMismatch: number;
  eventCount: number;
}

async function checkInvariants(r: Redis, applied: number): Promise<Violations> {
  const v: Violations = {
    illegalStates: 0, indexMismatch: 0, duplicateIndex: 0,
    versionMismatch: 0, eventCount: 0,
  };
  let versionSum = 0;

  for (let i = 0; i < RUNS; i++) {
    const rec = await r.hgetBuffer(shardKey(i), String(i));
    if (!rec) { v.indexMismatch++; continue; }
    const st = rec[0];
    versionSum += rec.readUInt32BE(1);

    // INVARIANT 1: the status byte must be a status that exists
    if (st < 0 || st > 5) v.illegalStates++;

    // INVARIANT 2: the run appears in exactly one status index, the right one
    let memberships = 0;
    for (let s = 0; s <= 5; s++) {
      if (await r.sismember(idxKey(i, s), String(i))) {
        memberships++;
        if (s !== st) v.indexMismatch++;
      }
    }
    if (memberships !== 1) v.duplicateIndex++;
  }

  // INVARIANT 3: every applied transition bumped the version exactly once
  if (versionSum !== applied) v.versionMismatch = Math.abs(versionSum - applied);

  // INVARIANT 4: the event log records exactly the applied transitions
  let events = 0;
  for (let s = 0; s < SHARDS; s++) events += await r.xlen(`evt:{s${s}}`);
  v.eventCount = Math.abs(events - applied);

  return v;
}

// ── Run all three ────────────────────────────────────────────────────────

const outcomes: Record<string, { o: Outcome; v: Violations }> = {};

for (const [label, fn] of [
  ['A · read-modify-write', strategyRMW],
  ['B · WATCH/MULTI/EXEC', strategyWatch],
  ['C · Lua atomic procedure', strategyLua],
] as const) {
  const o = await fn();
  const v = await checkInvariants(main, o.applied);
  outcomes[label] = { o, v };
  const total = Object.values(v).reduce((a, b) => a + b, 0);
  const s = stats(o.latencies);
  console.log(
    `  ${label.padEnd(28)} applied=${String(o.applied).padStart(6)} rejected=${String(o.rejected).padStart(6)}` +
      ` retries=${String(o.retries).padStart(5)} violations=${String(total).padStart(5)}  p99=${s.p99}ms`,
  );
}

const rmw = outcomes['A · read-modify-write'];
const watch = outcomes['B · WATCH/MULTI/EXEC'];
const lua = outcomes['C · Lua atomic procedure'];

const vsum = (v: Violations) => Object.values(v).reduce((a, b) => a + b, 0);

report.assert(
  `read-modify-write corrupts the store under contention: ${vsum(rmw.v)} invariant violations ` +
    `(${rmw.v.indexMismatch} index mismatches, ${rmw.v.duplicateIndex} runs in wrong number of indexes, ` +
    `${rmw.v.versionMismatch} lost version bumps)`,
  vsum(rmw.v) > 0,
  'This is what people actually ship, and it is the real source of "Redis lost my data" stories.',
);

report.assert(
  `WATCH/MULTI/EXEC holds the invariants: ${vsum(watch.v)} violations, at the cost of ${watch.o.retries} retries`,
  vsum(watch.v) === 0,
  `optimistic concurrency works, but ${((watch.o.retries / (watch.o.applied + watch.o.retries)) * 100).toFixed(1)}% of attempts had to be redone — that is wasted work that grows with contention`,
);

report.assert(
  `Lua holds the invariants with zero retries: ${vsum(lua.v)} violations, ${lua.o.retries} retries`,
  vsum(lua.v) === 0 && lua.o.retries === 0,
  'the script is the unit of atomicity; no other command interleaves, so there is nothing to retry',
);

report.assert(
  `Lua p99 latency ${stats(lua.o.latencies).p99} ms vs WATCH ${stats(watch.o.latencies).p99} ms`,
  stats(lua.o.latencies).p99 <= stats(watch.o.latencies).p99,
  'fewer round trips and no retry loop',
);

// ── Server-side validation cannot be bypassed by a buggy client ───────────

await seed(main);
const sha = (await main.script('LOAD', TRANSITION_LUA)) as string;
// A terminal run must never leave its terminal state, no matter what a client asks for.
await main.evalsha(sha, 4, shardKey(0), idxKey(0, QUEUED), idxKey(0, RUNNING), evtKey(0), '0', String(QUEUED), String(RUNNING));
await main.evalsha(sha, 4, shardKey(0), idxKey(0, RUNNING), idxKey(0, SUCCEEDED), evtKey(0), '0', String(RUNNING), String(SUCCEEDED));
const illegal = (await main.evalsha(
  sha, 4, shardKey(0), idxKey(0, SUCCEEDED), idxKey(0, RUNNING), evtKey(0),
  '0', String(SUCCEEDED), String(RUNNING),
)) as number;
const finalRec = await main.hgetBuffer(shardKey(0), '0');

report.assert(
  `a client asking for succeeded→running is refused server-side (returned ${illegal}); run stayed '${NAMES[finalRec![0]]}'`,
  illegal === 0 && finalRec![0] === SUCCEEDED,
  'the legality table lives in the script, so no client version, no retry loop and no rogue service can write an illegal state',
);

// ── Redis Functions: the same logic, but versioned and persistent ─────────

const LIB = `#!lua name=redops
redis.register_function('agent_transition', function(keys, args)
${TRANSITION_LUA.split('\n').map((l) => '  ' + l.replace(/\bKEYS\b/g, 'keys').replace(/\bARGV\b/g, 'args')).join('\n')}
end)`;
try {
  await main.call('FUNCTION', 'LOAD', 'REPLACE', LIB);
  await seed(main);
  const fres = (await main.call(
    'FCALL', 'agent_transition', '4',
    shardKey(1), idxKey(1, QUEUED), idxKey(1, RUNNING), evtKey(1),
    '1', String(QUEUED), String(RUNNING),
  )) as number;
  const flist = (await main.call('FUNCTION', 'LIST')) as any[];
  report.assert(
    `the same procedure loaded as a Redis Function library and executed via FCALL (returned ${fres})`,
    fres === 1 && flist.length > 0,
    'Functions persist in the RDB/AOF and replicate to replicas — server-side logic becomes part of the database, not of whichever client happens to connect',
  );
} catch (e: any) {
  report.assert('Redis Functions (FUNCTION LOAD / FCALL) available', false, String(e?.message));
}

await main.flushall('SYNC');
await main.quit();

const summary = report.summary();
const dir = path.resolve(import.meta.dirname, '../../results');
fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(
  path.join(dir, 'proof-integrity.json'),
  JSON.stringify(
    {
      ...summary,
      strategies: Object.fromEntries(
        Object.entries(outcomes).map(([k, { o, v }]) => [
          k,
          { applied: o.applied, rejected: o.rejected, retries: o.retries, errors: o.errors, violations: v, latencyMs: stats(o.latencies) },
        ]),
      ),
    },
    null, 2,
  ),
);
console.log('  → results/proof-integrity.json');
process.exit(summary.passed ? 0 : 1);
