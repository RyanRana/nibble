/**
 * Benchmark driver.
 *
 * Usage:
 *   node src/bench/run-all.ts            # every case
 *   node src/bench/run-all.ts 01 04      # only cases whose id starts with 01/04
 */
import fs from 'node:fs';
import path from 'node:path';
import { connect } from '../lib/redis.ts';
import { runCase, fmtBytes, type BenchCase, type CaseResult } from '../lib/measure.ts';
import { case01 } from './case-01-runstate.ts';
import { case02 } from './case-02-transcript.ts';
import { case03 } from './case-03-spans.ts';
import { case04 } from './case-04-vectors.ts';
import { case05 } from './case-05-dedup.ts';
import { case06 } from './case-06-ttl.ts';
import { case07 } from './case-07-index.ts';
import { case08 } from './case-08-sizeclass.ts';
import { case09 } from './case-09-queue.ts';
import { case10 } from './case-10-metrics.ts';
import { case11 } from './case-11-shardsize.ts';
import { case12 } from './case-12-percentiles.ts';

const CASES: BenchCase[] = [
  case01, case02, case03, case04, case05,
  case06, case07, case08, case09, case10, case11, case12,
];

const filters = process.argv.slice(2);
const selected = filters.length
  ? CASES.filter((c) => filters.some((f) => c.id.startsWith(f)))
  : CASES;

const r = connect();

const info = await r.info('server');
const version = /redis_version:(\S+)/.exec(info)?.[1] ?? '?';
const mem = await r.info('memory');
const allocator = /mem_allocator:(\S+)/.exec(mem)?.[1] ?? '?';

console.log(`\nRedis ${version}  ·  allocator ${allocator}\n`);

const results: CaseResult[] = [];
for (const c of selected) {
  console.log(`\n▸ ${c.id}  ${c.title}`);
  console.log(`  ${c.question}`);
  results.push(await runCase(r, c));
}

await r.flushall('SYNC');
await r.quit();

const outDir = path.resolve(import.meta.dirname, '../../results');
fs.mkdirSync(outDir, { recursive: true });

const payload = { redis: version, allocator, generatedAt: new Date().toISOString(), results };
fs.writeFileSync(path.join(outDir, 'bench.json'), JSON.stringify(payload, null, 2));
fs.writeFileSync(path.join(outDir, 'bench.md'), renderMarkdown(payload));

console.log(`\n✔ wrote results/bench.json and results/bench.md\n`);
printSummary(results);

// ────────────────────────────── rendering ───────────────────────────────

function renderMarkdown(p: { redis: string; allocator: string; generatedAt: string; results: CaseResult[] }): string {
  const L: string[] = [];
  L.push(`# Redis RAM benchmark — raw results`);
  L.push('');
  L.push(`Redis ${p.redis} · allocator \`${p.allocator}\` · generated ${p.generatedAt}`);
  L.push('');
  L.push(
    `All figures are deltas in \`used_memory\` minus client/AOF/replication buffers, ` +
      `sampled after \`MEMORY PURGE\` on an otherwise empty instance. Lower is better.`,
  );
  L.push('');

  for (const c of p.results) {
    L.push(`## ${c.id} — ${c.title}`);
    L.push('');
    L.push(`> ${c.question}`);
    L.push('');
    const base = c.variants[0];
    const sweep = c.kind === 'sweep';
    L.push(
      sweep
        ? `| point | bytes/${c.unit} | total | keys | encoding | note |`
        : `| variant | bytes/${c.unit} | total | keys | vs baseline | encoding | technique |`,
    );
    L.push(sweep ? `|---|--:|--:|--:|---|---|` : `|---|--:|--:|--:|--:|---|---|`);
    for (const v of c.variants) {
      const enc = Object.entries(v.encodings).map(([, e]) => e).join(', ') || '—';
      const f = v.bytesPerRecord > 0 ? `${(base.bytesPerRecord / v.bytesPerRecord).toFixed(2)}×` : '—';
      const note = v.caveat ? `${v.note} **⚠ ${v.caveat}**` : v.note;
      L.push(
        sweep
          ? `| ${v.name} | ${v.bytesPerRecord.toFixed(1)} | ${fmtBytes(v.totalBytes)} | ${v.keys.toLocaleString()} | \`${enc}\` | ${note} |`
          : `| ${v.name} | ${v.bytesPerRecord.toFixed(1)} | ${fmtBytes(v.totalBytes)} | ${v.keys.toLocaleString()} | ${f} | \`${enc}\` | ${note} |`,
      );
    }
    L.push('');
    for (const v of c.variants) {
      const ex = Object.entries(v.extra);
      if (ex.length) {
        L.push(`**${v.name}** — ${ex.map(([k, val]) => `${k}: \`${val}\``).join(' · ')}`);
        L.push('');
      }
    }
  }
  return L.join('\n');
}

function printSummary(rs: CaseResult[]) {
  console.log('┌─ summary (best like-for-like variant; ⚠-caveat rows excluded) ────────');
  for (const c of rs) {
    if (c.kind === 'sweep') {
      console.log(`│ ${c.id.padEnd(16)} (sweep — see results/bench.md)`);
      continue;
    }
    const base = c.variants[0];
    const eligible = c.variants.filter((v) => !v.caveat);
    const best = eligible.reduce((a, b) => (b.bytesPerRecord < a.bytesPerRecord ? b : a));
    const f = best.bytesPerRecord > 0 ? (base.bytesPerRecord / best.bytesPerRecord).toFixed(1) : '—';
    console.log(
      `│ ${c.id.padEnd(16)} ${fmtBytes(base.bytesPerRecord).padStart(10)} → ${fmtBytes(
        best.bytesPerRecord,
      ).padStart(10)}  ${String(f + '×').padStart(7)}  ${best.name}`,
    );
  }
  console.log('└──────────────────────────────────────────────────────────────────────');
}
