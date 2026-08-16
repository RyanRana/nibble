/**
 * Measured bytes -> monthly bill.
 *
 * Three multipliers people drop, worth ~3× together: reserved memory (~25%),
 * fork/COW during BGSAVE, and replication. Reads results/bench.json.
 */
import fs from 'node:fs';
import path from 'node:path';

const dir = path.resolve(import.meta.dirname, '../../results');
const benchPath = path.join(dir, 'bench.json');
if (!fs.existsSync(benchPath)) {
  console.error('run `node src/bench/run-all.ts` first — this model reads results/bench.json');
  process.exit(1);
}
const bench = JSON.parse(fs.readFileSync(benchPath, 'utf8'));

/** bytes/record for a given case + a predicate that picks the variant. */
function pick(caseId: string, match: string): { name: string; bytes: number } {
  const c = bench.results.find((x: any) => x.id === caseId);
  if (!c) throw new Error(`case ${caseId} missing from bench.json`);
  const v = c.variants.find((x: any) => x.name.startsWith(match));
  if (!v) throw new Error(`variant "${match}" missing from ${caseId}`);
  return { name: v.name, bytes: v.bytesPerRecord };
}

/** Cheapest variant that answers the same question (no caveat rows). */
function best(caseId: string): { name: string; bytes: number } {
  const c = bench.results.find((x: any) => x.id === caseId);
  const eligible = c.variants.filter((v: any) => !v.caveat);
  const v = eligible.reduce((a: any, b: any) => (b.bytesPerRecord < a.bytesPerRecord ? b : a));
  return { name: v.name, bytes: v.bytesPerRecord };
}

function baseline(caseId: string): { name: string; bytes: number } {
  const c = bench.results.find((x: any) => x.id === caseId);
  return { name: c.variants[0].name, bytes: c.variants[0].bytesPerRecord };
}

// ─────────────────────── the reference workload ────────────────────────

/**
 * A mid-size agentic platform. Deliberately concrete so the arithmetic is
 * auditable; change these six numbers to model your own.
 */
const W = {
  label: 'Reference agentic platform',
  activeRuns: 2_000_000,        // runs whose state is live in the hot tier
  turnsPerRun: 40,              // conversation turns retained
  spansPerRun: 100,             // tool-call spans ingested per run
  memories: 20_000_000,         // semantic memory vectors
  dedupIdsPerDay: 500_000_000,  // idempotency keys, 24h retention
  cacheEntries: 50_000_000,     // tool-result cache entries
  queueDepth: 2_000_000,        // tasks in flight
  indexMemberships: 100_000_000,// secondary-index entries
  metricSamples: 500_000_000,   // telemetry samples retained
};

interface Line {
  component: string;
  records: number;
  naiveBytes: number;
  optBytes: number;
  naiveName: string;
  optName: string;
}

const LINES: Line[] = [
  {
    component: 'Agent run state',
    records: W.activeRuns,
    ...zip(baseline('01-run-state'), best('01-run-state')),
  },
  {
    component: 'Conversation transcripts',
    records: W.activeRuns * W.turnsPerRun,
    ...zip(baseline('02-transcript'), best('02-transcript')),
  },
  {
    component: 'Tool-call spans',
    records: W.activeRuns * W.spansPerRun,
    ...zip(baseline('03-spans'), best('03-spans')),
  },
  {
    component: 'Semantic memory (searchable)',
    records: W.memories,
    ...zip(pick('04-vectors', 'A ·'), pick('04-vectors', 'H ·')),
  },
  {
    component: 'Idempotency / dedup',
    records: W.dedupIdsPerDay,
    ...zip(baseline('05-dedup'), best('05-dedup')),
  },
  {
    component: 'Tool-result cache',
    records: W.cacheEntries,
    ...zip(baseline('06-ttl'), best('06-ttl')),
  },
  {
    component: 'Secondary indexes',
    records: W.indexMemberships,
    ...zip(baseline('07-index'), best('07-index')),
  },
  {
    component: 'Task queue',
    records: W.queueDepth,
    ...zip(baseline('09-queue'), best('09-queue')),
  },
  {
    component: 'Telemetry',
    records: W.metricSamples,
    ...zip(baseline('10-metrics'), best('10-metrics')),
  },
];

function zip(a: { name: string; bytes: number }, b: { name: string; bytes: number }) {
  return { naiveBytes: a.bytes, optBytes: b.bytes, naiveName: a.name, optName: b.name };
}

const GiB = 1024 ** 3;
const naiveTotal = LINES.reduce((s, l) => s + l.records * l.naiveBytes, 0);
const optTotal = LINES.reduce((s, l) => s + l.records * l.optBytes, 0);

// ────────────────────────────── pricing ────────────────────────────────

interface Sku {
  name: string;
  advertisedPerGiBMonth: number;
  usableFraction: number;
  note: string;
}

/**
 * List, on-demand, 730 h/month. Sources are vendor price lists (AWS Price List
 * API, Azure Retail Prices API, published Redis Cloud plan tables) as of
 * August 2026. `usableFraction` is the vendor's own documented reserve.
 */
const SKUS: Sku[] = [
  {
    name: 'ElastiCache Valkey r8g.16xlarge',
    advertisedPerGiBMonth: 8.13,
    usableFraction: 0.75,
    note: 'cheapest RAM per GiB found; 25% reserved-memory-percent',
  },
  {
    name: 'ElastiCache Valkey r7g.xlarge',
    advertisedPerGiBMonth: 9.70,
    usableFraction: 0.75,
    note: 'Valkey engine is exactly 20% cheaper than Redis OSS on ElastiCache',
  },
  {
    name: 'ElastiCache Redis OSS r7g.xlarge',
    advertisedPerGiBMonth: 12.12,
    usableFraction: 0.75,
    note: 'same hardware, Redis engine; AWS ships r8g for Valkey only',
  },
  {
    name: 'GCP Memorystore highmem',
    advertisedPerGiBMonth: 10.80,
    usableFraction: 0.80,
    note: 'AOF billed separately at ~$0.40/GB-month',
  },
  {
    name: 'Azure Managed Redis M2000',
    advertisedPerGiBMonth: 10.51,
    usableFraction: 0.80,
    note: '10% maxmemory-reserved + 10% fragmentation reserve',
  },
  {
    name: 'MemoryDB Valkey r7g.xlarge',
    advertisedPerGiBMonth: 11.98,
    usableFraction: 0.75,
    note: 'durable by design (Multi-AZ transaction log); writes free to 10 TB/mo on Valkey',
  },
  {
    name: 'ElastiCache Valkey r6gd (SSD-tiered)',
    advertisedPerGiBMonth: 3.63,
    usableFraction: 0.75,
    note: '~+300 µs on SSD hits; NOT combinable with synchronous durability',
  },
  {
    name: 'Redis Cloud Essentials Flex',
    advertisedPerGiBMonth: 5.00,
    usableFraction: 1.0,
    note: 'flat $5/GB 1–100 GB; ~6× lower throughput allowance than the RAM plans',
  },
];

const fmtG = (b: number) => `${(b / GiB).toFixed(1)} GiB`;
const money = (n: number) =>
  n >= 1000 ? `$${(n / 1000).toFixed(1)}k` : `$${n.toFixed(0)}`;

console.log(`\n\x1b[1m▸ Capacity & cost model — ${W.label}\x1b[0m`);
console.log(`  measured on Redis ${bench.redis}, allocator ${bench.allocator}\n`);

console.log(
  '  component'.padEnd(32) + 'records'.padStart(14) +
  'naive'.padStart(12) + 'optimized'.padStart(12) + '  factor',
);
console.log('  ' + '─'.repeat(84));
for (const l of LINES) {
  const n = l.records * l.naiveBytes;
  const o = l.records * l.optBytes;
  console.log(
    '  ' + l.component.padEnd(30) +
    l.records.toLocaleString().padStart(14) +
    fmtG(n).padStart(12) + fmtG(o).padStart(12) +
    `  ${(n / o).toFixed(1)}×`,
  );
}
console.log('  ' + '─'.repeat(84));
console.log(
  '  ' + 'TOTAL'.padEnd(30) + ''.padStart(14) +
  fmtG(naiveTotal).padStart(12) + fmtG(optTotal).padStart(12) +
  `  \x1b[1m${(naiveTotal / optTotal).toFixed(1)}×\x1b[0m`,
);

console.log(`\n  Monthly cost of the SAME workload, list price, single primary (no HA):\n`);
console.log(
  '  SKU'.padEnd(42) + '$/usable GiB'.padStart(14) +
  'naive'.padStart(11) + 'optimized'.padStart(11) + '   saved/mo',
);
console.log('  ' + '─'.repeat(92));

const rows: any[] = [];
for (const s of SKUS) {
  const perUsable = s.advertisedPerGiBMonth / s.usableFraction;
  const nCost = (naiveTotal / GiB) * perUsable;
  const oCost = (optTotal / GiB) * perUsable;
  rows.push({ sku: s.name, perUsable: +perUsable.toFixed(2), naive: Math.round(nCost), optimized: Math.round(oCost), note: s.note });
  console.log(
    '  ' + s.name.padEnd(40) +
    `$${perUsable.toFixed(2)}`.padStart(14) +
    money(nCost).padStart(11) + money(oCost).padStart(11) +
    `   ${money(nCost - oCost)}`,
  );
}

// ── The multipliers people forget ────────────────────────────────────────

const ref = SKUS[1]; // ElastiCache Valkey r7g.xlarge
const perUsable = ref.advertisedPerGiBMonth / ref.usableFraction;
const optGiB = optTotal / GiB;

console.log(`\n  What durability and HA actually add (${ref.name}, optimized layout):\n`);
const configs = [
  { label: 'advertised RAM, naive arithmetic', mult: ref.usableFraction, extra: 1 },
  { label: 'after 25% reserved memory', mult: 1, extra: 1 },
  { label: '+ 1 replica for HA', mult: 1, extra: 2 },
  { label: '+ synchronous durability (+18%/node)', mult: 1, extra: 2 * 1.18 },
];
for (const c of configs) {
  const cost = optGiB * perUsable * c.mult * c.extra;
  console.log(
    '  ' + c.label.padEnd(44) + money(cost).padStart(10) + '/mo' +
    `   ($${(perUsable * c.mult * c.extra).toFixed(2)}/GiB)`,
  );
}

console.log(`
  Read that ladder carefully: the headline $/GB understates a durable,
  replicated deployment by about 3×. Optimizing the layout is what buys that
  multiplier back — the ${(naiveTotal / optTotal).toFixed(1)}× RAM reduction above is worth more than the
  difference between any two vendors on the list.

  And note the structural constraint the pricing exposes: on ElastiCache,
  synchronous durability and SSD tiering are mutually exclusive, and tiering
  costs ~300 µs on SSD hits. Cheap, durable, fast — pick two. Shrinking the
  dataset is the only lever that does not force that choice.
`);

fs.writeFileSync(
  path.join(dir, 'capacity.json'),
  JSON.stringify(
    {
      workload: W,
      lines: LINES.map((l) => ({
        ...l,
        naiveTotalBytes: l.records * l.naiveBytes,
        optTotalBytes: l.records * l.optBytes,
        factor: +(l.naiveBytes / l.optBytes).toFixed(2),
      })),
      naiveTotalBytes: naiveTotal,
      optTotalBytes: optTotal,
      overallFactor: +(naiveTotal / optTotal).toFixed(2),
      pricing: rows,
      durabilityLadder: configs.map((c) => ({
        label: c.label,
        monthly: Math.round(optGiB * perUsable * c.mult * c.extra),
      })),
    },
    null, 2,
  ),
);
console.log('  → results/capacity.json\n');
