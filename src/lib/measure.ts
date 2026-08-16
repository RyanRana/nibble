/**
 * Memory measurement harness.
 *
 * used_memory minus client/AOF/replication buffers, after MEMORY PURGE, on a
 * flushed instance. NOT used_memory_dataset - Redis counts the key dict as
 * "overhead", which is exactly what sharding attacks. See docs/measuring.md.
 */
import type Redis from 'ioredis';
import { configSet, DEFAULT_ENCODING_CONFIG } from './redis.ts';

export interface MemSample {
  used_memory: number;
  attributable: number;
  clients: number;
  /** allocator_frag_ratio - the true external fragmentation metric.
   *  mem_fragmentation_ratio is NOT this: it also folds in process overheads. */
  allocatorFrag: number;
  keys: number;
}

async function infoMemory(r: Redis): Promise<Record<string, number>> {
  const raw = await r.info('memory');
  const out: Record<string, number> = {};
  for (const line of raw.split('\n')) {
    const idx = line.indexOf(':');
    if (idx < 0) continue;
    const v = Number(line.slice(idx + 1).trim());
    if (Number.isFinite(v)) out[line.slice(0, idx)] = v;
  }
  return out;
}

export async function sample(r: Redis): Promise<MemSample> {
  await r.ping();
  try {
    await r.memory('PURGE');
  } catch {
    /* purge is a no-op on non-jemalloc builds */
  }
  const m = await infoMemory(r);
  const keys = await r.dbsize();
  const nonData =
    (m.mem_clients_normal ?? 0) +
    (m.mem_clients_slaves ?? 0) +
    (m.mem_aof_buffer ?? 0) +
    (m.mem_replication_backlog ?? 0) +
    (m.mem_cluster_links ?? 0);
  return {
    used_memory: m.used_memory,
    attributable: m.used_memory - nonData,
    clients: m.mem_clients_normal ?? 0,
    allocatorFrag: m.allocator_frag_ratio ?? 0,
    keys,
  };
}

export interface VariantResult {
  name: string;
  note: string;
  records: number;
  totalBytes: number;
  bytesPerRecord: number;
  keys: number;
  encodings: Record<string, string>;
  extra: Record<string, string | number>;
  ms: number;
  caveat?: string;
}

export interface Variant {
  name: string;
  /** Short description of the technique under test. */
  note: string;
  /** Encoding/threshold config this variant needs. Merged over defaults. */
  config?: Record<string, string | number>;
  /** Loads the data. Returns how many logical records were stored. */
  load: (r: Redis) => Promise<number>;
  /** Keys to report OBJECT ENCODING for - proves *why* the number is what it is. */
  encodingProbes?: string[];
  /** Extra facts to record (recall@10, false-positive rate, ratios, …). */
  probe?: (r: Redis) => Promise<Record<string, string | number>>;
  /**
   * Set when the variant does NOT answer the same question as the baseline
   * (lossy retention, cardinality-only, probabilistic membership). Such rows
   * are excluded from "best result" rankings - otherwise HyperLogLog wins every
   * comparison by answering a different question very cheaply.
   */
  caveat?: string;
}

/**
 * Run a variant `repeat` times and keep the cheapest observation.
 *
 * Allocator noise is one-sided: a run can be inflated by arena fragmentation,
 * a background rehash landing mid-sample, or pages jemalloc has not returned,
 * but it cannot be *deflated* below the true allocation. The minimum across
 * repeats is therefore the best estimator of the underlying cost, and it is
 * far more stable run-to-run than the mean.
 */
export async function runVariant(r: Redis, v: Variant, repeat = 2): Promise<VariantResult> {
  let best: VariantResult | null = null;

  for (let attempt = 0; attempt < repeat; attempt++) {
    await r.flushall('SYNC');
    await configSet(r, DEFAULT_ENCODING_CONFIG);
    if (v.config) await configSet(r, v.config);

    const before = await sample(r);
    const t0 = performance.now();
    const records = await v.load(r);
    const ms = performance.now() - t0;
    const after = await sample(r);

    const totalBytes = after.attributable - before.attributable;
    if (best && totalBytes >= best.totalBytes) continue;

    const encodings: Record<string, string> = {};
    for (const k of v.encodingProbes ?? []) {
      try {
        encodings[k] = String(await r.object('ENCODING', k));
      } catch {
        encodings[k] = 'n/a';
      }
    }
    const extra = v.probe ? await v.probe(r) : {};

    best = {
      name: v.name,
      note: v.note,
      records,
      totalBytes,
      bytesPerRecord: totalBytes / records,
      keys: after.keys,
      encodings,
      extra,
      ms: Math.round(ms),
      caveat: v.caveat,
    };
  }
  return best!;
}

export interface CaseResult {
  id: string;
  title: string;
  question: string;
  unit: string;
  kind: 'compare' | 'sweep';
  variants: VariantResult[];
}

export interface BenchCase {
  id: string;
  title: string;
  /** What this case is trying to settle. */
  question: string;
  /** What one "record" means here (per run, per turn, per vector, …). */
  unit: string;
  /**
   * 'compare' - variants are alternative designs for the same job, so
   *             variant[0] is the baseline and ratios are meaningful.
   * 'sweep'   - variants are points on an axis (value size, density) and
   *             ratios between them are meaningless.
   */
  kind?: 'compare' | 'sweep';
  variants: Variant[];
}

export async function runCase(r: Redis, c: BenchCase, repeat = 2): Promise<CaseResult> {
  const variants: VariantResult[] = [];
  for (const v of c.variants) {
    process.stdout.write(`    · ${v.name} … `);
    const res = await runVariant(r, v, repeat);
    variants.push(res);
    process.stdout.write(
      `${fmtBytes(res.bytesPerRecord)}/${c.unit}  (${fmtBytes(res.totalBytes)} total, ${res.keys} keys)` +
        `${res.caveat ? `  [${res.caveat}]` : ''}\n`,
    );
  }
  return {
    id: c.id, title: c.title, question: c.question,
    unit: c.unit, kind: c.kind ?? 'compare', variants,
  };
}

export function fmtBytes(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(2)} GiB`;
  if (abs >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(2)} MiB`;
  if (abs >= 1024) return `${(n / 1024).toFixed(2)} KiB`;
  return `${n.toFixed(1)} B`;
}

/** Reduction factor of the best (last-listed baseline vs each variant). */
export function factor(baseline: number, variant: number): string {
  if (variant <= 0) return '-';
  return `${(baseline / variant).toFixed(2)}×`;
}
