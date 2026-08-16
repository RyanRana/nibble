/**
 * █ nibble - layout primitives for Redis that make agent state small.
 *
 * Everything exported here is measured in `results/bench.json`. Nothing here is
 * clever; it is just the arrangement of bytes that the allocator happens to
 * charge least for, plus the discipline to check.
 */
export { schema, uuid, enum_, varint, svarint, u8, str, blob, flags } from './schema.ts';
export type { Codec, FieldType, Shape, Infer } from './schema.ts';

export { Pouch, fnv1a, shardCountFor, idFromField, genKeyFor } from './shard.ts';
export { split, growTo, readGeneration, publishGeneration } from './reshard.ts';
export type { SplitOpts, SplitResult, SplitProgress } from './reshard.ts';
export type { ShardOpts } from './shard.ts';

export { ShardedCache } from './cache.ts';
export type { CacheOpts } from './cache.ts';

export { ExactSeen, BloomSeen, hash52 } from './seen.ts';
export type { SeenOpts, BloomOpts } from './seen.ts';

export { VectorMemory, encodeVector, decodeVector, storageBytes, measureRecall } from './vectors.ts';

export { AgentCache } from './agent-cache.ts';
export type { AgentCacheOpts, CacheResult, CacheStats, HitKind } from './agent-cache.ts';
export type { Quant, VectorSetOpts } from './vectors.ts';

// NOTE: client.ts also exports `str`/`num` reply helpers, deliberately NOT
// re-exported here - `str()` in the public API is the schema field type.
// Import them from './client.ts' directly if you need them.
export { Client, isErr } from './client.ts';
export type { Arg, Reply } from './client.ts';
export { zstd, unzstd, makeDictionary, deflateDict, inflateDict } from '../lib/codec.ts';

import { Client } from './client.ts';

/** Open a connection. Reads REDIS_URL, else localhost. */
export const connect = (url?: string) => Client.connect(url);

async function configSet(r: Client, kv: Record<string, string | number>): Promise<void> {
  await r.pipelineChunked(Object.entries(kv).map(([k, v]) => ['CONFIG', 'SET', k, String(v)]));
}

/**
 * The config the kit wants. Call once at startup.
 *
 * These are per-instance, not per-key, so they affect everything on the node.
 * Read `docs/tuning.md` before widening them past what your shards need -
 * a huge listpack threshold turns unrelated small hashes into slow linear scans.
 */
export async function applyKitConfig(
  r: Client,
  opts: { shardWidth?: number; maxValueBytes?: number; hashTemplates?: boolean } = {},
): Promise<void> {
  const width = opts.shardWidth ?? 124;
  await configSet(r, {
    'hash-max-listpack-entries': String(Math.max(128, width * 2)),
    'hash-max-listpack-value': String(opts.maxValueBytes ?? 512),
  });

  // Redis 8.10 compact hashes: store field names ONCE across hashes that share
  // a schema. Worth 1.78× when you keep one hash per record with real field
  // names (729 -> 409 B/run measured).
  //
  // DEFAULT OFF, because it is actively harmful to the sharded layouts this kit
  // is built around. A shard's field names are unique record ids, so there is no
  // schema to share and the template is pure overhead: measured 70.1 -> 84.4
  // B/record, a 20% REGRESSION.
  //
  // These are the two biggest wins available and they are mutually exclusive.
  // Sharding wins (147 vs 409 B/run), so the kit shards and leaves this off.
  // Turn it on only for hashes you have deliberately NOT sharded.
  if (opts.hashTemplates ?? false) {
    // Redis < 8.10 does not know these params; ignoring the error is the
    // correct degradation, not a bug to fix.
    await r.raw('CONFIG', 'SET', 'hash-min-template-entries', '4');
    await r.raw('CONFIG', 'SET', 'hash-max-template-entries', '128');
  } else {
    await r.raw('CONFIG', 'SET', 'hash-min-template-entries', '0');
    await r.raw('CONFIG', 'SET', 'hash-max-template-entries', '0');
  }
}

/**
 * The single most important line of config in this repo, and it is not about
 * memory layout at all.
 *
 * `allkeys-*` policies treat your durable records as cache. Measured: filling
 * an instance under `allkeys-lru` destroyed 99.8% of durable records while
 * reporting success on every write. Under `volatile-ttl` it destroyed none,
 * because a key with no TTL is never an eviction candidate.
 *
 * So: durable records get NO TTL, cache entries get one, and both live safely
 * on the same instance.
 */
export async function applySafeEviction(r: Client, maxmemoryBytes?: number): Promise<void> {
  const cfg: Record<string, string> = { 'maxmemory-policy': 'volatile-ttl' };
  if (maxmemoryBytes) cfg.maxmemory = String(maxmemoryBytes);
  await configSet(r, cfg);
}
