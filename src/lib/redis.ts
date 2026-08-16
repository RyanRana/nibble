import Redis from 'ioredis';

export const REDIS_PORT = Number(process.env.REDIS_PORT ?? 6399);
export const REDIS_HOST = process.env.REDIS_HOST ?? '127.0.0.1';

export function connect(opts: Record<string, unknown> = {}): Redis {
  return new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    maxRetriesPerRequest: null,
    enableAutoPipelining: false,
    lazyConnect: false,
    ...opts,
  });
}

/** Run a raw command and get the reply as a Buffer (never utf8-mangled). */
export function callBuf(r: Redis, ...args: (string | Buffer | number)[]): Promise<Buffer> {
  // @ts-ignore ioredis exposes Buffer variants via callBuffer
  return r.callBuffer(...args);
}

/**
 * Execute `cmds` in bounded pipeline batches.
 *
 * Batch size matters for measurement integrity, not just speed: an unbounded
 * pipeline inflates the client's output buffer, and Redis counts that buffer in
 * used_memory. Bounded batches keep client memory ~flat between samples.
 */
export async function pipe(
  r: Redis,
  cmds: (string | Buffer | number)[][],
  batch = 1000,
): Promise<void> {
  for (let i = 0; i < cmds.length; i += batch) {
    const p = r.pipeline();
    for (const c of cmds.slice(i, i + batch)) {
      // @ts-ignore variadic raw command
      p.call(...c);
    }
    const res = await p.exec();
    if (res) {
      for (const [err] of res) if (err) throw err;
    }
  }
}

export async function configSet(r: Redis, kv: Record<string, string | number>): Promise<void> {
  for (const [k, v] of Object.entries(kv)) await r.config('SET', k, String(v));
}

export async function configGet(r: Redis, key: string): Promise<string> {
  const res = (await r.config('GET', key)) as string[];
  return res[1];
}

/**
 * The ACTUAL Redis 8.10 defaults, restored between every variant.
 *
 * Verified against a pristine `redis:8-alpine` container rather than from
 * memory, because two of these were wrong in an earlier version of this file
 * and one of them changed real measurements:
 *
 *   hash-max-listpack-entries  was 128 here, is actually 512.  Harmless in
 *     practice — every case that cares sets its own threshold — but it meant
 *     the row labelled "default" was not the default.
 *
 *   list-max-listpack-size     was 128 here, is actually -2.  NOT harmless.
 *     A negative value is a SIZE cap (-2 = 8 KiB per quicklist node), not an
 *     entry count. With ~1.2 KiB transcript turns that is ~6 turns per node
 *     instead of 128, which changes quicklist memory materially. Cases 02, 03
 *     and 09 were re-measured after this fix.
 */
export const DEFAULT_ENCODING_CONFIG: Record<string, string> = {
  // Redis 8.10 "compact hashes": 0 disables the shared field-name template.
  // Reset explicitly so a variant that enables it cannot leak into the next one.
  'hash-min-template-entries': '0',
  'hash-max-template-entries': '0',
  'hash-max-listpack-entries': '512',
  'hash-max-listpack-value': '64',
  'list-max-listpack-size': '-2',
  'list-compress-depth': '0',
  'set-max-intset-entries': '512',
  'set-max-listpack-entries': '128',
  'set-max-listpack-value': '64',
  'zset-max-listpack-entries': '128',
  'zset-max-listpack-value': '64',
};
