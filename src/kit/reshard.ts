/**
 * Online resharding by splitting.
 *
 * Growing a Pouch means k -> k+1. Because routing is `h % 2^k`, every key
 * either stays where it is or moves to `s + 2^k`. Half the keys move, one
 * shard at a time, while reads and writes keep running.
 *
 * The protocol, and why each step is safe:
 *
 *   1. Publish generation "k+1:k". Writers immediately target the NEW home.
 *      Readers check new, then old. A key is in at least one of the two, so no
 *      read can miss.
 *
 *   2. Drain each old shard. For every field whose new home differs:
 *        HSETNX new field val          (never clobbers a fresher direct write)
 *        if HGET old field == val then HDEL old field
 *      Both scripts touch ONE key, so both are legal in cluster mode. Nothing
 *      here needs a cross-slot transaction, which is the whole reason the
 *      protocol is shaped this way.
 *
 *   3. Publish generation "k+1". Readers stop double-checking.
 *
 * The compare-and-delete is what makes concurrent writes safe. If a client
 * running the old generation writes to the old home after the mover read it,
 * the value no longer matches and the delete is skipped, so the record survives
 * and the next pass moves it. That is why the drain loops until the old shard
 * is empty rather than making a single sweep.
 */
import { type Client, num, str } from './client.ts';
import { fnv1a, idFromField, genKeyFor } from './shard.ts';

/** Copy into the new home without overwriting anything newer. */
const CLAIM = `return redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])`;

/** Delete from the old home only if it still holds the value we moved. */
const RETIRE = `
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
  return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0`;

export interface SplitProgress {
  shard: number;
  ofShards: number;
  moved: number;
  passes: number;
}

export interface SplitOpts {
  prefix: string;
  /** Current log2(shards). The split takes you to k + 1. */
  k: number;
  /** Fields per HSCAN page. */
  batch?: number;
  /** Safety valve on the drain loop for a shard that keeps taking writes. */
  maxPasses?: number;
  /**
   * BOUNDED STALENESS WINDOW, ms. This is a correctness parameter, not a tuning
   * knob, and it must be longer than your clients' generation refresh interval.
   *
   * A client that has not refreshed still writes to the pre-split home. If we
   * cleared the draining marker while such a write was in flight, readers would
   * stop checking the old home and that write would be silently lost. So after
   * the main drain we wait out the window, sweep once more, and only then
   * declare the split finished.
   *
   * If a client can be stale for longer than this, the split is not safe. There
   * is no way to make it safe without the clients participating.
   */
  settleMs?: number;
  onProgress?: (p: SplitProgress) => void;
}

export interface SplitResult {
  from: number;
  to: number;
  moved: number;
  scanned: number;
  ms: number;
}

/** Read the published generation, or null if none has been written yet. */
export async function readGeneration(
  r: Client,
  prefix: string,
): Promise<{ k: number; prev: number | null } | null> {
  const raw = (await r.cmd('GET', genKeyFor(prefix))) as Buffer | null;
  if (!raw) return null;
  const [k, prev] = raw.toString().split(':');
  return { k: Number(k), prev: prev === undefined || prev === '' ? null : Number(prev) };
}

export async function publishGeneration(
  r: Client,
  prefix: string,
  k: number,
  prev: number | null,
): Promise<void> {
  await r.cmd('SET', genKeyFor(prefix), prev === null ? String(k) : `${k}:${prev}`);
}

/**
 * Double the shard count, online.
 *
 * Safe to re-run: a split that was interrupted leaves the generation marker in
 * the draining state, and calling `split` again finishes the drain.
 */
export async function split(r: Client, opts: SplitOpts): Promise<SplitResult> {
  const { prefix, k } = opts;
  const batch = opts.batch ?? 200;
  const maxPasses = opts.maxPasses ?? 50;
  const settleMs = opts.settleMs ?? 5_000;
  const t0 = Date.now();

  const claim = str(await r.cmd('SCRIPT', 'LOAD', CLAIM));
  const retire = str(await r.cmd('SCRIPT', 'LOAD', RETIRE));

  // Step 1: writers move to the new home, readers start checking both.
  await publishGeneration(r, prefix, k + 1, k);

  let moved = 0;
  let scanned = 0;
  const oldShards = 2 ** k;

  const drainShard = async (s: number) => {
    const oldKey = `${prefix}:{${s}}`;
    let passes = 0;

    // Loop until the shard is empty. A single sweep is not enough: a client on
    // the old generation can write into a shard we have already swept.
    for (;;) {
      passes++;
      let cursor = '0';
      let movedThisPass = 0;

      do {
        const page = await r.cmd('HSCAN', oldKey, cursor, 'COUNT', String(batch));
        if (!Array.isArray(page)) break;
        cursor = str(page[0]);
        const flat = page[1] as Buffer[];
        scanned += flat.length / 2;

        for (let i = 0; i < flat.length; i += 2) {
          const field = flat[i];
          const value = flat[i + 1];
          const id = idFromField(field);
          const home = fnv1a(id) % 2 ** (k + 1);
          if (home === s) continue; // stays put

          const newKey = `${prefix}:{${home}}`;
          await r.cmd('EVALSHA', claim, '1', newKey, field, value);
          const gone = num(await r.cmd('EVALSHA', retire, '1', oldKey, field, value));
          if (gone === 1) {
            moved++;
            movedThisPass++;
          }
        }
      } while (cursor !== '0');

      if (movedThisPass === 0 || passes >= maxPasses) break;
    }

    opts.onProgress?.({ shard: s, ofShards: oldShards, moved, passes });
  };

  for (let s = 0; s < oldShards; s++) await drainShard(s);

  // Step 3: wait out the staleness window, then sweep again. Anything a
  // not-yet-refreshed client wrote into an already-drained shard is picked up
  // here. Without this the split loses those writes the moment step 4 runs.
  await new Promise((res) => setTimeout(res, settleMs));
  for (let s = 0; s < oldShards; s++) await drainShard(s);

  // Step 4: the old homes are drained and no stale writer remains, so readers
  // can stop double-checking.
  await publishGeneration(r, prefix, k + 1, null);

  return { from: oldShards, to: 2 ** (k + 1), moved, scanned, ms: Date.now() - t0 };
}

/**
 * Grow to at least `targetShards`, splitting repeatedly.
 *
 * Each doubling is independently safe and restartable, so a growth of 4x is
 * three separate online splits rather than one long unsafe window.
 */
export async function growTo(
  r: Client,
  prefix: string,
  currentK: number,
  targetShards: number,
  opts: Partial<SplitOpts> = {},
): Promise<SplitResult[]> {
  const targetK = Math.ceil(Math.log2(Math.max(1, targetShards)));
  const out: SplitResult[] = [];
  for (let k = currentK; k < targetK; k++) {
    out.push(await split(r, { ...opts, prefix, k }));
  }
  return out;
}
