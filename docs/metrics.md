# Percentiles and time series

> "What's my p95 tool latency?" - and the two ways of answering it that cost 40×
> more than they need to.

---

## Copy this

Integer values, compressed encoding. That's the whole optimization.

```ts
// one series per (tool × tenant), or whatever you actually group by
await redis.cmd('TS.CREATE', `lat:${tool}:${tenant}`,
  'ENCODING', 'COMPRESSED',
  'RETENTION', String(24 * 3600 * 1000),
  'LABELS', 'metric', 'tool_latency_ms', 'tool', tool);

// round to an integer BEFORE you store it - see below, it is worth 4×
await redis.cmd('TS.ADD', `lat:${tool}:${tenant}`, String(tsMs), String(Math.round(ms)));
```

**2.1 bytes per sample**, exact, range-queryable. A ZSET of the same samples
costs 88.6 B - a **42.8×** difference.

---

## Round your metrics to integers

Time-series compression XORs consecutive float64s and stores only the differing
mantissa window. An integer millisecond count has a short, stable mantissa. The
same value with two decimal places has a long, noisy one.

Identical random walk, identical series, only the rounding changes:

| Stored as | B/sample |
|---|--:|
| ZSET, `"<ts>:<value>"` members | 88.6 |
| STREAM, ts + value fields | 16.8 |
| TIMESERIES, `UNCOMPRESSED` | 16.8 |
| TIMESERIES, `COMPRESSED`, **2 decimal places** | 8.6 |
| **TIMESERIES, `COMPRESSED`, integer ms** | **2.1** |

Four times cheaper for a `Math.round()`. If you genuinely need sub-millisecond
resolution, store microseconds as an integer rather than milliseconds as a float.

An adversarial control: pure incompressible noise also measured 8.6 B/sample, so
the float penalty is real and not an artifact of the signal.

---

## Downsampling is a config change

Raw for a day, hourly averages for a year, set up once at series creation:

```ts
await redis.cmd('TS.CREATE', key, 'ENCODING', 'COMPRESSED',
                'RETENTION', String(86_400_000));            // raw: 24h
await redis.cmd('TS.CREATE', `${key}:1h`, 'ENCODING', 'COMPRESSED',
                'RETENTION', String(31_536_000_000));        // rollup: 1y
await redis.cmd('TS.CREATERULE', key, `${key}:1h`, 'AGGREGATION', 'avg', '3600000');
```

Redis maintains the rollup on write. Measured 3.7 B/sample including both tiers.

---

## When a sketch is NOT the answer

This is the part that surprised me, so it gets the space.

A **t-digest** summarizes a distribution in fixed space, so its cost per sample
falls as you add samples. The obvious conclusion is "use a t-digest for
percentiles". The obvious conclusion is often wrong.

**A t-digest's memory is allocated at `CREATE` and never changes.** Verified on
the live instance - 9,864 B at compression 100, and *still* 9,864 B after
100,000 adds:

```
bytes = 80 + 16 × (6 × compression + 10)
```

| compression | bytes/series |
|--:|--:|
| 100 (default) | 9,864 |
| 200 | 19,464 |
| 500 | 48,264 |

So it only pays off above a break-even sample count:

| vs | break-even (C=100) |
|---|--:|
| ZSET of raw samples (88.6 B/sample) | ~111 samples |
| **TimeSeries COMPRESSED (2.1 B/sample)** | **~4,700 samples** |

Measured head-to-head at 200 series × 5,000 samples:

| | B/sample | exact? |
|---|--:|---|
| ZSET of raw samples | 88.5 | yes |
| **TIMESERIES COMPRESSED** | **2.6** | **yes** |
| T-DIGEST, compression 100 | 2.1 | no |
| T-DIGEST, compression 200 | 4.1 | no |
| T-DIGEST, compression 500 | 9.9 | no |

> **At 5,000 samples per series the sketch essentially ties the exact answer.**
> Below that it loses outright. Use TimeSeries unless you are well past ~10k
> samples per series per window - then a digest starts to win properly.

And watch the cardinality trap: a digest costs its full 9,864 B whether it holds
one sample or a billion. Per-minute digests at (tenant × tool) cardinality are
enormously more expensive than hourly ones, for data you will look at once.

---

## Accuracy, measured

Compression buys accuracy on clean data, with diminishing returns:

| compression | p50 | p95 | p99 | p99.9 |
|--:|--:|--:|--:|--:|
| 100 | 0.89% | 0.67% | 0.95% | 1.63% |
| 200 | 0.35% | 0.41% | 0.81% | 0.00% |
| 500 | 0.25% | 0.26% | 0.49% | 0.00% |

---

## ⚠ Timeouts destroy p99 - and agent traffic is full of timeouts

Agent tool calls are **bimodal**: a body of normal latencies, plus a spike of
calls that hit the timeout. p99 lands exactly on the cliff between them, where
the CDF is near-vertical - so a tiny rank error becomes an enormous value error.

Same sketch, same compression, only the input changes:

| input | p50 | p95 | **p99** | p99.9 |
|---|--:|--:|--:|--:|
| clean lognormal | 0.89% | 0.67% | **0.95%** | 1.63% |
| **+ 0.5% hitting a 30 s timeout** | 0.91% | 0.82% | **10.32%** | 0.00% |
| timeouts excluded, counted separately | 0.85% | 0.72% | **0.89%** | 1.90% |

**An 11× error swing from half a percent of your traffic.**

The fix costs nothing and makes both numbers better:

```ts
if (durationMs >= TIMEOUT_MS) {
  // exact count, in a shared hash - one field per series
  await redis.cmd('HINCRBY', `timeouts:{${shard}}`, seriesId, '1');
} else {
  await redis.cmd('TDIGEST.ADD', `lat:${seriesId}`, String(durationMs));
}
```

You get an accurate distribution for the calls that completed, and an **exact**
timeout count - which is the number you actually alert on anyway.

---

## Archiving cold digests

`DUMP` persists only the merged centroids, not the input buffer that dominates a
live digest. Measured: 200 archived digests in sharded hashes cost **0.2
B/sample vs 2.1** - a 9.8× archive win. `RESTORE` when someone asks.

```ts
const blob = await redis.cmd('DUMP', `lat:${s}`);
await redis.cmd('HSET', `cold:{${s % 64}}`, String(s), blob);
await redis.cmd('DEL', `lat:${s}`);
```

---

## Operational sharp edges

These are undocumented or easy to miss, and each one has bitten someone:

- **`TDIGEST.MERGE` accumulates and is not idempotent.** Merging the same source
  twice doubles it. **A retried rollup job silently double-counts.** Pass
  `OVERRIDE` to make rollups safe to retry.
- **`TDIGEST.MERGE` clears the destination's TTL.** Re-apply `EXPIRE` after every
  rollup or your archive tiers never expire.
- **`TDIGEST.ADD` does not create the key.** It errors. `TS.ADD` does create one.
- **Read commands mutate the sketch.** `QUANTILE`/`CDF`/`RANK` compress
  internally, so results are mildly path-dependent on read timing.
- **`TDIGEST.MIN`/`MAX` are exact**, even after arbitrary merging - safe for a
  "worst observed latency" report.
- **`TDIGEST.INFO` → `Observations` is exact** and stays exact through merges.
  It is the one sketch output safe to bill on.

---

## What not to bill on

Percentiles from a t-digest have **no proven error bound**, and this page has
already shown a 10% p99 error on realistic data. Fine for dashboards, alerting,
autoscaling and capacity planning. Not fine for an invoice or a contractual SLA.

Bill on exact counters - `TDIGEST.INFO → Observations`, or a plain `HINCRBY`.
The things you need to be exact about (call counts, threshold breaches) are the
cheap ones; the expensive thing is percentiles, and percentiles are exactly what
you shouldn't bill on.
