# Redis RAM benchmark — raw results

Redis 8.10.0 · allocator `jemalloc-5.3.0` · generated 2026-08-16T21:39:25.312Z

All figures are deltas in `used_memory` minus client/AOF/replication buffers, sampled after `MEMORY PURGE` on an otherwise empty instance. Lower is better.

## 12-percentiles — Tool-latency percentiles (200 series × 5,000 = 1,000,000 samples)

> Sketches promise huge savings on percentiles. Where is the break-even, and where do they lie to you?

| variant | bytes/sample | total | keys | vs baseline | encoding | technique |
|---|--:|--:|--:|--:|---|---|
| A · ZSET of raw samples | 88.5 | 84.37 MiB | 200 | 1.00× | `skiplist` | Keep everything, sort on read. Exact, queryable, and the most expensive option available. |
| B · TIMESERIES COMPRESSED (integer ms) | 2.6 | 2.48 MiB | 200 | 34.04× | `—` | Purpose-built, exact, and the row most people skip straight past on their way to a sketch. |
| C · T-DIGEST, compression 100 (default) | 2.1 | 1.98 MiB | 200 | 42.62× | `—` | Fixed 9,864 B per series REGARDLESS of sample count. Cheap per sample only if you feed it enough. |
| D · T-DIGEST, compression 200 | 4.1 | 3.93 MiB | 200 | 21.45× | `—` | Double the RAM, several times the accuracy. The defensible default for anything you alert on. |
| E · T-DIGEST, compression 500 | 9.9 | 9.40 MiB | 200 | 8.97× | `—` | Diminishing returns on clean data — but see variant G for where it earns its keep. |
| F · T-DIGEST c=100, archived as DUMP blobs | 0.2 | 208.69 KiB | 4 | 413.98× | `—` | Cold rollups do not need to be live keys. DUMP persists only the merged centroids, not the input buffer. **⚠ not queryable until RESTOREd** |
| G · T-DIGEST c=100, BIMODAL (0.5% timeouts) | 2.1 | 1.98 MiB | 200 | 42.68× | `—` | Identical sketch, realistic agent data: a body of normal latencies plus calls that hit the 30 s timeout. **⚠ different input data — compare its error to C, not its size** |
| H · BIMODAL, timeouts excluded + exact counter | 2.1 | 1.98 MiB | 204 | 42.60× | `—` | The fix: digest only completed calls, count timeouts exactly. Both answers get better, and the count is exact. **⚠ different input data — compare its error to G** |

**A · ZSET of raw samples** — exact?: `yes` · also gives you: `the raw samples, for anything else you need later`

**B · TIMESERIES COMPRESSED (integer ms)** — exact?: `yes` · percentiles: `computed client-side from TS.RANGE, or via a rollup rule`

**C · T-DIGEST, compression 100 (default)** — p50 error: `0.89%` · p95 error: `0.67%` · p99 error: `0.95%` · p99.9 error: `1.63%` · bytes/series: `9864` · exact?: `no — and it has no proven error bound`

**D · T-DIGEST, compression 200** — p50 error: `0.35%` · p95 error: `0.41%` · p99 error: `0.81%` · p99.9 error: `0.00%` · bytes/series: `19464`

**E · T-DIGEST, compression 500** — p50 error: `0.25%` · p95 error: `0.26%` · p99 error: `0.49%` · p99.9 error: `0.00%` · bytes/series: `48264`

**F · T-DIGEST c=100, archived as DUMP blobs** — archive is: `RESTORE-able back into a live digest on demand` · shards: `4`

**G · T-DIGEST c=100, BIMODAL (0.5% timeouts)** — p50 error: `0.91%` · p95 error: `0.82%` · p99 error: `10.32%` · p99.9 error: `0.00%` · why: `p99 sits on the cliff between the body and the timeout spike, where the CDF is near-vertical`

**H · BIMODAL, timeouts excluded + exact counter** — p50 error: `0.85%` · p95 error: `0.72%` · p99 error: `0.89%` · p99.9 error: `1.90%` · timeout count: `exact, in a sharded hash` · note: `error is now measured against the completed-call distribution, which is the one you can act on`
