# Running Redis as a primary database

> Five objections. Four are fixable. One is not, and we say so.

Every claim here is a program in `src/proof/` that exits non-zero when it stops
being true. Run them.

---

## Copy this

```ts
await applySafeEviction(redis, 8 * 1024 ** 3);   // volatile-ttl + a memory ceiling
```

```
appendonly yes
appendfsync everysec              # or `always` if you cannot lose ~2 s
no-appendfsync-on-rewrite no      # audit this - `yes` silently defeats `always`
maxmemory-policy volatile-ttl     # durable records carry no TTL, so cannot be evicted
maxmemory-clients 5%              # default 0 = unlimited = a real outage path
```

Then verify it against your own instance, not ours:

```bash
node doctor.mjs redis://your-host:6379
```

Every claim below is a program in `src/proof/` that exits non-zero when it stops
being true.

---

## Durability

**The objection is correct by default, and the default is dangerous.**

```bash
node src/proof/durability.ts
```

SIGKILL the container mid-write, restart, count how many *acknowledged* writes
came back:

| Config | Survived |
|---|--:|
| stock (`appendonly no`) | **0 / 20,000** |
| RDB snapshots only | **0 / 20,000** |
| AOF, `appendfsync everysec` | **20,000 / 20,000** |
| AOF, `appendfsync always` | **20,000 / 20,000** |

### The distinction most write-ups blur

There are two crashes and they have different answers.

**Process loss** - segfault, OOM killer, container killed, pod evicted. The
kernel survives, so anything Redis has `write(2)`-ed is already in the page
cache and still lands on disk. `everysec` loses **nothing** here, because the
`everysec` window is an *fsync* window, not a `write()` window.

**Machine loss** - power failure, kernel panic, instance vanishing. The page
cache goes with it. Now the fsync window is real.

`docker kill -s KILL` reproduces process loss exactly. Nothing running inside a
healthy kernel can reproduce machine loss - so we measured the window instead of
asserting it.

`WAITAOF` blocks until a write is fsynced but does **not** trigger an fsync. So
its latency after a write *is* the exposure window:

```
everysec    p50 1,034 ms    p99 1,083 ms    max 1,142 ms
always      p50  0.26 ms    p99  0.87 ms    max  2.98 ms
```

**The docs understate the bound.** redis.io says "1 second". `src/aof.c`
postpones the `write()` by up to **2000 ms** when an fsync is already in flight.
Plan for ~2 s and watch `aof_delayed_fsync`.

### Two traps

**`no-appendfsync-on-rewrite yes`** degrades you to `appendfsync no` during
every AOF rewrite - and it returns *before* the `always` branch, so it defeats
`appendfsync always` entirely. Audit this.

**`WAITAOF` under `everysec`** collapsed to **201 ops/s**, because each call
waits for the next *scheduled* fsync rather than triggering one. If you want
per-write durability confirmation you need `appendfsync always`, where the same
pattern sustained ~138k ops/s.

### Recovery

500,000 records reloaded from the AOF in **0.28 s**. Reload is single-threaded
and roughly linear in dataset size - budget your RTO from that rate, and keep a
replica if the number it gives you is too slow.

### What we're not claiming

Throughput cost of durability measured between 7% and 43% across runs on a
contended laptop. That spread is the rig, not Redis. Benchmark it on your own
hardware. The survival counts and fsync windows reproduced tightly; the
throughput deltas did not.

---

## Eviction

**The scariest failure mode Redis has, and completely real.**

```bash
node src/proof/eviction.ts
```

Fill an instance past `maxmemory` with 20,000 durable records plus cache traffic:

| Policy | Durable records destroyed | Errors raised |
|---|--:|--:|
| `allkeys-lru` | **19,965 of 20,000 (99.8%)** | **0** |
| `allkeys-random` | 17,457 | 0 |
| `volatile-ttl` | **0** | 0 |
| `noeviction` | **0** | 2,181 |

Under `allkeys-lru` Redis reported success on every single write while deleting
almost everything. The word "allkeys" is not decoration.

### The fix isn't clever, it's deliberate

Eviction is a per-key property in disguise, and `volatile-*` policies expose it:
**a key with no TTL is never an eviction candidate.**

```
durable records  →  written WITHOUT a TTL  →  never evictable
cache entries    →  written WITH a TTL     →  evictable, by design
```

One instance holds both, safely. `applySafeEviction()` sets this up.

Under `noeviction` you get a loud, machine-readable failure instead:

```
OOM command not allowed when used memory > 'maxmemory'.
```

That's an incident you can page on, rather than silent corruption you discover
next quarter.

### Budget against the instance, not your records

At the eviction boundary, **41% of `used_memory` was overhead** - key dict,
client buffers, replication backlog - not values. And `maxmemory-clients`
defaults to `0` (unlimited), so one slow consumer's output buffer can push a
healthy dataset into eviction without your data growing at all.

---

## Atomicity

**Half the objection is correct.** `MULTI`/`EXEC` does **not** roll back. Proven
live: a transaction with one failing command still applied the commands around
it. It gives you batching and isolation, not atomicity of effect. Do not build
invariants on it.

The other half is wrong. Redis runs a Lua script as a single unit against a
single-threaded core - stronger isolation than the READ COMMITTED most people
actually run Postgres at.

```bash
node src/proof/integrity.ts
```

64 workers racing on the same agent runs, checked against four invariants:

| Strategy | Violations | Retries | p99 |
|---|--:|--:|--:|
| read-modify-write | **69** | 0 | 10.2 ms |
| `WATCH`/`MULTI`/`EXEC` | 0 | 285 | 23.5 ms |
| **Lua procedure** | **0** | **0** | 11.3 ms |

Read-modify-write - what most teams actually ship - corrupted the store:
mismatched indexes, runs in the wrong number of index sets, lost version bumps.

### Validation the client cannot bypass

The Lua script carries the legality table, so a buggy or out-of-date client
*physically cannot* write an illegal state:

```lua
local legal = {
  [0] = {[1]=true, [5]=true},        -- queued  → running | cancelled
  [1] = {[2]=true, [3]=true, ...},   -- running → awaiting | succeeded | ...
  [3] = {}, [4] = {}, [5] = {}       -- terminal states are terminal
}
if not legal[cur] or not legal[cur][nxt] then return 0 end
```

A request for `succeeded → running` was refused; the run stayed `succeeded`.

### The layout synergy

The script patches the status byte and version counter of a **schema-packed
binary record in place**, using `string.byte`/`string.char`, never decoding the
other 107 bytes:

```lua
local cur = string.byte(rec, 1)
local patched = string.char(nxt) .. version_bytes .. string.sub(rec, 6)
```

The RAM optimization doesn't cost you server-side logic - it *enables* it,
because there's no round trip. And because every key for a shard shares one
`{hash tag}`, the whole script touches a single cluster slot and stays correct
under Redis Cluster.

Load it as a **Redis Function** and it persists in the RDB/AOF and replicates to
replicas - server-side logic becomes part of the database rather than part of
whichever client happens to connect.

---

## Querying

**Obsolete objection** - Redis 8 ships the Query Engine in the open-source
build. But there's a real tension with [sharding](records.md) worth stating
plainly: **`FT.SEARCH` indexes keys by prefix and cannot see inside a hash.**

```bash
node src/proof/query.ts
```

100,000 runs, four queries, all three designs checked against ground truth
computed outside Redis:

| Design | RAM/run | Status count p50 | Query language |
|---|--:|--:|---|
| sharded + hand-rolled indexes | **205 B** | 2.03 ms | you write it |
| **hybrid** | 519 B | 0.56 ms | **full** |
| hash per run + full FT index | 1,058 B | 0.56 ms | full |

### The hybrid layout

Keep the fat record packed inside a sharded hash. Index a **small** document
holding only the columns you actually filter on.

```
run:{7}        HASH   <16-byte id> → <111-byte packed record>   ← not indexed
ix:<run-id>    HASH   s=running m=claude-opus-5 t=<tenant> c=913222
```

```
FT.CREATE idx:ix ON HASH PREFIX 1 ix: SCHEMA
  s TAG  m TAG  t TAG  c NUMERIC SORTABLE
```

**2.04× cheaper than indexing everything**, identical query surface. You index
the four columns you filter on, not the twenty you store.

If you don't need a query language, hand-rolled inverted indexes are cheaper
still - `SINTERCARD` and `ZCOUNT` do the set algebra server-side, and the
per-shard fan-out pipelines into one round trip.

---

## The one we can't fix

**The dataset has to fit in RAM.** Still true in Redis Open Source, and it always
has been. Tiered storage is commercial-only; the managed SSD tiers add roughly
300 µs on tier hits.

The pricing exposes a constraint worth designing around: on ElastiCache,
**synchronous durability and data tiering are mutually exclusive.**

Cheap, durable, fast - pick two.

Shrinking the dataset is the only lever that doesn't force the choice, which is
the entire argument for the rest of this repo.

---

## The config that makes it a database

```ts
await applySafeEviction(redis, maxmemoryBytes);
```

```
appendonly yes
appendfsync everysec              # or `always` if you can't lose ~2 s
no-appendfsync-on-rewrite no      # ← audit this, it silently defeats `always`
maxmemory-policy volatile-ttl     # durable records carry no TTL
maxmemory-clients 5%              # default 0 = unlimited = a real outage path
```

And then run the proofs against *your* config, not ours.
