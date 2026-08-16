# Agent runs, sessions, checkpoints

> One key per record is the expensive way to store records. This is the biggest single win in nibble - 15.4×.

---

## Copy this

```ts
import { connect, applyKitConfig, Pouch, schema, uuid, enum_, varint } from './src/kit/index.ts';

const redis = await connect();
await applyKitConfig(redis, { shardWidth: 124 });

const Run = schema({ run_id: uuid(), status: enum_(['queued','running','done']), steps: varint() });

const runs = new Pouch(redis, {
  prefix: 'run', capacity: 2_000_000, width: 124,
  encode: Run.encode, decode: Run.decode,
});

await runs.set(id, run);
await runs.get(id);
await runs.mget(ids);   // grouped by shard - one HMGET per shard, not per id
```

**~70 B/record**, down from 401 B as a JSON key per run. The rest of this page is
why, and what it costs you.

---

## The tax

A Redis top-level key is not a pointer to your data. It is:

```
dictEntry            ~24 B     the hashtable entry
redisObject (robj)   ~16 B     type, encoding, refcount, LRU/LFU bits
SDS of the key name  3 B hdr + strlen + 1
hashtable slack      ~8 B      amortized, depends on load factor
                     ───────
                     60–90 B   before a single byte of your record
```

Store two million agent runs as `run:<uuid>` and you have bought ~160 MB of
bookkeeping. The runs themselves, packed, are ~140 MB. **You are paying more for
the filing cabinet than the files.**

Measured on 5,000 twenty-field agent runs:

| Layout | B/run | Keys |
|---|--:|--:|
| a key per field (`run:{id}:{field}`) | 2,248 | 100,000 |
| one hash per run, forced to `hashtable` | 1,142 | 5,000 |
| one hash per run, `listpack` | 729 | 5,000 |
| one hash per run + 8.10 templates | 409 | 5,000 |
| one `STRING` per run, packed binary | 217 | 5,000 |
| **sharded hash, 128/shard, packed** | **147** | **40** |

The last row is 15.3× the first, and the difference between the last two rows -
217 vs 147 - is *entirely* per-key overhead. Same bytes of record.

---

## How it works

Put N records inside one hash. Field = record id, value = packed record.

```ts
const runs = new Pouch(redis, {
  prefix: 'run',
  capacity: 2_000_000,   // shard count derives from this
  width: 124,            // records per shard - see tuning.md
  encode: Run.encode,
  decode: Run.decode,
});

await runs.set(id, run);         // HSET run:{7} <16-byte id> <packed>
await runs.get(id);              // HGET run:{7} <16-byte id>
await runs.mget([a, b, c]);      // grouped by shard, one HMGET each
```

Routing is FNV-1a over the id, modulo the shard count. Stable across processes,
restarts, and languages, so a Python worker and a Node worker agree on where a
record lives.

Field names are the id's **16 raw bytes** when it's a UUID, not 36 hex
characters. That is 20 bytes per record for free.

---

## The braces are load-bearing

```
run:{7}
    ↑ ↑
```

That's a Redis Cluster **hash tag**. Only the text between braces is hashed to a
slot, so `run:{7}`, `idx:{7}:status:running`, and `evt:{7}` all land on the same
node.

Why you care: Redis Cluster rejects multi-key operations across slots. Without
hash tags you cannot write a Lua script that updates a record *and* its
secondary indexes atomically - and unmaintained indexes are how a cheap layout
becomes a wrong layout.

So the sharded layout isn't just cheaper, it's what makes server-side atomicity
possible in cluster mode. Measured: 64 workers racing, read-modify-write
produced **69 invariant violations**, the Lua version produced **zero**.

→ [04 · primary database](production.md#atomicity)

---

## What it costs you

Be honest about this before you adopt it.

**Per-key TTL.** Gone - a shard is one key. Use `ShardedCache` and `HEXPIRE` for
per-field expiry instead. (Redis ≥ 7.4.)

**Per-key eviction.** A shard evicts as a unit, taking ~124 records with it.
Under `volatile-ttl` with no TTL on the shard this never happens, which is the
configuration you want anyway.

**`FT.SEARCH`.** The Query Engine indexes keys *by prefix* and cannot see inside
a hash. Sharding and the query engine are straightforwardly incompatible. The
fix is a hybrid layout - fat record sharded, thin index document per record -
measured at 2.04× cheaper than indexing everything.

→ [04 · primary database](production.md#querying)

**`OBJECT ENCODING` discipline.** A shard must stay `listpack`. If it converts
to `hashtable` you lose about a quarter of the win:

```
128/shard, listpack    147 B/run
1024/shard, hashtable  180 B/run
```

`applyKitConfig()` sets `hash-max-listpack-entries` to `width × 2` for exactly
this reason. If you change `width`, call it again.

**Lookup is a linear scan.** A listpack has no index; `HGET` walks it. At 124
entries that is far cheaper than the cache miss you avoided. At 10,000 entries
it is not. Do not set `width` to 10,000 and then act surprised.

---

## The trap: templates and shards don't mix

Redis 8.10 added `template-listpack`, which stores field names once across
hashes that share a schema. It is excellent - **729 → 409 B/run** - for
one-hash-per-record layouts.

It is actively harmful here. A shard's field names are *unique record ids*, so
there's no schema to share and the template is pure overhead:

```
templates off, width 124    70.1 B/record    listpack
templates on,  width 124    84.4 B/record    template-listpack   ← 20% worse
```

The two biggest wins available are mutually exclusive. Sharding wins (147 vs
409), so `applyKitConfig()` explicitly sets
`hash-min-template-entries 0`.

Turn templates on only for hashes you have deliberately chosen *not* to shard.

---

## Sizing

```
shards = ceil(capacity / width)
```

Pick `capacity` generously - over-provisioning shards costs one key each
(~90 B), under-provisioning makes every shard a longer linear scan.

At 2M records and width 124: 16,130 shards, ~1.4 MB of key overhead total. That
is the entire tax, versus ~160 MB unsharded.
