# Packing records, and compressing prose

> The reader already knows the field order. Stop telling it, 20 times per record.

---

## Copy this

```ts
import { schema, uuid, enum_, varint, flags } from './src/kit/index.ts';

const Span = schema({
  span_id:   uuid(),                                  // 16 B, not 36
  tool:      enum_(['web_search','bash','sql_query']), // 1 B
  dur_ms:    varint(),                                 // 1 B under 128
  bits:      flags(['ok','cache_hit','retried']),      // 3 bools -> 1 B
});

Span.encode(span);   // ~25 B.  The same object as JSON: ~250 B.
```

Add new fields at the **end** and bump `version`; old and new readers both keep
working. Never reorder or remove - see [changing a schema](#changing-a-schema-without-an-outage).

---

## Where the bytes go

A twenty-field agent run, as JSON:

```json
{"run_id":"3946f31a-da9d-02d9-a002-a80469567fb9","tenant_id":"c0ffee00-…
```

577 bytes. Of which:

- ~180 B - field names, repeated on every single record
- ~200 B - five UUIDs as 36 hex characters each, when they are 16 bytes of entropy
- ~60 B  - quotes, braces, colons, commas
- ~110 B - actual information

Encodings, same record:

| Encoding | Bytes | vs JSON |
|---|--:|--:|
| JSON | 577 | 1.0× |
| MessagePack | 488 | 1.2× |
| JSON + dictionary-deflate | 198 | 2.9× |
| **schema-packed** | **111** | **5.2×** |

MessagePack helps a little - it drops the quotes, braces and commas - but still
ships every field name, so it lands roughly halfway. Compression helps a lot *if*
you prime it with a dictionary (see below). Schema packing wins because it stores
the names zero times.

One counter-intuitive place MessagePack *loses*: inside a Redis stream. Streams
already deduplicate field names across entries sharing a fieldset, so a
field-per-attribute `XADD` cost 168.2 B/entry while the same span packed into a
single MessagePack field cost 250.9 B. Check what the container already does for
you before you optimize the payload.

---

## The DSL

```ts
import { schema, uuid, enum_, varint, svarint, u8, str, blob, flags } from './src/kit/index.ts';

const Run = schema({
  run_id:      uuid(),
  tenant_id:   uuid(),
  status:      enum_(['queued', 'running', 'awaiting_tool', 'succeeded', 'failed']),
  model:       enum_(['claude-opus-5', 'claude-sonnet-5', 'claude-haiku-4-5-20251001']),
  step_count:  varint(),
  cost_micros: varint(),
  bits:        flags(['cached', 'retried', 'billable']),
});

Run.encode(obj);      // Buffer
Run.decode(buf);      // obj
Run.describe();       // field table with typical sizes
```

| Type | Bytes | Use for |
|---|--:|---|
| `uuid()` | 16 | any UUID string - 2.25× off the top |
| `enum_([…])` | 1 | status, model, region, tool name. ≤255 members |
| `varint()` | 1–5 | counts, tokens, timestamps. 1 B under 128 |
| `svarint()` | 1–5 | **deltas** - zigzag keeps small negatives at 1 B |
| `u8()` | 1 | small bounded ints |
| `flags([…])` | 1 | up to 8 booleans. Eight JSON bools cost ~90 B |
| `str()` | 1 + len | the only variable-cost field. Keep them rare |
| `blob()` | 1 + len | raw bytes |

---

## Two tricks worth knowing

**Store timestamps as deltas.** An epoch second is a 5-byte varint. The
*difference* between `created_at` and `updated_at` is usually a 1–2 byte
`svarint`. On a record with four timestamps that is ~12 bytes for free:

```ts
const Span = schema({
  started_at:   varint(),     // absolute, 5 B
  finished_at:  svarint(),    // delta from started_at, usually 1–2 B
});
// encode: { started_at: t0, finished_at: t1 - t0 }
```

**Fold booleans and small enums into one byte.** `flags()` does eight. If you
also have a 0–7 retry counter, pack it into the spare bits yourself - see
`packSpan` in `src/lib/codec.ts`, which puts `ok`, `cache_hit` and a 3-bit
retry count into a single byte.

---

## Changing a schema without an outage

This is the part people skip and then regret.

**The rule: add fields at the END, never reorder, never remove.**

```ts
// v1, in production
const Run = schema({ run_id: uuid(), status: enum_([...]) });

// v2 - new field appended, version bumped
const Run = schema({
  run_id: uuid(),
  status: enum_([...]),
  retry_budget: varint(),        // ← new, at the end
}, { version: 2 });
```

A v2 reader decoding a v1 record stops when the buffer runs out and leaves
`retry_budget` undefined. A v1 reader decoding a v2 record reads the fields it
knows and ignores the trailing bytes. **Both directions work**, which is what
lets you deploy readers and writers in any order.

Verified in `src/kit/smoke.ts`:

```
✔ a v2 reader decodes a v1 record (trailing field undefined)
```

**What you cannot do:**

- Reorder fields. The reader positionally misinterprets everything after the
  swap, silently, with no error.
- Remove a field. Append a replacement and ignore the old one.
- Reorder or remove `enum_` members. Insert new members at the end only -
  the index *is* the wire format.
- Widen `flags()` past 8. Add a second `flags()` field.

`Run.versionOf(buf)` reads just the leading byte if you need to branch.

---

## When to compress instead

Schema packing and compression are **substitutes, not complements**. Once
packing has removed the redundancy, there is nothing left for a compressor:

```
packed              111 B
packed + zstd       111 B    (no change, sometimes worse)
packed + dict       113 B    (worse - frame overhead)
```

Compression wins where packing can't reach: **prose**.

| Transcript turn (~1.2 KB) | B/turn |
|---|--:|
| JSON in a LIST | 2,010 |
| + `list-compress-depth 1` (config only!) | 1,444 |
| per-turn zstd | 934 |
| **per-turn dictionary-deflate** | **595** |

`list-compress-depth 1` is worth remembering - Redis LZF-compresses interior
quicklist nodes and leaves the head and tail hot. Zero code change.

Note `list-max-listpack-size` defaults to **-2**, which is a *size* cap (8 KiB
per quicklist node), not an entry count. With ~1.2 KiB turns that is ~6 turns
per node. A positive value switches it to counting entries instead, and the two
regimes give materially different memory for the same data - an earlier version
of this benchmark had the wrong default here and reported 1,150 B for the row
above. Check which regime you are in before tuning it.

### Dictionary-primed compression

A 300-byte record has no history for a compressor to back-reference, so zstd
often makes it *bigger*. A preset dictionary hands the compressor a prebuilt
history - your field names, your enum values, your URL prefixes - so the very
first byte can be a back-reference.

```ts
import { makeDictionary, deflateDict, inflateDict } from './src/kit/index.ts';

const dict = makeDictionary(sampleRecords.map((r) => Buffer.from(JSON.stringify(r))));
const small = deflateDict(Buffer.from(JSON.stringify(record)), dict);
const back  = inflateDict(small, dict);
```

**Ship the dictionary as a versioned asset.** It is part of your data format: a
record compressed with dictionary v3 cannot be read without dictionary v3. Store
its id alongside the payload, or in the schema version byte.

zlib caps dictionaries at 32 KiB and uses the tail if you pass more.

---

## Reality check on our numbers

The compression figures here are measured on text Zipf-sampled from a
20,000-word vocabulary. That has *more* entropy than real prose - no syntax, no
phrase repetition, no shared sentence structure - so real transcripts should
compress **better** than these numbers, not worse.

An earlier version of this benchmark used a 60-word lexicon and reported 4.9:1
for dictionary compression. That number was an artifact of a dictionary that had
memorized the entire vocabulary. It is not in this repo any more.
