# AGENTS.md

Guidance for coding agents working in this repo.

## What this is

`nibble` is a zero-dependency toolkit for cutting Redis RAM on agent workloads,
plus a doctor that audits and fixes a live instance, plus the benchmarks and
proofs that back every number.

Two hard rules define the project:

1. **Every number is measured, not estimated.** If you add a claim, add the code
   that produces it and commit the result under `results/`.
2. **The kit and the doctor have zero dependencies.** `git clone && node` is the
   whole setup. Only the benchmark and proof suite may use `devDependencies`.

## Setup

```bash
bash scripts/up.sh              # Redis 8.10 in Docker on :6399
node src/kit/smoke.ts           # ~30s, asserts every snippet in the docs
node doctor.mjs redis://127.0.0.1:6399
```

`REDIS_URL` overrides the target. Node >= 22 runs the TypeScript directly; there
is no build step and there must not be one.

## Layout

```
doctor.mjs         one file, zero deps, read-only unless --apply
src/kit/           the public toolkit (no dependencies, ever)
src/lib/           internals shared by benchmarks (may use deps)
src/bench/         12 cases, ~140 variants -> results/bench.json
src/proof/         6 adversarial proofs -> results/proof-*.json
docs/              use-case first; each page opens with a "Copy this" block
```

## Rules

**No em dashes.** Anywhere. Not in code, comments, docs, or commit messages.

**Comments explain why, not what.** Keep header blocks to a few lines. If a
comment restates the code, delete it.

**Never claim a number you did not measure.** The repo has been wrong four times
and each correction is documented rather than quietly patched. Follow that:
when you find an error, fix it, keep the wrong number visible, and say what it
taught you.

**Do not weaken a test to make it pass.** Two proofs currently encode real
constraints discovered by failure:
- `src/proof/cache.ts` found a lease race under 200-way concurrency
- `src/proof/reshard.ts` found that permanently stale clients cannot converge

If one of these goes red, the protocol is wrong, not the test.

**Zero dependencies in `src/kit/`.** If you need a Redis command, add it through
`src/kit/client.ts`. If you need msgpack or ioredis, you are in the wrong
directory.

## Adding a benchmark case

```ts
export const caseNN: BenchCase = {
  id: 'NN-thing', title: '...', question: 'what does this settle?', unit: 'record',
  variants: [
    { name: 'A · what we ship today', note: '...', load: async (r) => { ...; return N; } },
    { name: 'B · the candidate', note: '...', config: {...}, load: ..., encodingProbes: ['k:{0}'] },
  ],
};
```

Register it in `src/bench/run-all.ts`. `variants[0]` is the baseline. Two fields
carry the honesty:

- `caveat` marks a variant that answers a **different question** (lossy
  retention, probabilistic membership). Caveated rows are excluded from "best"
  rankings, or HyperLogLog wins every comparison by answering something else.
- `kind: 'sweep'` marks variants that are points on an axis rather than
  competing designs, so no ratios are computed between them.

Always set `encodingProbes`. A number without `OBJECT ENCODING` is unexplained,
and roughly half the results in this repo are "the encoding changed".

## Measurement

Use `sample()` from `src/lib/measure.ts`. It reports `used_memory` minus
client, AOF and replication buffers, after `MEMORY PURGE`.

**Do not use `used_memory_dataset`.** Redis counts the top-level key dictionary
as overhead, which is exactly what sharding attacks, so that metric reports the
largest optimization in the repo as zero.

Run each variant twice and keep the cheaper result. Allocator noise is
one-sided.

## Verify before you commit

```bash
node src/kit/smoke.ts        # kit
node doctor.mjs --fix        # dry run, must not crash
bash scripts/run-all.sh      # everything, ~40 min
```

The proofs exit non-zero on failure, so they work in CI as-is.

## Things that are true and easy to get wrong

- Redis 8.10 defaults: `hash-max-listpack-entries` is **512**, and
  `list-max-listpack-size` is **-2**, which is a size cap (8 KiB/node), not an
  entry count. Getting the second one wrong changed real measurements here.
- Hash templates and sharding are **mutually exclusive**. Templates dedupe field
  names across hashes sharing a schema; a shard's field names are unique ids, so
  templates make sharded layouts 20% worse.
- Encoding conversions are **one-way**. Raising a threshold does nothing to
  existing keys. Use `DUMP` + `RESTORE REPLACE` to re-encode.
- Shard counts must be **powers of two** so growth is a split, not a reshuffle.
- Everything that must be atomic together needs the same `{hash tag}`, or Redis
  Cluster rejects the script.
