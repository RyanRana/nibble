#!/usr/bin/env bash
# Everything, in dependency order. ~40 minutes on a laptop.
set -euo pipefail
cd "$(dirname "$0")/.."

bash scripts/up.sh
node src/kit/smoke.ts
node --max-old-space-size=8192 src/bench/run-all.ts
node --max-old-space-size=8192 src/bench/vector-rank-study.ts
node src/proof/integrity.ts
node src/proof/query.ts
node src/proof/eviction.ts
node src/proof/cache.ts
node src/proof/durability.ts
node src/proof/capacity.ts   # reads results/bench.json — must run last
