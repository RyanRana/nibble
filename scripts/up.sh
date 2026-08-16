#!/usr/bin/env bash
# Start the benchmark Redis instance.
#
# Persistence is deliberately OFF here: this instance exists to be measured, and
# an AOF buffer or a background save would contaminate `used_memory` samples.
# The durability proofs start their own instances with their own settings.
set -euo pipefail

NAME=${NAME:-redops-redis}
PORT=${PORT:-6399}
IMAGE=${IMAGE:-redis:8-alpine}

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p "${PORT}:6379" "$IMAGE" \
  redis-server --save '' --appendonly no --maxmemory 0 >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$NAME" redis-cli PING >/dev/null 2>&1; then
    echo "redis up on :${PORT}  ($(docker exec "$NAME" redis-cli INFO server | grep -o 'redis_version:[^\r]*'))"
    exit 0
  fi
  sleep 0.5
done
echo "redis failed to start" >&2
exit 1
