#!/bin/bash
set -euo pipefail

# Log to stderr so messages stay ordered with Python logging on the same stream
# in Docker/ECS (awslogs captures container stdout+stderr).

# Start Redis server in the background
redis-server /etc/redis/redis.conf --daemonize yes

# Wait for Redis to be ready
until redis-cli ping > /dev/null 2>&1; do
  echo "[spine-startup] Waiting for Redis..." >&2
  sleep 1
done
echo "[spine-startup] Redis is ready." >&2

# Optional: pull operator config from S3 (boto3; no AWS CLI). Off when unset.
if [ -n "${SPINE_CONFIG_S3_URI:-}" ]; then
  SYNC_TARGET="${CONFIG_PATH:-/config}"
  mkdir -p "${SYNC_TARGET}"
  echo "[spine-startup] Pulling config from ${SPINE_CONFIG_S3_URI} -> ${SYNC_TARGET}" >&2
  python -m scripts.s3_config_pull "${SPINE_CONFIG_S3_URI}" "${SYNC_TARGET}"
  export CONFIG_PATH="${SYNC_TARGET}"
  echo "[spine-startup] S3 config pull finished." >&2
else
  echo "[spine-startup] SPINE_CONFIG_S3_URI unset; skipping S3 pull." >&2
fi

echo "[spine-startup] Starting python -m src.main ..." >&2
exec python -m src.main "$@" 