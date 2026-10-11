#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:9000}"
BUCKET="${S3_BUCKET:-bowl-data}"

# Ensure local volume directory exists so MinIO automatically mounts it as a bucket
mkdir -p "data/minio/${BUCKET}"

echo "Waiting for MinIO at ${ENDPOINT} to be ready..."
for i in {1..30}; do
  if curl -s -f "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
    echo "MinIO at ${ENDPOINT} is ready. Bucket '${BUCKET}' mounted successfully."
    exit 0
  fi
  sleep 1
done

echo "Error: Timed out waiting for MinIO at ${ENDPOINT}."
exit 1
