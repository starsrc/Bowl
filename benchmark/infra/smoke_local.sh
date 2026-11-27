#!/usr/bin/env bash
set -euo pipefail
export GOWORK=off

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.smoke.yaml"
SMOKE_PROJECT="${COMPOSE_PROJECT_NAME:-bowl-smoke}"
SMOKE_NET="${SMOKE_PROJECT}_bowl-smoke-net"
SMOKE_NVME_VOL="${SMOKE_PROJECT}_bench_nvme_data"
BENCH_IMAGE="${BENCH_IMAGE:-bowl-bench:latest}"
STORAGE_IMAGE_NAME="${STORAGE_IMAGE_NAME:-bowl-storage}"
TAG="${TAG:-latest}"
KEEP_SMOKE_ENV="${KEEP_SMOKE_ENV:-0}"

RUNNER_ARGS=("$@")
BENCH_CONTAINER_NAME="bowl-smoke-runner-$$"

log() {
  printf '[%s] [smoke-local] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

cleanup() {
  local exit_code=$?
  set +e
  docker rm -f "${BENCH_CONTAINER_NAME}" >/dev/null 2>&1 || true
  if [[ "${KEEP_SMOKE_ENV}" != "1" ]]; then
    log "Tearing down local smoke Docker Compose stack and volumes..."
    docker compose -p "${SMOKE_PROJECT}" -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
    docker volume rm -f "${SMOKE_NVME_VOL}" >/dev/null 2>&1 || true
  fi
  exit "${exit_code}"
}
trap cleanup EXIT INT TERM

log "Vendoring storage and client modules for offline container builds..."
go -C "${REPO_ROOT}/services/storage" mod vendor
go -C "${REPO_ROOT}/client" mod vendor

log "Building ${BENCH_IMAGE} from benchmark/Dockerfile.bench..."
docker build -t "${BENCH_IMAGE}" -f "${REPO_ROOT}/benchmark/Dockerfile.bench" "${REPO_ROOT}"

log "Starting storage-service via Docker Compose with named volumes (no VirtioFS bind mounts)..."
STORAGE_IMAGE_NAME="${STORAGE_IMAGE_NAME}" TAG="${TAG}" \
  docker compose -p "${SMOKE_PROJECT}" -f "${COMPOSE_FILE}" up -d --build --wait storage-service

docker volume create "${SMOKE_NVME_VOL}" >/dev/null

log "Running benchmark/runner.py --profile smoke-local inside ${BENCH_IMAGE}..."
set +e
docker run \
  --name "${BENCH_CONTAINER_NAME}" \
  --privileged \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --cap-add LINUX_IMMUTABLE \
  --security-opt apparmor:unconfined \
  --network "${SMOKE_NET}" \
  -v "${SMOKE_NVME_VOL}:/mnt/nvme" \
  -w /workspace \
  -e PYTHONPATH=/workspace \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTHONUNBUFFERED=1 \
  -e BOWL_BENCH_IN_CONTAINER=1 \
  "${BENCH_IMAGE}" \
  python3 /workspace/benchmark/runner.py \
    --profile smoke-local \
    --server-endpoint http://storage-service:9000 \
    --server-metrics-url http://storage-service:9090 \
    --nvme-root /mnt/nvme \
    ${RUNNER_ARGS[@]+"${RUNNER_ARGS[@]}"}
RUN_EXIT=$?
set -e

log "Copying smoke run artifacts back to ${REPO_ROOT}/benchmark/runs/..."
mkdir -p "${REPO_ROOT}/benchmark/runs"
docker cp "${BENCH_CONTAINER_NAME}:/workspace/benchmark/runs/." "${REPO_ROOT}/benchmark/runs/" 2>/dev/null || true

if [[ "${RUN_EXIT}" -ne 0 ]]; then
  log "ERROR: smoke-local run exited with code ${RUN_EXIT}."
  exit "${RUN_EXIT}"
fi

log "smoke-local run completed. Results saved under benchmark/runs/."
