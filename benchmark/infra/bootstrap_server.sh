#!/usr/bin/env bash
set -euo pipefail

NVME_ROOT="${NVME_ROOT:-/mnt/nvme}"
SLICE_NAME="${SLICE_NAME:-bench-target.slice}"
SLICE_CPU_QUOTA="${SLICE_CPU_QUOTA:-400%}"
SLICE_MEMORY_MAX="${SLICE_MEMORY_MAX:-16G}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() {
  printf '[%s] [bootstrap-server] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

wait_apt_lock() {
  while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do
    sleep 2
  done
}

log "Installing server runtime dependencies and Docker..."
wait_apt_lock
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq \
  docker.io \
  iperf3 \
  fio \
  strace \
  jq \
  curl \
  ca-certificates \
  python3 \
  iproute2 \
  psmisc \
  util-linux >/dev/null

systemctl enable --now docker

log "Stopping and removing any observability services (Prometheus/Loki/Alloy/Jaeger/Grafana)..."
for unit in prometheus loki alloy grafana-server jaeger jaeger-all-in-one node_exporter; do
  if systemctl list-unit-files "${unit}.service" >/dev/null 2>&1; then
    systemctl stop "${unit}.service" >/dev/null 2>&1 || true
    systemctl disable "${unit}.service" >/dev/null 2>&1 || true
  fi
done

if command -v docker >/dev/null 2>&1; then
  obs_ids="$(docker ps -aq --filter 'name=prometheus' --filter 'name=loki' --filter 'name=alloy' --filter 'name=jaeger' --filter 'name=grafana' 2>/dev/null || true)"
  if [[ -n "${obs_ids}" ]]; then
    # shellcheck disable=SC2086
    docker rm -f ${obs_ids} >/dev/null 2>&1 || true
  fi
fi

log "Configuring systemd parent slice /etc/systemd/system/${SLICE_NAME} (CPUQuota=${SLICE_CPU_QUOTA}, MemoryMax=${SLICE_MEMORY_MAX}, MemorySwapMax=0)..."
cat >"/etc/systemd/system/${SLICE_NAME}" <<EOF
[Unit]
Description=Bowl Benchmark v2 Shared Target Resource Slice
Before=slices.target

[Slice]
CPUAccounting=true
MemoryAccounting=true
IOAccounting=true
CPUQuota=${SLICE_CPU_QUOTA}
MemoryMax=${SLICE_MEMORY_MAX}
MemorySwapMax=0
EOF

systemctl daemon-reload
systemctl start "${SLICE_NAME}"

log "Preparing local NVMe target data directories under ${NVME_ROOT}..."
mkdir -p \
  "${NVME_ROOT}/bowl" \
  "${NVME_ROOT}/minio" \
  "${NVME_ROOT}/minio/juicefs" \
  "${NVME_ROOT}/minio/jfs-pg" \
  "${NVME_ROOT}/minio/jfs-redis" \
  "${NVME_ROOT}/pg" \
  "${NVME_ROOT}/redis" \
  "${NVME_ROOT}/drive9"
chmod 0777 \
  "${NVME_ROOT}/bowl" \
  "${NVME_ROOT}/minio" \
  "${NVME_ROOT}/minio/juicefs" \
  "${NVME_ROOT}/minio/jfs-pg" \
  "${NVME_ROOT}/minio/jfs-redis" \
  "${NVME_ROOT}/pg" \
  "${NVME_ROOT}/redis" \
  "${NVME_ROOT}/drive9"

log "Applying host network and file descriptor tuning..."
sysctl -qw net.ipv4.tcp_autocorking=0 2>/dev/null || true
ulimit -n 1048576 2>/dev/null || true

if [[ -f "${SCRIPT_DIR}/deploy_target.sh" ]]; then
  chmod +x "${SCRIPT_DIR}/deploy_target.sh"
  ln -sf "${SCRIPT_DIR}/deploy_target.sh" /usr/local/bin/deploy_target.sh
fi
if [[ -f "${SCRIPT_DIR}/env_check.sh" ]]; then
  chmod +x "${SCRIPT_DIR}/env_check.sh"
fi

log "Pre-pulling comparison backend container images (postgres:16-alpine, redis:7-alpine, minio)..."
docker pull -q postgres:16-alpine >/dev/null
docker pull -q redis:7-alpine >/dev/null
MINIO_PINNED="minio/minio:RELEASE.2025-02-28T09-55-16Z"
if docker pull -q "${MINIO_PINNED}" >/dev/null 2>&1; then
  docker tag "${MINIO_PINNED}" bowl-bench-minio:latest
else
  docker pull -q minio/minio:latest >/dev/null
  docker tag minio/minio:latest bowl-bench-minio:latest
fi

log "Server bootstrap complete."
