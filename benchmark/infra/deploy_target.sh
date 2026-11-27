#!/usr/bin/env bash
set -euo pipefail

NVME_ROOT="${NVME_ROOT:-/mnt/nvme}"
SLICE_NAME="${SLICE_NAME:-bench-target.slice}"
STATE_FILE="${STATE_FILE:-/run/bowl-bench-target.state}"
ENV_CACHE_FILE="${ENV_CACHE_FILE:-/run/bowl-bench-target.env}"

STORAGE_BIN="${STORAGE_BIN:-/usr/local/bin/storage-service}"
STORAGE_IMAGE="${STORAGE_IMAGE:-bowl-storage:latest}"
MINIO_IMAGE="${MINIO_IMAGE:-bowl-bench-minio:latest}"
PG_IMAGE="${PG_IMAGE:-postgres:16-alpine}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7-alpine}"

log() {
  printf '[%s] [deploy-target] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

docker_cgroup_parent() {
  local driver
  driver="$(docker info --format '{{.CgroupDriver}}' 2>/dev/null || echo 'systemd')"
  if [[ "${driver}" == "systemd" ]]; then
    echo "${SLICE_NAME}"
  else
    echo "/${SLICE_NAME}"
  fi
}

ensure_slice_ready() {
  if command -v systemctl >/dev/null 2>&1 && systemctl is-system-running >/dev/null 2>&1 || systemctl status "${SLICE_NAME}" >/dev/null 2>&1; then
    systemctl start "${SLICE_NAME}" >/dev/null 2>&1 || true
  fi
}

stop_all() {
  log "Stopping active storage targets..."
  for unit in bowl-bench-storage.service bowl-bench-drive9.service; do
    if command -v systemctl >/dev/null 2>&1; then
      systemctl stop "${unit}" >/dev/null 2>&1 || true
      systemctl reset-failed "${unit}" >/dev/null 2>&1 || true
    fi
  done

  if command -v docker >/dev/null 2>&1; then
    for ctr in bowl-bench-storage bowl-bench-minio bowl-bench-pg bowl-bench-redis bowl-bench-drive9; do
      if docker ps -aq --filter "name=^/${ctr}$" | grep -q .; then
        docker rm -f "${ctr}" >/dev/null 2>&1 || true
      fi
    done
  fi
  rm -f "${STATE_FILE}"
}

wait_http_ok() {
  local url="$1"
  local timeout_s="${2:-30}"
  local i
  for ((i = 1; i <= timeout_s; i++)); do
    if curl -fsS --max-time 2 "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  log "ERROR: Timed out waiting for ${url}"
  return 1
}

wait_tcp_port() {
  local port="$1"
  local timeout_s="${2:-30}"
  local i
  for ((i = 1; i <= timeout_s; i++)); do
    if ss -tln | grep -q ":${port} "; then
      return 0
    fi
    sleep 1
  done
  log "ERROR: Timed out waiting for TCP port ${port}"
  return 1
}

start_bowl() {
  local target="$1"
  local data_dir="${STORAGE_DATA_DIR:-${NVME_ROOT}/bowl}"
  local gc_interval="${STORAGE_GC_INTERVAL:-30s}"
  local entry_ttl="${STORAGE_ENTRY_TIMEOUT_SECONDS:-1.0}"
  local attr_ttl="${STORAGE_ATTR_TIMEOUT_SECONDS:-1.0}"
  local dirent_ttl="${STORAGE_DIR_ENTRY_TIMEOUT_SECONDS:-1.0}"
  local profiling="${PROFILING:-0}"

  local pprof_enabled="false"
  local pprof_mutex="0"
  local pprof_block="0"
  local otel_enabled="false"
  if [[ "${profiling}" == "1" ]]; then
    pprof_enabled="true"
    pprof_mutex="${PPROF_MUTEX_FRACTION:-5}"
    pprof_block="${PPROF_BLOCK_RATE:-10000}"
  fi

  if [[ "${RESET_DATA:-0}" == "1" ]]; then
    rm -rf "${data_dir:?}"/*
  fi
  mkdir -p "${data_dir}"
  chmod 0777 "${data_dir}"

  cat >"${ENV_CACHE_FILE}" <<EOF
STORAGE_DATA_DIR=${data_dir}
STORAGE_GC_INTERVAL=${gc_interval}
STORAGE_ENTRY_TIMEOUT_SECONDS=${entry_ttl}
STORAGE_ATTR_TIMEOUT_SECONDS=${attr_ttl}
STORAGE_DIR_ENTRY_TIMEOUT_SECONDS=${dirent_ttl}
PROFILING=${profiling}
PPROF_MUTEX_FRACTION=${pprof_mutex}
PPROF_BLOCK_RATE=${pprof_block}
EOF

  ensure_slice_ready

  if [[ -x "${STORAGE_BIN}" ]] && command -v systemd-run >/dev/null 2>&1 && [[ -d /run/systemd/system ]]; then
    log "Starting ${target} via systemd-run in ${SLICE_NAME} (gc=${gc_interval}, ttl=${entry_ttl}/${attr_ttl}/${dirent_ttl}, profiling=${profiling})..."
    systemd-run \
      --unit=bowl-bench-storage.service \
      --slice="${SLICE_NAME}" \
      --property=LimitNOFILE=1048576 \
      --property=Restart=no \
      --setenv="SERVER_HOST=0.0.0.0" \
      --setenv="SERVER_PORT=9000" \
      --setenv="INTERNAL_SERVER_HOST=0.0.0.0" \
      --setenv="INTERNAL_SERVER_PORT=9090" \
      --setenv="SERVER_READ_MAX_BYTES=16777216" \
      --setenv="STORAGE_DATA_DIR=${data_dir}" \
      --setenv="STORAGE_ATIME_MODE=noatime" \
      --setenv="STORAGE_ENTRY_TIMEOUT_SECONDS=${entry_ttl}" \
      --setenv="STORAGE_ATTR_TIMEOUT_SECONDS=${attr_ttl}" \
      --setenv="STORAGE_DIR_ENTRY_TIMEOUT_SECONDS=${dirent_ttl}" \
      --setenv="STORAGE_OPEN_KEEP_CACHE=false" \
      --setenv="STORAGE_OPEN_DIR_CACHE=false" \
      --setenv="STORAGE_GC_INTERVAL=${gc_interval}" \
      --setenv="LOG_LEVEL=info" \
      --setenv="LOG_FORMAT=json" \
      --setenv="LOG_ADD_SOURCE=false" \
      --setenv="METRICS_ENABLED=true" \
      --setenv="OTEL_ENABLED=${otel_enabled}" \
      --setenv="PPROF_ENABLED=${pprof_enabled}" \
      --setenv="PPROF_MUTEX_FRACTION=${pprof_mutex}" \
      --setenv="PPROF_BLOCK_RATE=${pprof_block}" \
      "${STORAGE_BIN}" >/dev/null
  else
    local cg_parent
    cg_parent="$(docker_cgroup_parent)"
    log "Starting ${target} via Docker in ${cg_parent} (--network host)..."
    docker run -d \
      --name bowl-bench-storage \
      --cgroup-parent="${cg_parent}" \
      --network host \
      --ulimit nofile=1048576:1048576 \
      -v "${data_dir}:/var/lib/bowl/data" \
      -e SERVER_HOST=0.0.0.0 \
      -e SERVER_PORT=9000 \
      -e INTERNAL_SERVER_HOST=0.0.0.0 \
      -e INTERNAL_SERVER_PORT=9090 \
      -e SERVER_READ_MAX_BYTES=16777216 \
      -e STORAGE_DATA_DIR=/var/lib/bowl/data \
      -e STORAGE_ATIME_MODE=noatime \
      -e STORAGE_ENTRY_TIMEOUT_SECONDS="${entry_ttl}" \
      -e STORAGE_ATTR_TIMEOUT_SECONDS="${attr_ttl}" \
      -e STORAGE_DIR_ENTRY_TIMEOUT_SECONDS="${dirent_ttl}" \
      -e STORAGE_OPEN_KEEP_CACHE=false \
      -e STORAGE_OPEN_DIR_CACHE=false \
      -e STORAGE_GC_INTERVAL="${gc_interval}" \
      -e LOG_LEVEL=info \
      -e LOG_FORMAT=json \
      -e LOG_ADD_SOURCE=false \
      -e METRICS_ENABLED=true \
      -e OTEL_ENABLED="${otel_enabled}" \
      -e PPROF_ENABLED="${pprof_enabled}" \
      -e PPROF_MUTEX_FRACTION="${pprof_mutex}" \
      -e PPROF_BLOCK_RATE="${pprof_block}" \
      "${STORAGE_IMAGE}" >/dev/null
  fi

  wait_http_ok "http://127.0.0.1:9090/readyz" 30
  echo "${target}" >"${STATE_FILE}"
  log "${target} is ready on :9000 (admin :9090)."
}

start_minio() {
  local bucket="$1"
  local cg_parent="$2"
  local minio_dir="${NVME_ROOT}/minio"
  local minio_port="${JFS_MINIO_PORT:-9000}"

  if [[ "${RESET_DATA:-0}" == "1" ]]; then
    rm -rf "${minio_dir:?}"/*
  fi
  mkdir -p "${minio_dir}/juicefs" "${minio_dir}/${bucket}"
  chmod 0777 "${minio_dir}" "${minio_dir}/juicefs" "${minio_dir}/${bucket}"

  local img="${MINIO_IMAGE}"
  if ! docker image inspect "${img}" >/dev/null 2>&1; then
    img="minio/minio:latest"
  fi

  docker run -d \
    --name bowl-bench-minio \
    --cgroup-parent="${cg_parent}" \
    --network host \
    --ulimit nofile=1048576:1048576 \
    -v "${minio_dir}:/data" \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    "${img}" server /data --address ":${minio_port}" --console-address :9091 >/dev/null

  wait_http_ok "http://127.0.0.1:${minio_port}/minio/health/live" 30
}

start_jfs_pg_minio() {
  local cg_parent
  cg_parent="$(docker_cgroup_parent)"
  local pg_dir="${NVME_ROOT}/pg"
  local minio_port="${JFS_MINIO_PORT:-9000}"

  if [[ "${RESET_DATA:-0}" == "1" ]]; then
    rm -rf "${pg_dir:?}"/*
  fi
  mkdir -p "${pg_dir}"
  chmod 0777 "${pg_dir}"

  printf 'JFS_MINIO_PORT=%s\n' "${minio_port}" >"${ENV_CACHE_FILE}"
  ensure_slice_ready
  log "Starting jfs_pg_minio (PostgreSQL 16 + MinIO :${minio_port}) inside ${cg_parent} (--network host)..."
  start_minio "jfs-pg" "${cg_parent}"

  docker run -d \
    --name bowl-bench-pg \
    --cgroup-parent="${cg_parent}" \
    --network host \
    --ulimit nofile=1048576:1048576 \
    -v "${pg_dir}:/var/lib/postgresql/data" \
    -e POSTGRES_USER=juicefs \
    -e POSTGRES_PASSWORD=juicefs \
    -e POSTGRES_DB=juicefs \
    "${PG_IMAGE}" \
    postgres \
      -c listen_addresses='*' \
      -c max_connections=300 \
      -c shared_buffers=2GB \
      -c synchronous_commit=on \
      -c fsync=on >/dev/null

  local i
  for ((i = 1; i <= 40; i++)); do
    if docker exec bowl-bench-pg pg_isready -U juicefs -d juicefs >/dev/null 2>&1; then
      echo "jfs_pg_minio" >"${STATE_FILE}"
      log "jfs_pg_minio is ready (PG :5432, MinIO :${minio_port})."
      return 0
    fi
    sleep 1
  done
  log "ERROR: PostgreSQL failed to become ready."
  docker logs --tail 30 bowl-bench-pg >&2 || true
  return 1
}

start_jfs_redis_minio() {
  local cg_parent
  cg_parent="$(docker_cgroup_parent)"
  local redis_dir="${NVME_ROOT}/redis"
  local appendfsync="${REDIS_APPENDFSYNC:-everysec}"
  local minio_port="${JFS_MINIO_PORT:-9000}"

  if [[ "${RESET_DATA:-0}" == "1" ]]; then
    rm -rf "${redis_dir:?}"/*
  fi
  mkdir -p "${redis_dir}"
  chmod 0777 "${redis_dir}"

  printf 'REDIS_APPENDFSYNC=%s\nJFS_MINIO_PORT=%s\n' "${appendfsync}" "${minio_port}" >"${ENV_CACHE_FILE}"
  ensure_slice_ready
  log "Starting jfs_redis_minio (Redis 7 appendfsync=${appendfsync} + MinIO :${minio_port}) inside ${cg_parent} (--network host)..."
  start_minio "jfs-redis" "${cg_parent}"

  docker run -d \
    --name bowl-bench-redis \
    --cgroup-parent="${cg_parent}" \
    --network host \
    --ulimit nofile=1048576:1048576 \
    -v "${redis_dir}:/data" \
    "${REDIS_IMAGE}" \
    redis-server \
      --bind 0.0.0.0 \
      --protected-mode no \
      --appendonly yes \
      --appendfsync "${appendfsync}" \
      --dir /data >/dev/null

  local i
  for ((i = 1; i <= 30; i++)); do
    if docker exec bowl-bench-redis redis-cli ping 2>/dev/null | grep -q PONG; then
      echo "jfs_redis_minio" >"${STATE_FILE}"
      log "jfs_redis_minio is ready (Redis :6379, MinIO :${minio_port})."
      return 0
    fi
    sleep 1
  done
  log "ERROR: Redis failed to become ready."
  docker logs --tail 30 bowl-bench-redis >&2 || true
  return 1
}

start_drive9() {
  local d9_bin="${DRIVE9_SERVER_BIN:-/usr/local/bin/drive9-server}"
  if [[ ! -x "${d9_bin}" ]]; then
    if [[ -n "${DRIVE9_REPO:-}" && -x "${DRIVE9_REPO}/drive9-server" ]]; then
      d9_bin="${DRIVE9_REPO}/drive9-server"
    else
      log "ERROR: drive9-server binary not found (set DRIVE9_REPO or install /usr/local/bin/drive9-server)."
      return 1
    fi
  fi
  local d9_dir="${NVME_ROOT}/drive9"
  if [[ "${RESET_DATA:-0}" == "1" ]]; then
    rm -rf "${d9_dir:?}"/*
  fi
  mkdir -p "${d9_dir}"
  ensure_slice_ready
  log "Starting optional drive9 target in ${SLICE_NAME}..."
  systemd-run \
    --unit=bowl-bench-drive9.service \
    --slice="${SLICE_NAME}" \
    --property=LimitNOFILE=1048576 \
    --setenv="DRIVE9_DATA_DIR=${d9_dir}" \
    "${d9_bin}" --listen 0.0.0.0:9009 --data-dir "${d9_dir}" >/dev/null
  wait_tcp_port 9009 30
  echo "drive9" >"${STATE_FILE}"
  log "drive9 is ready on :9009."
}

start_target() {
  local target="$1"
  stop_all
  case "${target}" in
    bowl_staged|bowl_sync|bowl)
      start_bowl "${target}"
      ;;
    jfs_pg_minio)
      start_jfs_pg_minio
      ;;
    jfs_redis_minio)
      start_jfs_redis_minio
      ;;
    drive9)
      start_drive9
      ;;
    *)
      log "ERROR: Unknown target '${target}'. Expected: bowl_staged, bowl_sync, jfs_pg_minio, jfs_redis_minio, drive9."
      return 1
      ;;
  esac
}

restart_cold() {
  local target="${1:-}"
  if [[ -z "${target}" && -f "${STATE_FILE}" ]]; then
    target="$(cat "${STATE_FILE}")"
  fi
  if [[ -z "${target}" ]]; then
    log "ERROR: restart-cold requires <target> when no active target is recorded."
    return 1
  fi

  if [[ -f "${ENV_CACHE_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_CACHE_FILE}"
  fi

  stop_all
  sync
  if [[ -w /proc/sys/vm/drop_caches ]]; then
    echo 3 > /proc/sys/vm/drop_caches
  fi
  RESET_DATA=0 start_target "${target}"
}

read_cgroup_rbytes() {
  local mode="${1:-}"
  local rbytes=0
  local wbytes=0
  local source="none"

  local io_stat="/sys/fs/cgroup/${SLICE_NAME}/io.stat"
  if [[ -r "${io_stat}" ]]; then
    read -r rbytes wbytes < <(awk '
      {
        for (i = 1; i <= NF; i++) {
          if ($i ~ /^rbytes=/) { split($i, a, "="); rb += a[2] }
          if ($i ~ /^wbytes=/) { split($i, a, "="); wb += a[2] }
        }
      }
      END { printf "%.0f %.0f\n", rb + 0, wb + 0 }
    ' "${io_stat}")
    source="${io_stat}"
  fi

  if [[ "${rbytes}" -eq 0 && -f /etc/bowl_nvme_dev ]]; then
    local dev_name
    dev_name="$(basename "$(cat /etc/bowl_nvme_dev)")"
    if [[ -r /proc/diskstats ]]; then
      read -r rbytes wbytes < <(awk -v d="${dev_name}" '
        $3 == d { printf "%.0f %.0f\n", $6 * 512, $10 * 512; found=1 }
        END { if (!found) print "0 0" }
      ' /proc/diskstats)
      source="/proc/diskstats:${dev_name}"
    fi
  fi

  if [[ "${mode}" == "--json" ]]; then
    printf '{"rbytes":%s,"wbytes":%s,"source":"%s"}\n' "${rbytes}" "${wbytes}" "${source}"
  else
    printf '%s\n' "${rbytes}"
  fi
}

show_status() {
  local active="none"
  [[ -f "${STATE_FILE}" ]] && active="$(cat "${STATE_FILE}")"

  local obs_count=0
  if command -v docker >/dev/null 2>&1; then
    obs_count="$(docker ps --format '{{.Names}}' 2>/dev/null | grep -Eic 'prometheus|loki|alloy|jaeger|grafana' || true)"
  fi

  local slice_active="false"
  if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "${SLICE_NAME}" 2>/dev/null; then
    slice_active="true"
  fi

  local readyz_code="0"
  readyz_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:9090/readyz 2>/dev/null || echo 0)"

  local io_json
  io_json="$(read_cgroup_rbytes --json)"

  printf '{"active_target":"%s","slice":"%s","slice_active":%s,"observability_containers":%s,"bowl_readyz_http":%s,"io":%s}\n' \
    "${active}" "${SLICE_NAME}" "${slice_active}" "${obs_count}" "${readyz_code}" "${io_json}"
}

cmd="${1:-}"
shift || true

case "${cmd}" in
  start)
    [[ $# -ge 1 ]] || { log "Usage: $0 start <target>"; exit 2; }
    start_target "$1"
    ;;
  stop)
    stop_all
    ;;
  restart-cold)
    restart_cold "${1:-}"
    ;;
  cgroup-rbytes)
    read_cgroup_rbytes "${1:-}"
    ;;
  status)
    show_status
    ;;
  *)
    log "Usage: $0 {start <target>|stop|restart-cold <target>|cgroup-rbytes [--raw]|status}"
    exit 2
    ;;
esac
