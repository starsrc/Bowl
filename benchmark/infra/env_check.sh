#!/usr/bin/env bash
set -euo pipefail

NVME_ROOT="${NVME_ROOT:-/mnt/nvme}"
SERVER_HOST="${SERVER_HOST:-}"
SERVER_SSH="${SERVER_SSH:-}"
OUT_FILE=""
SKIP_FIO="${SKIP_FIO:-0}"
SKIP_NET="${SKIP_NET:-0}"
SKIP_MINIO_FSYNC="${SKIP_MINIO_FSYNC:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nvme-root) NVME_ROOT="$2"; shift 2 ;;
    --server-host) SERVER_HOST="$2"; shift 2 ;;
    --server-ssh) SERVER_SSH="$2"; shift 2 ;;
    --out) OUT_FILE="$2"; shift 2 ;;
    --skip-fio) SKIP_FIO=1; shift ;;
    --skip-net) SKIP_NET=1; shift ;;
    --skip-minio-fsync) SKIP_MINIO_FSYNC=1; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

log() {
  printf '[%s] [env-check] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=8)

remote_exec() {
  local cmd="$1"
  if [[ -n "${SERVER_SSH}" ]]; then
    # shellcheck disable=SC2086
    ssh "${SSH_OPTS[@]}" ${SERVER_SSH} "${cmd}"
  else
    bash -lc "${cmd}"
  fi
}

check_obs_containers() {
  local local_obs="[]"
  local server_obs="[]"
  if command -v docker >/dev/null 2>&1; then
    local_obs="$(docker ps --format '{{.Names}}' 2>/dev/null | grep -Ei 'prometheus|loki|alloy|jaeger|grafana' | jq -R . | jq -s . || echo '[]')"
  fi
  if [[ -n "${SERVER_SSH}" ]]; then
    server_obs="$(remote_exec "if command -v docker >/dev/null 2>&1; then docker ps --format '{{.Names}}' 2>/dev/null | grep -Ei 'prometheus|loki|alloy|jaeger|grafana' || true; fi" | jq -R . | jq -s . || echo '[]')"
  fi
  jq -n --argjson local_list "${local_obs}" --argjson server_list "${server_obs}" \
    '{local: $local_list, server: $server_list, clean: (($local_list | length) == 0 and ($server_list | length) == 0)}'
}

measure_nvme_fio() {
  if [[ "${SKIP_FIO}" == "1" ]] || ! command -v fio >/dev/null 2>&1; then
    echo '{"skipped": true}'
    return 0
  fi
  local dir="${NVME_ROOT}/env_check_fio"
  rm -rf "${dir}"
  mkdir -p "${dir}"

  local seq_w_bps seq_r_bps rand_w_iops rand_r_iops fsync_json fsync_iops fsync_p99_us
  seq_w_bps="$(fio --name=ec_seq_w --directory="${dir}" --rw=write --bs=4M --size=512M --direct=1 --end_fsync=1 --output-format=json 2>/dev/null | sed -n '/^{/,$p' | jq -r '.jobs[0].write.bw_bytes // 0')"
  seq_r_bps="$(fio --name=ec_seq_r --directory="${dir}" --rw=read --bs=4M --size=512M --direct=1 --output-format=json 2>/dev/null | sed -n '/^{/,$p' | jq -r '.jobs[0].read.bw_bytes // 0')"
  rand_w_iops="$(fio --name=ec_rand_w --directory="${dir}" --ioengine=libaio --rw=randwrite --bs=4k --size=256M --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --group_reporting --output-format=json 2>/dev/null | sed -n '/^{/,$p' | jq -r '.jobs[0].write.iops // 0')"
  rand_r_iops="$(fio --name=ec_rand_r --directory="${dir}" --ioengine=libaio --rw=randread --bs=4k --size=256M --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --group_reporting --output-format=json 2>/dev/null | sed -n '/^{/,$p' | jq -r '.jobs[0].read.iops // 0')"
  fsync_json="$(fio --name=ec_fsync --directory="${dir}" --ioengine=psync --rw=write --bs=4k --size=32M --numjobs=1 --direct=1 --fsync=1 --runtime=5 --time_based=1 --lat_percentiles=1 --output-format=json 2>/dev/null | sed -n '/^{/,$p')"
  fsync_iops="$(echo "${fsync_json}" | jq -r '.jobs[0].write.iops // 0')"
  fsync_p99_us="$(echo "${fsync_json}" | jq -r '((.jobs[0].sync.lat_ns.percentile["99.000000"] // 0) + (.jobs[0].write.clat_ns.percentile["99.000000"] // 0)) / 1000.0')"
  rm -rf "${dir}"

  jq -n \
    --argjson seq_w "${seq_w_bps:-0}" \
    --argjson seq_r "${seq_r_bps:-0}" \
    --argjson rand_w "${rand_w_iops:-0}" \
    --argjson rand_r "${rand_r_iops:-0}" \
    --argjson fsync_iops "${fsync_iops:-0}" \
    --argjson fsync_p99 "${fsync_p99_us:-0}" \
    '{
      skipped: false,
      seq_write_mib_s: (($seq_w / 1048576.0) * 100 | round / 100),
      seq_read_mib_s: (($seq_r / 1048576.0) * 100 | round / 100),
      randwrite_4k_iops: ($rand_w | round),
      randread_4k_iops: ($rand_r | round),
      fsync_4k_iops: ($fsync_iops | round),
      fsync_4k_p99_us: ($fsync_p99 * 10 | round / 10)
    }'
}

measure_network() {
  if [[ "${SKIP_NET}" == "1" || -z "${SERVER_HOST}" ]]; then
    echo '{"skipped": true}'
    return 0
  fi

  local ping_line ping_avg_ms iperf_bps iperf_gbps started_iperf=0
  ping_line="$(ping -c 20 -i 0.05 "${SERVER_HOST}" 2>/dev/null | tail -n 1 || true)"
  ping_avg_ms="$(echo "${ping_line}" | awk -F'/' '{if (NF >= 5) print $5; else print "0"}')"

  if [[ -n "${SERVER_SSH}" ]]; then
    remote_exec "pkill -x iperf3 2>/dev/null || true; iperf3 -s -D" >/dev/null 2>&1 && started_iperf=1 || true
    sleep 1
  fi

  iperf_bps="0"
  if command -v iperf3 >/dev/null 2>&1; then
    iperf_bps="$(iperf3 -c "${SERVER_HOST}" -t 5 -J 2>/dev/null | jq -r '.end.sum_received.bits_per_second // 0' || echo 0)"
  fi

  if [[ "${started_iperf}" == "1" ]]; then
    remote_exec "pkill -x iperf3 2>/dev/null || true" >/dev/null 2>&1 || true
  fi

  iperf_gbps="$(awk -v bps="${iperf_bps:-0}" 'BEGIN {printf "%.2f", bps / 1e9}')"
  jq -n \
    --arg host "${SERVER_HOST}" \
    --arg ping_raw "${ping_line}" \
    --argjson ping_avg "${ping_avg_ms:-0}" \
    --argjson gbps "${iperf_gbps:-0}" \
    '{skipped: false, server_host: $host, ping_rtt_raw: $ping_raw, ping_avg_ms: $ping_avg, iperf3_gbps: $gbps}'
}

verify_minio_fsync() {
  if [[ "${SKIP_MINIO_FSYNC}" == "1" ]]; then
    echo '{"checked": false, "reason": "skipped"}'
    return 0
  fi

  local target_host="${SERVER_HOST:-127.0.0.1}"
  local minio_port="${JFS_MINIO_PORT:-9000}"
  local minio_pid=""
  minio_pid="$(remote_exec "docker inspect -f '{{.State.Pid}}' bowl-bench-minio 2>/dev/null || pgrep -xo minio 2>/dev/null || true" | tr -d '[:space:]')"
  if [[ -z "${minio_pid}" || "${minio_pid}" == "0" ]]; then
    echo '{"checked": false, "present": false, "reason": "minio not running"}'
    return 0
  fi

  log "Verifying MinIO SNSD PUT fsync on PID ${minio_pid} (port ${minio_port})..."
  local trace_out
  trace_out="$(remote_exec "
    set -e
    tmp_log=\$(mktemp /tmp/minio-fsync-XXXXXX.log)
    timeout 8 strace -f -p '${minio_pid}' -e trace=fsync,fdatasync,rename,renameat,renameat2 -o \"\${tmp_log}\" &
    spid=\$!
    sleep 1
    python3 - '${target_host}' '${minio_port}' <<'PY_PUT'
import datetime, hashlib, hmac, sys, urllib.request
host, port = sys.argv[1], sys.argv[2]
access_key, secret_key = 'minioadmin', 'minioadmin'
bucket, key = 'juicefs', 'env-check-fsync-probe.bin'
payload = b'bowl-minio-fsync-probe' * 256
payload_hash = hashlib.sha256(payload).hexdigest()
now = datetime.datetime.now(datetime.timezone.utc)
amz_date = now.strftime('%Y%m%dT%H%M%SZ')
date_stamp = now.strftime('%Y%m%d')
region, service = 'us-east-1', 's3'
canonical_uri = f'/{bucket}/{key}'
canonical_headers = f'host:{host}:{port}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n'
signed_headers = 'host;x-amz-content-sha256;x-amz-date'
canonical_request = f'PUT\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}'
scope = f'{date_stamp}/{region}/{service}/aws4_request'
string_to_sign = f'AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n' + hashlib.sha256(canonical_request.encode()).hexdigest()
def sign(k, m): return hmac.new(k, m.encode('utf-8'), hashlib.sha256).digest()
k_signing = sign(sign(sign(sign(('AWS4' + secret_key).encode('utf-8'), date_stamp), region), service), 'aws4_request')
sig = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
auth = f'AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={sig}'
req = urllib.request.Request(
    f'http://{host}:{port}{canonical_uri}',
    data=payload,
    method='PUT',
    headers={'x-amz-date': amz_date, 'x-amz-content-sha256': payload_hash, 'Authorization': auth},
)
with urllib.request.urlopen(req, timeout=5) as resp:
    assert resp.status in (200, 204), f'unexpected PUT status {resp.status}'
PY_PUT
    sleep 1
    kill -INT \"\${spid}\" 2>/dev/null || true
    wait \"\${spid}\" 2>/dev/null || true
    fsync_cnt=\$(grep -Ec 'f(data)?sync\(' \"\${tmp_log}\" || true)
    rename_cnt=\$(grep -Ec 'rename(at2?)?\(' \"\${tmp_log}\" || true)
    rm -f \"\${tmp_log}\"
    printf '{\"checked\":true,\"present\":true,\"fsync_calls\":%s,\"rename_calls\":%s,\"verified\":%s}\n' \
      \"\${fsync_cnt}\" \"\${rename_cnt}\" \"\$([[ \"\${fsync_cnt}\" -gt 0 ]] && echo true || echo false)\"
  " 2>/dev/null || echo '{"checked":true,"present":true,"fsync_calls":0,"rename_calls":0,"verified":false}')"
  echo "${trace_out}"
}

OBS_JSON="$(check_obs_containers)"
FIO_JSON="$(measure_nvme_fio)"
NET_JSON="$(measure_network)"
MINIO_JSON="$(verify_minio_fsync)"

CPU_MODEL="$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || uname -m)"
VCPU="$(nproc 2>/dev/null || echo 1)"
MEM_KIB="$(awk '/MemTotal/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)"
KERNEL="$(uname -r)"
OS_REL="$(. /etc/os-release 2>/dev/null && echo "${PRETTY_NAME:-unknown}" || uname -s)"

RESULT_JSON="$(jq -n \
  --arg ts "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
  --arg nvme_root "${NVME_ROOT}" \
  --arg cpu "${CPU_MODEL}" \
  --argjson vcpu "${VCPU}" \
  --argjson mem_gib "$(awk -v k="${MEM_KIB}" 'BEGIN {printf "%.1f", k/1048576.0}')" \
  --arg kernel "${KERNEL}" \
  --arg os "${OS_REL}" \
  --argjson obs "${OBS_JSON}" \
  --argjson fio "${FIO_JSON}" \
  --argjson net "${NET_JSON}" \
  --argjson minio "${MINIO_JSON}" \
  '{
    timestamp: $ts,
    nvme_root: $nvme_root,
    host: {
      cpu_model: $cpu,
      vcpu: $vcpu,
      memory_gib: $mem_gib,
      kernel: $kernel,
      os_release: $os
    },
    observability_check: $obs,
    nvme_fio_ceiling: $fio,
    network_baseline: $net,
    minio_fsync_check: $minio,
    passed: ($obs.clean and (($minio.present // false) == false or ($minio.verified // false) == true))
  }')"

if [[ -n "${OUT_FILE}" ]]; then
  mkdir -p "$(dirname "${OUT_FILE}")"
  printf '%s\n' "${RESULT_JSON}" >"${OUT_FILE}"
fi

printf '%s\n' "${RESULT_JSON}"

if [[ "$(echo "${RESULT_JSON}" | jq -r '.observability_check.clean')" != "true" ]]; then
  log "ERROR: Forbidden observability containers are running: $(echo "${RESULT_JSON}" | jq -c '.observability_check')"
  exit 1
fi
