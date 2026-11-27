#!/usr/bin/env bash
set -euo pipefail
export GOWORK=off

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

REGION_ID="${REGION_ID:-${REGION:-us-west-1}}"
ZONE_ID="${ZONE_ID:-${ZONE:-us-west-1a}}"
INSTANCE_TYPE="${INSTANCE_TYPE:-ecs.i5.xlarge}"
IMAGE_ID="${IMAGE_ID:-ubuntu_24_04_x64_20G_alibase_20261016.vhd}"
SYSTEM_DISK_CATEGORY="${SYSTEM_DISK_CATEGORY:-cloud_essd}"
SYSTEM_DISK_SIZE="${SYSTEM_DISK_SIZE:-40}"
KEEP_INSTANCES="${KEEP_INSTANCES:-0}"
VPC_ID="${VPC_ID:-}"
VSWITCH_ID="${VSWITCH_ID:-}"
SECURITY_GROUP_ID="${SECURITY_GROUP_ID:-}"

RUNNER_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-instances)
      KEEP_INSTANCES=1
      shift
      ;;
    --suite|--targets|--resume)
      RUNNER_ARGS+=("$1" "$2")
      shift 2
      ;;
    --profiling)
      RUNNER_ARGS+=("$1")
      shift
      ;;
    *)
      RUNNER_ARGS+=("$1")
      shift
      ;;
  esac
done

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
KEY_PAIR_NAME="bowl-bench-kp-${RUN_TS}"
SERVER_NAME="bowl-bench-server-${RUN_TS}"
CLIENT_NAME="bowl-bench-client-${RUN_TS}"
WORK_DIR="$(mktemp -d "/tmp/bowl-bench-v2-${RUN_TS}-XXXXXX")"
KEY_FILE="${WORK_DIR}/id_rsa"

SERVER_INSTANCE_ID=""
CLIENT_INSTANCE_ID=""
KEY_CREATED=0
SERVER_SSH_HOST=""
CLIENT_SSH_HOST=""
SERVER_SSH_OPTS=()
CLIENT_SSH_OPTS=()

log() {
  printf '[%s] [aliyun-ecs] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || {
    log "ERROR: Required CLI '$1' is not installed."
    exit 1
  }
}

cleanup() {
  local exit_code=$?
  set +e
  log "=== Triggering Aliyun ECS Cleanup Trap (exit_code=${exit_code}) ==="

  if [[ "${exit_code}" != "0" && -n "${CLIENT_SSH_HOST}" && -f "${KEY_FILE}" ]]; then
    mkdir -p "${REPO_ROOT}/benchmark/runs"
    scp -r "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/opt/bowl-bench/benchmark/runs/*" \
      "${REPO_ROOT}/benchmark/runs/" 2>/dev/null || true
  fi

  if [[ "${KEEP_INSTANCES}" == "1" ]]; then
    log "KEEP_INSTANCES=1 set; preserving instances (${SERVER_INSTANCE_ID:-none}, ${CLIENT_INSTANCE_ID:-none}) and key ${KEY_FILE}."
    exit "${exit_code}"
  fi

  local ids=()
  [[ -n "${SERVER_INSTANCE_ID}" ]] && ids+=("${SERVER_INSTANCE_ID}")
  [[ -n "${CLIENT_INSTANCE_ID}" ]] && ids+=("${CLIENT_INSTANCE_ID}")

  if [[ ${#ids[@]} -gt 0 ]]; then
    log "Terminating temporary ECS instances in ${REGION_ID}: ${ids[*]}"
    local del_args=(ecs DeleteInstances --RegionId "${REGION_ID}" --Force true)
    local idx=1
    for id in "${ids[@]}"; do
      del_args+=("--InstanceId.${idx}" "${id}")
      idx=$((idx + 1))
    done

    local attempt
    for attempt in 1 2 3 4 5 6; do
      if aliyun "${del_args[@]}" >/dev/null 2>&1; then
        log "DeleteInstances accepted on attempt ${attempt}."
        break
      fi
      sleep 5
    done

    local ids_json
    ids_json="$(printf '%s\n' "${ids[@]}" | jq -R . | jq -sc .)"
    local poll
    for poll in $(seq 1 24); do
      local remaining
      remaining="$(aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "${ids_json}" 2>/dev/null | jq -r '.TotalCount // (.Instances.Instance | length) // 0')"
      if [[ "${remaining}" == "0" ]]; then
        log "Verified 0 remaining benchmark ECS instances."
        break
      fi
      sleep 5
    done
  fi

  if [[ "${KEY_CREATED}" == "1" && -n "${KEY_PAIR_NAME}" ]]; then
    log "Deleting temporary SSH KeyPair ${KEY_PAIR_NAME}..."
    aliyun ecs DeleteKeyPairs \
      --RegionId "${REGION_ID}" \
      --KeyPairNames "[\"${KEY_PAIR_NAME}\"]" >/dev/null 2>&1 || true
    KEY_CREATED=0
  fi

  rm -rf "${WORK_DIR}"
  log "=== Cleanup Complete ==="
  exit "${exit_code}"
}

trap cleanup EXIT INT TERM

discover_network() {
  if [[ -z "${VPC_ID}" ]]; then
    log "Auto-discovering VPC in ${REGION_ID}..."
    local vpcs_json
    vpcs_json="$(aliyun ecs DescribeVpcs --RegionId "${REGION_ID}" --PageSize 50)"
    if [[ -z "${VSWITCH_ID}" ]]; then
      # Prefer a VPC that actually has an Available VSwitch in ZONE_ID
      local vsw_all
      vsw_all="$(aliyun ecs DescribeVSwitches --RegionId "${REGION_ID}" --ZoneId "${ZONE_ID}" --PageSize 50)"
      VSWITCH_ID="$(echo "${vsw_all}" | jq -r '[.VSwitches.VSwitch[] | select(.Status == "Available")] | sort_by(.IsDefault | not) | .[0].VSwitchId // empty')"
      VPC_ID="$(echo "${vsw_all}" | jq -r --arg vsw "${VSWITCH_ID}" '.VSwitches.VSwitch[] | select(.VSwitchId == $vsw) | .VpcId // empty')"
    fi
    if [[ -z "${VPC_ID}" ]]; then
      VPC_ID="$(echo "${vpcs_json}" | jq -r '[.Vpcs.Vpc[] | select(.Status == "Available")] | sort_by(.IsDefault | not) | .[0].VpcId // empty')"
    fi
    if [[ -z "${VPC_ID}" ]]; then
      log "ERROR: No available VPC found in ${REGION_ID}. Set VPC_ID explicitly or create a VPC."
      exit 1
    fi
  fi

  if [[ -z "${VSWITCH_ID}" ]]; then
    log "Auto-discovering VSwitch in ${ZONE_ID} for VPC ${VPC_ID}..."
    VSWITCH_ID="$(aliyun ecs DescribeVSwitches --RegionId "${REGION_ID}" --VpcId "${VPC_ID}" --ZoneId "${ZONE_ID}" --PageSize 50 \
      | jq -r '[.VSwitches.VSwitch[] | select(.Status == "Available")] | sort_by(.IsDefault | not) | .[0].VSwitchId // empty')"
    if [[ -z "${VSWITCH_ID}" ]]; then
      log "ERROR: No available VSwitch found in zone ${ZONE_ID} under VPC ${VPC_ID}. Set VSWITCH_ID explicitly."
      exit 1
    fi
  fi

  if [[ -z "${SECURITY_GROUP_ID}" ]]; then
    log "Auto-discovering SecurityGroup in ${REGION_ID} for VPC ${VPC_ID}..."
    SECURITY_GROUP_ID="$(aliyun ecs DescribeSecurityGroups --RegionId "${REGION_ID}" --VpcId "${VPC_ID}" --PageSize 50 \
      | jq -r '.SecurityGroups.SecurityGroup[0].SecurityGroupId // empty')"
    if [[ -z "${SECURITY_GROUP_ID}" ]]; then
      SECURITY_GROUP_ID="$(aliyun ecs DescribeSecurityGroups --RegionId "${REGION_ID}" --PageSize 50 \
        | jq -r '.SecurityGroups.SecurityGroup[0].SecurityGroupId // empty')"
    fi
    if [[ -z "${SECURITY_GROUP_ID}" ]]; then
      log "ERROR: No SecurityGroup found in ${REGION_ID} for VPC ${VPC_ID}. Set SECURITY_GROUP_ID explicitly."
      exit 1
    fi
  fi

  log "Network resolved: Region=${REGION_ID} Zone=${ZONE_ID} VPC=${VPC_ID} VSwitch=${VSWITCH_ID} SG=${SECURITY_GROUP_ID}"
}

format_and_mount_nvme_cmd() {
  cat <<'EOF_NVME'
set -euo pipefail
if mountpoint -q /mnt/nvme; then
  echo "NVMe already mounted at /mnt/nvme"
  exit 0
fi
ROOT_PART="$(findmnt -n -o SOURCE /)"
ROOT_DISK="/dev/$(lsblk -ndo pkname "${ROOT_PART}" 2>/dev/null || basename "${ROOT_PART}")"
NVME_DEV="$(lsblk -dbpno NAME,SIZE,TYPE | awk -v root="${ROOT_DISK}" '$3 == "disk" && $1 != root {print $2, $1}' | sort -rn | head -n1 | awk '{print $2}')"
if [[ -z "${NVME_DEV}" ]]; then
  echo "ERROR: Local NVMe SSD device not found!" >&2
  lsblk >&2
  exit 1
fi
echo "${NVME_DEV}" > /etc/bowl_nvme_dev
mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0 "${NVME_DEV}" >/dev/null
mkdir -p /mnt/nvme
mount -o noatime,nodiratime "${NVME_DEV}" /mnt/nvme
df -h /mnt/nvme
EOF_NVME
}

require_tool aliyun
require_tool jq
require_tool go
require_tool ssh
require_tool scp
require_tool tar

discover_network

# Step 1: Cross-compile Bowl binaries for linux/amd64
log "Cross-compiling storage-service and bowl for linux/amd64 (GOWORK=off CGO_ENABLED=0)..."
GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C "${REPO_ROOT}/services/storage" build -mod=mod -trimpath -o "${WORK_DIR}/storage-service" ./cmd

GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C "${REPO_ROOT}/client" build -mod=mod -trimpath -o "${WORK_DIR}/bowl" ./cmd/bowl

chmod +x "${WORK_DIR}/storage-service" "${WORK_DIR}/bowl"

if [[ -n "${DRIVE9_REPO:-}" && -d "${DRIVE9_REPO}" ]]; then
  log "Cross-compiling optional Drive9 binaries from ${DRIVE9_REPO}..."
  DRIVE9_GO_TAGS="nogspt,noredis,nosqlite,nomysql,nopg,notikv,nobadger,noetcd,nocos,nobos,nohdfs,noibmcos,noobs,nooss,noqingstor,nosftp,noswift,noazure,nogs,noufile,nob2,nonfs,nodragonfly,nocifs,nostorj,noqiniu,notos,noks3,nowebdav,nogateway"
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go -C "${DRIVE9_REPO}" build -tags "${DRIVE9_GO_TAGS}" -trimpath -ldflags="-s -w" -o "${WORK_DIR}/drive9-server" ./cmd/drive9-server
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go -C "${DRIVE9_REPO}" build -tags "${DRIVE9_GO_TAGS}" -trimpath -ldflags="-s -w" -o "${WORK_DIR}/drive9" ./cmd/drive9
  chmod +x "${WORK_DIR}/drive9-server" "${WORK_DIR}/drive9"
fi

log "Packaging benchmark/ tree..."
COPYFILE_DISABLE=1 tar -czf "${WORK_DIR}/benchmark-tree.tar.gz" \
  --exclude='benchmark/runs' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  -C "${REPO_ROOT}" benchmark

# Step 2: Create ephemeral SSH keypair and 2 ECS instances
log "Creating ephemeral SSH KeyPair ${KEY_PAIR_NAME} in ${REGION_ID}..."
aliyun ecs CreateKeyPair \
  --RegionId "${REGION_ID}" \
  --KeyPairName "${KEY_PAIR_NAME}" \
  | jq -r '.PrivateKeyBody' > "${KEY_FILE}"
chmod 600 "${KEY_FILE}"
KEY_CREATED=1

launch_instance() {
  local inst_name="$1"
  aliyun ecs RunInstances \
    --RegionId "${REGION_ID}" \
    --ZoneId "${ZONE_ID}" \
    --ImageId "${IMAGE_ID}" \
    --InstanceType "${INSTANCE_TYPE}" \
    --SecurityGroupId "${SECURITY_GROUP_ID}" \
    --VSwitchId "${VSWITCH_ID}" \
    --InstanceChargeType PostPaid \
    --SpotStrategy NoSpot \
    --InternetChargeType PayByTraffic \
    --InternetMaxBandwidthOut 100 \
    --SystemDisk.Category "${SYSTEM_DISK_CATEGORY}" \
    --SystemDisk.Size "${SYSTEM_DISK_SIZE}" \
    --KeyPairName "${KEY_PAIR_NAME}" \
    --Amount 1 \
    --InstanceName "${inst_name}" \
    | jq -r '.InstanceIdSets.InstanceIdSet[0]'
}

log "Launching Node B (${SERVER_NAME}) and Node A (${CLIENT_NAME}) (${INSTANCE_TYPE}, ${IMAGE_ID})..."
SERVER_INSTANCE_ID="$(launch_instance "${SERVER_NAME}")"
CLIENT_INSTANCE_ID="$(launch_instance "${CLIENT_NAME}")"
log "Created Server=${SERVER_INSTANCE_ID} (${SERVER_NAME}), Client=${CLIENT_INSTANCE_ID} (${CLIENT_NAME})"

IDS_JSON="$(printf '%s\n' "${SERVER_INSTANCE_ID}" "${CLIENT_INSTANCE_ID}" | jq -R . | jq -sc .)"
SERVER_PUB_IP=""
SERVER_PRIV_IP=""
CLIENT_PUB_IP=""
CLIENT_PRIV_IP=""

log "Waiting for both ECS instances to reach Running state..."
for _ in $(seq 1 60); do
  DESC_JSON="$(aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "${IDS_JSON}")"
  RUNNING_CNT="$(echo "${DESC_JSON}" | jq '[.Instances.Instance[] | select(.Status == "Running")] | length')"
  if [[ "${RUNNING_CNT}" == "2" ]]; then
    SERVER_PUB_IP="$(echo "${DESC_JSON}" | jq -r --arg id "${SERVER_INSTANCE_ID}" '.Instances.Instance[] | select(.InstanceId == $id) | .PublicIpAddress.IpAddress[0]')"
    SERVER_PRIV_IP="$(echo "${DESC_JSON}" | jq -r --arg id "${SERVER_INSTANCE_ID}" '.Instances.Instance[] | select(.InstanceId == $id) | .VpcAttributes.PrivateIpAddress.IpAddress[0]')"
    CLIENT_PUB_IP="$(echo "${DESC_JSON}" | jq -r --arg id "${CLIENT_INSTANCE_ID}" '.Instances.Instance[] | select(.InstanceId == $id) | .PublicIpAddress.IpAddress[0]')"
    CLIENT_PRIV_IP="$(echo "${DESC_JSON}" | jq -r --arg id "${CLIENT_INSTANCE_ID}" '.Instances.Instance[] | select(.InstanceId == $id) | .VpcAttributes.PrivateIpAddress.IpAddress[0]')"
    break
  fi
  sleep 5
done

if [[ -z "${SERVER_PUB_IP}" || -z "${CLIENT_PUB_IP}" ]]; then
  log "ERROR: Timed out waiting for ECS instances to start."
  exit 1
fi
log "Server: pub=${SERVER_PUB_IP} priv=${SERVER_PRIV_IP} | Client: pub=${CLIENT_PUB_IP} priv=${CLIENT_PRIV_IP}"

BASE_SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=6 -o LogLevel=ERROR -i "${KEY_FILE}")
SERVER_SSH_OPTS=("${BASE_SSH_OPTS[@]}")
CLIENT_SSH_OPTS=("${BASE_SSH_OPTS[@]}")
SERVER_SSH_HOST="${SERVER_PUB_IP}"
CLIENT_SSH_HOST="${CLIENT_PUB_IP}"

log "Waiting for SSH readiness on both nodes..."
SERVER_OK=0
CLIENT_OK=0
for _ in $(seq 1 40); do
  if [[ "${SERVER_OK}" == "0" ]] && ssh "${BASE_SSH_OPTS[@]}" "root@${SERVER_PUB_IP}" "true" >/dev/null 2>&1; then
    SERVER_OK=1
    log "SSH ready on Server (${SERVER_PUB_IP})."
  fi
  if [[ "${CLIENT_OK}" == "0" ]] && ssh "${BASE_SSH_OPTS[@]}" "root@${CLIENT_PUB_IP}" "true" >/dev/null 2>&1; then
    CLIENT_OK=1
    log "SSH ready on Client (${CLIENT_PUB_IP})."
  fi
  [[ "${SERVER_OK}" == "1" && "${CLIENT_OK}" == "1" ]] && break
  sleep 3
done

if [[ "${SERVER_OK}" == "0" && "${CLIENT_OK}" == "1" ]]; then
  SERVER_SSH_HOST="${SERVER_PRIV_IP}"
  SERVER_SSH_OPTS=("${BASE_SSH_OPTS[@]}" -o "ProxyCommand=ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i ${KEY_FILE} -W %h:%p root@${CLIENT_PUB_IP}")
  ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "true"
elif [[ "${CLIENT_OK}" == "0" && "${SERVER_OK}" == "1" ]]; then
  CLIENT_SSH_HOST="${CLIENT_PRIV_IP}"
  CLIENT_SSH_OPTS=("${BASE_SSH_OPTS[@]}" -o "ProxyCommand=ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i ${KEY_FILE} -W %h:%p root@${SERVER_PUB_IP}")
  ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "true"
elif [[ "${SERVER_OK}" == "0" && "${CLIENT_OK}" == "0" ]]; then
  log "ERROR: Both instances timed out on SSH."
  exit 1
fi

# Install private key on Client node for VPC SSH into Server node
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "mkdir -p /root/.ssh && chmod 700 /root/.ssh"
scp "${CLIENT_SSH_OPTS[@]}" "${KEY_FILE}" "root@${CLIENT_SSH_HOST}:/root/.ssh/id_rsa"
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "chmod 600 /root/.ssh/id_rsa"

# Step 3: Format & mount NVMe on both nodes in parallel
log "Formatting and mounting local NVMe to /mnt/nvme on Server and Client..."
format_and_mount_nvme_cmd | ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "bash -s" &
PID_NVME_SRV=$!
format_and_mount_nvme_cmd | ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "bash -s" &
PID_NVME_CLI=$!
wait "${PID_NVME_SRV}" "${PID_NVME_CLI}"

# Step 4: Upload binaries and benchmark tree to both nodes
log "Uploading binaries and benchmark/ tree to Server and Client..."
ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "mkdir -p /opt/bowl-bench"
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "mkdir -p /opt/bowl-bench"

scp "${SERVER_SSH_OPTS[@]}" "${WORK_DIR}/storage-service" "root@${SERVER_SSH_HOST}:/usr/local/bin/storage-service"
scp "${SERVER_SSH_OPTS[@]}" "${WORK_DIR}/benchmark-tree.tar.gz" "root@${SERVER_SSH_HOST}:/tmp/benchmark-tree.tar.gz"
scp "${CLIENT_SSH_OPTS[@]}" "${WORK_DIR}/bowl" "root@${CLIENT_SSH_HOST}:/usr/local/bin/bowl"
scp "${CLIENT_SSH_OPTS[@]}" "${WORK_DIR}/benchmark-tree.tar.gz" "root@${CLIENT_SSH_HOST}:/tmp/benchmark-tree.tar.gz"

if [[ -x "${WORK_DIR}/drive9-server" && -x "${WORK_DIR}/drive9" ]]; then
  scp "${SERVER_SSH_OPTS[@]}" "${WORK_DIR}/drive9-server" "root@${SERVER_SSH_HOST}:/usr/local/bin/drive9-server"
  scp "${CLIENT_SSH_OPTS[@]}" "${WORK_DIR}/drive9" "root@${CLIENT_SSH_HOST}:/usr/local/bin/drive9"
fi

ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" \
  "chmod +x /usr/local/bin/storage-service && tar -xzf /tmp/benchmark-tree.tar.gz -C /opt/bowl-bench && rm -f /tmp/benchmark-tree.tar.gz && chmod +x /opt/bowl-bench/benchmark/infra/*.sh"
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" \
  "chmod +x /usr/local/bin/bowl && tar -xzf /tmp/benchmark-tree.tar.gz -C /opt/bowl-bench && rm -f /tmp/benchmark-tree.tar.gz && chmod +x /opt/bowl-bench/benchmark/infra/*.sh"

# Step 5: Bootstrap Server and Client in parallel
log "Bootstrapping Server (Node B) and Client (Node A) in parallel..."
ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "bash /opt/bowl-bench/benchmark/infra/bootstrap_server.sh" &
PID_BOOT_SRV=$!
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "bash /opt/bowl-bench/benchmark/infra/bootstrap_client.sh" &
PID_BOOT_CLI=$!
wait "${PID_BOOT_SRV}" "${PID_BOOT_CLI}"

# Step 6: Run env_check.sh and execute benchmark/runner.py on Node A
log "Running environment pre-check (NVMe fio, iperf3, ping RTT, observability isolation)..."
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" \
  "bash /opt/bowl-bench/benchmark/infra/env_check.sh --nvme-root /mnt/nvme --server-host '${SERVER_PRIV_IP}' --server-ssh 'root@${SERVER_PRIV_IP}' --out /opt/bowl-bench/benchmark/env_check.json"

RUNNER_ARGS_STR=""
for arg in ${RUNNER_ARGS[@]+"${RUNNER_ARGS[@]}"}; do
  RUNNER_ARGS_STR+=" $(printf '%q' "${arg}")"
done

log "Executing benchmark/runner.py --profile full-vpc on Client (Node A)..."
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "bash -lc '
  set -euo pipefail
  cd /opt/bowl-bench
  ulimit -n 1048576 || true
  PYTHONPATH=/opt/bowl-bench PYTHONDONTWRITEBYTECODE=1 python3 benchmark/runner.py \
    --profile full-vpc \
    --server-endpoint \"http://${SERVER_PRIV_IP}:9000\" \
    --server-metrics-url \"http://${SERVER_PRIV_IP}:9090\" \
    --server-ssh \"root@${SERVER_PRIV_IP}\" \
    --nvme-root /mnt/nvme \
    --env-check-json /opt/bowl-bench/benchmark/env_check.json \
    --update-latest${RUNNER_ARGS_STR}
'"

# Step 7: Copy runs/, latest_summary.json, and REPORT.md back to local benchmark/
log "Downloading benchmark artifacts (runs/, latest_summary.json, REPORT.md) back to ${REPO_ROOT}/benchmark/..."
mkdir -p "${REPO_ROOT}/benchmark/runs"
REMOTE_RUN_ID="$(ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "ls -1t /opt/bowl-bench/benchmark/runs 2>/dev/null | head -n1 || true")"
if [[ -n "${REMOTE_RUN_ID}" ]]; then
  scp -r "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/opt/bowl-bench/benchmark/runs/${REMOTE_RUN_ID}" \
    "${REPO_ROOT}/benchmark/runs/"
  log "Downloaded run directory: benchmark/runs/${REMOTE_RUN_ID}"
fi
scp "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/opt/bowl-bench/benchmark/latest_summary.json" \
  "${REPO_ROOT}/benchmark/latest_summary.json" 2>/dev/null || true
scp "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/opt/bowl-bench/benchmark/REPORT.md" \
  "${REPO_ROOT}/benchmark/REPORT.md" 2>/dev/null || true

log "Aliyun ECS benchmark run finished successfully."
