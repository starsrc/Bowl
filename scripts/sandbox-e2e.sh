#!/usr/bin/env bash
# 在一台有 KVM 的 Linux 上把 bowl-sandbox 从头到尾跑一遍:装 Cloud Hypervisor 和虚机内核、
# 建一个 XFS 状态目录、做模板、建沙箱,逐项检查结果。任何一项不符就以非零退出。
# CI(.github/workflows/sandbox-e2e.yml)和本机都用这一个脚本:
#   scripts/sandbox-e2e.sh
# 需要:/dev/kvm、sudo、Docker、Go、curl、iptables。会在 $SANDBOX_E2E_DIR 下留一个回环文件系统,
# 结束时卸载并删除。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CH_VERSION="${CH_VERSION:-v54.1}"
KERNEL_TAG="${KERNEL_TAG:-ch-release-v7.2.8-20261009}"
IMAGE="${SANDBOX_E2E_IMAGE:-node:22-slim}"
WORK="${SANDBOX_E2E_DIR:-/var/lib/bowl-sandbox-e2e}"

if [ "$(id -u)" != 0 ]; then
  BIN="$(mktemp -d)"
  echo "== build"
  for cmd in bowl-sandbox bowl-sandbox-guest; do
    (cd "$ROOT/client" && GOWORK=off CGO_ENABLED=0 GOOS=linux go build -o "$BIN/$cmd" "./cmd/$cmd")
  done
  exec sudo env "PATH=$PATH" SANDBOX_E2E_BIN="$BIN" CH_VERSION="$CH_VERSION" KERNEL_TAG="$KERNEL_TAG" \
    SANDBOX_E2E_IMAGE="$IMAGE" SANDBOX_E2E_DIR="$WORK" "$0" "$@"
fi

BIN="${SANDBOX_E2E_BIN:?以 root 运行时要用 SANDBOX_E2E_BIN 指出两个可执行文件所在的目录}"
[ -e /dev/kvm ] || { echo "没有 /dev/kvm" >&2; exit 1; }

STATE="$WORK/state"
B=("$BIN/bowl-sandbox" --state-dir "$STATE")
failed=0

cleanup() {
  "${B[@]}" kill --all >/dev/null 2>&1 || true
  pkill -f "$BIN/bowl-sandbox --state-dir $STATE serve" 2>/dev/null || true
  mountpoint -q "$STATE" && umount "$STATE" || true
  rm -rf "$WORK"
}
trap cleanup EXIT

check() { # check <说明> <期望> <实际>
  if [ "$2" = "$3" ]; then
    echo "ok   $1"
  else
    echo "FAIL $1: 期望 [$2],实际 [$3]"
    failed=1
  fi
}

echo "== host setup"
command -v cloud-hypervisor >/dev/null || {
  curl -fsSL -o /usr/local/bin/cloud-hypervisor \
    "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/$CH_VERSION/cloud-hypervisor-static"
  chmod +x /usr/local/bin/cloud-hypervisor
}
command -v mkfs.xfs >/dev/null || { apt-get update -qq && apt-get install -y -qq xfsprogs; }
mkdir -p "$STATE"
truncate -s 8G "$WORK/state.img"
mkfs.xfs -q -m reflink=1 "$WORK/state.img"
mount -o loop "$WORK/state.img" "$STATE"
curl -fsSL -o "$STATE/vmlinux" "https://github.com/cloud-hypervisor/linux/releases/download/$KERNEL_TAG/vmlinux-x86_64"
install -m755 "$BIN/bowl-sandbox-guest" "$STATE/bowl-sandbox-guest"
cloud-hypervisor --version

echo "== template"
"${B[@]}" template create "$IMAGE" --prewarm "node -v"
check "模板带内存快照" yes "$("${B[@]}" template list | awk 'NR==2{print $2}')"

echo "== create and exec"
t0=$(date +%s%N)
ID=$("${B[@]}" create --template "$IMAGE" -q)
echo "create 用了 $(( ($(date +%s%N) - t0) / 1000000 )) ms"
check "状态是 running" running "$("${B[@]}" info "$ID" | awk '/^Status:/{print $2}')"
check "node 能运行" v22 "$("${B[@]}" exec "$ID" -- node -v | cut -d. -f1)"
set +e; "${B[@]}" exec "$ID" -- sh -c 'exit 7'; code=$?; set -e
check "退出码透传" 7 "$code"

RUN=$("${B[@]}" exec -b "$ID" -- sh -c 'sleep 1; echo bg; exit 3')
check "后台命令的退出码" 3 "$("${B[@]}" wait "$ID" "$RUN" || true)"
check "后台命令的输出" bg "$("${B[@]}" logs "$ID" --exec "$RUN" | head -1)"

echo hello > "$WORK/a.txt"
"${B[@]}" cp "$WORK/a.txt" "$ID:/root/b.txt"
"${B[@]}" cp "$ID:/root/b.txt" "$WORK/c.txt"
check "文件拷进再拷出" hello "$(cat "$WORK/c.txt")"

echo "== network"
probe='fetch(process.argv[1],{signal:AbortSignal.timeout(5000)}).then(r=>console.log(r.status),()=>console.log("blocked"))'
check "元数据地址被挡住" blocked "$("${B[@]}" exec "$ID" -- node -e "$probe" http://169.254.169.254/)"
check "能访问外网" 200 "$("${B[@]}" exec "$ID" -- node -e "$probe" https://example.com/)"

echo "== pause, stop, start"
"${B[@]}" pause "$ID" >/dev/null
check "暂停后的状态" paused "$("${B[@]}" info "$ID" | awk '/^Status:/{print $2}')"
"${B[@]}" resume "$ID" >/dev/null
"${B[@]}" exec "$ID" -- sh -c 'echo persisted > /root/p.txt'
"${B[@]}" stop "$ID" >/dev/null
check "停机后的状态" stopped "$("${B[@]}" info "$ID" | awk '/^Status:/{print $2}')"
"${B[@]}" start "$ID" >/dev/null
check "重启后文件还在" persisted "$("${B[@]}" exec "$ID" -- cat /root/p.txt)"

echo "== cold boot and snapshot isolation"
COLD=$("${B[@]}" create --template "$IMAGE" --cpus 2 -q)
check "规格和快照不同时冷启动" 2 "$("${B[@]}" exec "$COLD" -- nproc)"
"${B[@]}" kill "$COLD" >/dev/null
A=$("${B[@]}" create --template "$IMAGE" -q)
C=$("${B[@]}" create --template "$IMAGE" -q)
addr() { "${B[@]}" info "$1" | awk '/^Address:/{print $2}'; }
rand() { "${B[@]}" exec "$1" -- sh -c 'head -c 16 /dev/urandom | od -An -tx1'; }
check "两个沙箱地址不同" different "$([ "$(addr "$A")" != "$(addr "$C")" ] && echo different || echo same)"
check "两个沙箱随机数不同" different "$([ "$(rand "$A")" != "$(rand "$C")" ] && echo different || echo same)"
skew=$(( $("${B[@]}" exec "$C" -- date +%s) - $(date +%s) ))
check "恢复后时钟正确" ok "$([ "${skew#-}" -le 2 ] && echo ok || echo "差 $skew 秒")"
"${B[@]}" kill "$A" >/dev/null
"${B[@]}" kill "$C" >/dev/null

echo "== timeout"
"${B[@]}" timeout "$ID" 10s >/dev/null
sleep 25
check "到期后沙箱被回收" gone "$("${B[@]}" info "$ID" >/dev/null 2>&1 && echo present || echo gone)"

echo "== serve and SDK"
"${B[@]}" serve >"$WORK/serve.log" 2>&1 &
sleep 2
sdk=$(cd "$ROOT/sdk/python" && python3 -c '
from bowl_sandbox import Sandbox
with Sandbox.create(template="'"$IMAGE"'", timeout=120) as s:
    s.files.write("/root/x.txt", "hi")
    print(s.commands.run("cat /root/x.txt").stdout.strip(), s.files.read("/root/x.txt"),
          s.commands.run("sleep 1; echo bg", background=True).wait().stdout.strip())
')
check "SDK 执行、读写文件、后台命令" "hi hi bg" "$sdk"
pkill -f "$BIN/bowl-sandbox --state-dir $STATE serve" || true

echo "== leftovers"
"${B[@]}" kill --all >/dev/null 2>&1 || true
check "没有残留的虚机进程" 0 "$(pgrep -fc "cloud-hypervisor.*$STATE" || true)"
check "没有残留的 tap 网卡" 0 "$(ip -br link | grep -c '^bsbx' || true)"

[ "$failed" = 0 ] && echo "ALL OK" || { echo "有检查没通过"; exit 1; }
