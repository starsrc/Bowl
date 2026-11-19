#!/usr/bin/env bash
# 注意:这个脚本调用 bowl disk push/pull/head、bowl node set、bowl sandbox mount/move 等交接命令。
# 它们来自迁移 cobra 之前的客户端,还没有迁到现在的 bowl 上,所以现在跑不了。sandbox 一级的交接
# 已经是 bowl-sandbox move。
# 本地 ⇄ 云端交接的端到端冒烟：把六个目标场景按顺序各跑一遍并断言结果。
#
#   A 本地干活，写入自动同步到远端
#   B 切到云端继续干，再切回本地
#   C 第一次去新节点不用先下载数据
#   D 云端干活时，本地只读查看
#   E 在云端 fork、修改、合并
#   F 本地有没同步的改动时被云端接管，之后合并回来
#
# 前提：scripts/handoff-up.sh 已经把两个节点拉起来。
# 用法:
#   scripts/handoff-smoke.sh
#   BOWL_BIN=./bowl LOCAL=http://127.0.0.1:9100 CLOUD=http://127.0.0.1:9200 scripts/handoff-smoke.sh
#   COMPUTE=1 scripts/handoff-smoke.sh     # 云端的沙箱起在独立的 Docker 上（需先 COMPUTE=1 scripts/handoff-up.sh）
#
# 只清理本次创建的沙箱和磁盘。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
L="${LOCAL:-http://127.0.0.1:9100}"
C="${CLOUD:-http://127.0.0.1:9200}"
WORK="$(mktemp -d)"
sandboxes=()
disks=()

log() { printf '\n[handoff-smoke] %s\n' "$*"; }
fail() { printf '\n[handoff-smoke] 失败: %s\n' "$*" >&2; exit 1; }

cleanup() {
  for id in "${sandboxes[@]:-}"; do
    [[ -n "$id" ]] && "$BOWL" sandbox kill "$id" --keep-disk >/dev/null 2>&1 || true
  done
  for id in "${disks[@]:-}"; do
    [[ -z "$id" ]] && continue
    "$BOWL" disk delete "$id" -y --endpoint "$L" >/dev/null 2>&1 || true
    "$BOWL" disk delete "$id" -y --endpoint "$C" >/dev/null 2>&1 || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

if [[ -n "${BOWL_BIN:-}" ]]; then
  BOWL="$BOWL_BIN"
else
  BOWL="$WORK/bowl"
  ( cd "$ROOT/client" && GOWORK=off go build -o "$BOWL" ./cmd/bowl ) || fail "编译客户端失败"
fi
[[ -x "$BOWL" ]] || fail "找不到可执行的客户端: $BOWL"
# 沙箱里用的 Linux 客户端从源码编译，需要让它找到源码。
export BOWL_SOURCE_DIR="${BOWL_SOURCE_DIR:-$ROOT}"

generation() { "$BOWL" disk head "$1" --json --endpoint "$2" | sed -n 's/.*"generation":\([0-9]*\).*/\1/p'; }
owner() { "$BOWL" disk head "$1" --json --endpoint "$2" | sed -n 's/.*"owner":"\([^"]*\)".*/\1/p'; }
# in_sandbox <id> <shell 片段>：在沙箱的磁盘挂载点下执行
in_sandbox() { local id="$1"; shift; "$BOWL" sandbox exec "$id" -- sh -c "$*"; }
# expect_fail <说明> <命令...>
expect_fail() { local what="$1"; shift; if "$@" >/dev/null 2>&1; then fail "$what"; fi; }

# 节点登记用临时目录，不读也不改使用者自己的登记。
export BOWL_CONFIG_DIR="$WORK/config"
CLOUD_DOCKER="tcp://127.0.0.1:${CLOUD_DOCKER_PORT:-2385}"
if [[ "${COMPUTE:-}" == 1 ]]; then
  docker -H "$CLOUD_DOCKER" version >/dev/null 2>&1 || fail "连不上云端算力 $CLOUD_DOCKER（先跑 COMPUTE=1 scripts/handoff-up.sh）"
  "$BOWL" node set local --endpoint "$L" >/dev/null
  "$BOWL" node set cloud --endpoint "$C" --docker "$CLOUD_DOCKER" --sandbox-endpoint http://172.31.250.10:9000 >/dev/null
  echo "[handoff-smoke] 云端沙箱将起在独立的 Docker 上: $CLOUD_DOCKER"
fi

for ep in "$L" "$C"; do
  "$BOWL" disk list --endpoint "$ep" >/dev/null 2>&1 || fail "连不上存储节点 $ep（先跑 scripts/handoff-up.sh）"
done

# ---------------------------------------------------------------- A
log "A 本地干活，写入自动同步到远端"
S="$("$BOWL" sandbox create --endpoint "$L" --size 1GiB -q)"
[[ -n "$S" ]] || fail "没有拿到沙箱 ID"
sandboxes+=("$S"); disks+=("$S")
echo "    sandbox=$S"
in_sandbox "$S" 'echo v1 > notes.txt; head -c 20000000 /dev/urandom > data.bin; sha256sum data.bin | cut -c1-16 > data.sha; sync'
"$BOWL" disk push "$S" --endpoint "$L" >/dev/null
g1="$(generation "$S" "$L")"
in_sandbox "$S" 'echo v2 >> notes.txt; sync'
synced=0
for _ in $(seq 1 45); do
  sleep 1
  if [[ "$(generation "$S" "$L")" -gt "$g1" ]]; then synced=1; break; fi
done
[[ "$synced" == 1 ]] || fail "45 秒内后台同步没有把新写入推上去"
[[ "$(owner "$S" "$L")" == "local" ]] || fail "后台同步不应改变写者"

# ---------------------------------------------------------------- B / C
log "B 切到云端   C 第一次去新节点不用先下载数据"
out="$("$BOWL" sandbox move "$S" --to "$C")"
echo "$out" | sed 's/^/    /'
[[ "$out" == *"本地复用 0 个"* && "$out" == *"留待读到时再取"* ]] || fail "第一次去云端应当一个数据包都不先下载"
[[ "$(owner "$S" "$C")" == "cloud" ]] || fail "切换后写者应为 cloud"
if [[ "${COMPUTE:-}" == 1 ]]; then
  docker -H "$CLOUD_DOCKER" ps --format '{{.Names}}' | grep -q "bowl-sbx-$S" || fail "沙箱应当跑在云端的 Docker 上"
  if docker ps -a --format '{{.Names}}' | grep -q "bowl-sbx-$S"; then fail "本机 Docker 上不应再有这个沙箱"; fi
  [[ "$(in_sandbox "$S" 'echo ok')" == "ok" ]] || fail "无法在云端算力上的沙箱里执行命令"
fi
in_sandbox "$S" '[ "$(tail -1 notes.txt)" = v2 ] && [ "$(sha256sum data.bin | cut -c1-16)" = "$(cat data.sha)" ]' \
  || fail "云端看到的内容和本地写的不一致"
in_sandbox "$S" 'echo computed-on-cloud > result.txt; sync'

# ---------------------------------------------------------------- D
log "D 云端干活时，本地只读查看"
"$BOWL" disk push "$S" --endpoint "$C" >/dev/null
V="$("$BOWL" sandbox create --endpoint "$L" --size 64MiB -q)"
sandboxes+=("$V"); disks+=("$V")
"$BOWL" sandbox mount "$V" --disk "$S" --path /mnt/view --read-only >/dev/null
got="$("$BOWL" sandbox exec --cwd /mnt/view "$V" -- cat result.txt)"
[[ "$got" == "computed-on-cloud" ]] || fail "本地只读副本没有看到云端已同步的内容: $got"
expect_fail "只读副本不应可写" "$BOWL" sandbox exec --cwd /mnt/view "$V" -- sh -c 'echo x > nope.txt'
expect_fail "写者在云端时，本地不应能可写挂载" "$BOWL" sandbox mount "$V" --disk "$S" --path /mnt/rw --force
"$BOWL" sandbox kill "$V" >/dev/null

# ---------------------------------------------------------------- E
log "E 在云端 fork、修改、合并"
F="$("$BOWL" sandbox fork "$S" -q --endpoint "$C")"
sandboxes+=("$F"); disks+=("$F")
in_sandbox "$F" '[ "$(cat result.txt)" = computed-on-cloud ] && echo from-fork > attempt.txt && sync' || fail "fork 出来的沙箱内容不对"
expect_fail "fork 的写入不应出现在原沙箱" "$BOWL" sandbox exec "$S" -- test -f attempt.txt
"$BOWL" sandbox kill "$F" --keep-disk >/dev/null
"$BOWL" sandbox unmount "$S" --path /mnt/data >/dev/null
"$BOWL" disk merge "$F" --into "$S" --endpoint "$C" >/dev/null || fail "合并 fork 失败"
"$BOWL" sandbox mount "$S" --disk "$S" --path /mnt/data >/dev/null
[[ "$(in_sandbox "$S" 'cat attempt.txt')" == "from-fork" ]] || fail "合并后原盘没有 fork 的改动"
"$BOWL" disk delete "$F" -y --endpoint "$C" >/dev/null

# ---------------------------------------------------------------- B（回程）
log "B 切回本地"
out="$("$BOWL" sandbox move "$S" --to "$L")"
echo "$out" | sed 's/^/    /'
[[ "$out" != *"本地复用 0 个"* ]] || fail "切回去过的节点应当复用本地已有的数据包"
in_sandbox "$S" '[ "$(cat result.txt)" = computed-on-cloud ] && [ "$(cat attempt.txt)" = from-fork ] && [ "$(sha256sum data.bin | cut -c1-16)" = "$(cat data.sha)" ]' \
  || fail "切回本地后内容不完整"

# ---------------------------------------------------------------- F
log "F 本地有没同步的改动时被云端接管，之后合并回来"
in_sandbox "$S" 'echo synced > f-synced.txt; sync'
"$BOWL" disk push "$S" --endpoint "$L" >/dev/null
in_sandbox "$S" 'echo offline-work > f-local-only.txt; sync'
"$BOWL" sandbox move "$S" --to "$C" --force >/dev/null
in_sandbox "$S" '[ "$(cat f-synced.txt)" = synced ] && echo cloud-work > f-cloud-only.txt && sync' || fail "强行接管后云端应拿到最后一次同步的内容"
"$BOWL" sandbox kill "$S" --keep-disk >/dev/null
"$BOWL" disk push "$S" --release --endpoint "$C" >/dev/null
if "$BOWL" disk pull "$S" --endpoint "$L" >/dev/null 2>&1; then
  # 接管前后台同步恰好已把那次写入推上去了：本地没有分叉，无需合并。
  echo "    本地改动在接管前已同步，无需合并"
else
  kept="$("$BOWL" disk pull "$S" --keep-local --json --endpoint "$L" | sed -n 's/.*"kept_local_disk_id":"\([^"]*\)".*/\1/p')"
  [[ -n "$kept" ]] || fail "--keep-local 没有另存本地改动"
  disks+=("$kept")
  echo "    本地改动另存为 $kept"
  "$BOWL" disk merge "$kept" --into "$S" --endpoint "$L" >/dev/null || fail "把本地改动合并回来失败"
  "$BOWL" disk delete "$kept" -y --endpoint "$L" >/dev/null
fi
V="$("$BOWL" sandbox create --endpoint "$L" --size 64MiB -q)"
sandboxes+=("$V"); disks+=("$V")
"$BOWL" sandbox mount "$V" --disk "$S" --path /mnt/work >/dev/null
got="$("$BOWL" sandbox exec --cwd /mnt/work "$V" -- sh -c 'cat f-local-only.txt f-cloud-only.txt | tr "\n" " "')"
[[ "$got" == "offline-work cloud-work " ]] || fail "合并后应同时有两边的改动，实际: $got"
"$BOWL" sandbox kill "$V" >/dev/null

log "清理"
"$BOWL" disk delete "$S" -y --endpoint "$L" >/dev/null
"$BOWL" disk delete "$S" -y --endpoint "$C" >/dev/null 2>&1 || true
sandboxes=(); disks=()

printf '\n[handoff-smoke] 六个场景全部通过\n'
