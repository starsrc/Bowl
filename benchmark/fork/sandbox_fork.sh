#!/usr/bin/env bash
# 量 sandbox 一级的 fork:存储 fork 本身(含暂停源 sandbox 和刷写)的时间,和发出 fork 到新 sandbox
# 能执行命令的时间。源 sandbox 里一直有进程在写,结束时核对 fork 里看到的是 fork 那一刻的值。
#
#   TEMPLATE=python:3.12-slim RUNS=5 bash benchmark/fork/sandbox_fork.sh
#
# 用当前的 bowl-sandbox 配置(--runtime 由 $BOWL_SANDBOX_RUNTIME 决定,存储节点由
# $BOWL_STORAGE_ENDPOINT 决定)。模板要先建好。
set -euo pipefail
BS=${BOWL_SANDBOX:-bowl-sandbox}
TEMPLATE=${TEMPLATE:?set TEMPLATE to a template name}
RUNS=${RUNS:-5}

src=$($BS create --template "$TEMPLATE" -q)
trap '$BS kill "$src" >/dev/null 2>&1 || true' EXIT
loop=$(mktemp)
printf 'i=0; while true; do i=$((i+1)); echo $i > /root/counter; sleep 0.05; done\n' > "$loop"
$BS cp "$loop" "$src:/root/loop.sh"
rm -f "$loop"
$BS exec -b "$src" -- sh /root/loop.sh >/dev/null
sleep 1

echo "runtime=${BOWL_SANDBOX_RUNTIME:-auto} template=$TEMPLATE runs=$RUNS"
printf '%-4s %-14s %-14s %s\n' run disk_fork_ms ready_ms counter_ok
for i in $(seq 1 "$RUNS"); do
  out=$($BS fork "$src")
  id=$(awk '/forked from/{print $2}' <<<"$out")
  disk=$(awk '/Disk fork:/{print $3}' <<<"$out")
  ready=$(awk '/Ready:/{print $2}' <<<"$out")
  # 源 sandbox 在 fork 后继续写,fork 里的值必须比源现在的值小
  forked=$($BS exec "$id" -- cat /root/counter)
  now=$($BS exec "$src" -- cat /root/counter)
  ok=no; [ "$forked" -lt "$now" ] && ok=yes
  ms() { case $1 in *ms) echo "${1%ms}";; *s) awk "BEGIN{printf \"%d\", ${1%s}*1000}";; esac; }
  printf '%-4s %-14s %-14s %s\n' "$i" "$(ms "$disk")" "$(ms "$ready")" "$ok"
  $BS kill "$id" >/dev/null
done
