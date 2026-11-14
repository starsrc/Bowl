#!/usr/bin/env bash
# 构建可分发的客户端：本机平台的 bowl，加上注入沙箱用的 Linux 客户端（arm64 与 amd64）。
#
# bowl 建沙箱时会先找自己旁边的 bowl-linux-<arch>，所以把输出目录整个带走，
# 使用的人就不需要安装 Go，也不需要源码。
#
# 用法:
#   scripts/build-client.sh [输出目录]      # 默认 dist/
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/dist}"
case "$OUT" in /*) ;; *) OUT="$PWD/$OUT" ;; esac
mkdir -p "$OUT"

cd "$ROOT/client"
export GOWORK=off CGO_ENABLED=0
echo "[build-client] bowl ($(go env GOOS)/$(go env GOARCH))"
go build -trimpath -o "$OUT/bowl" ./cmd/bowl
for arch in arm64 amd64; do
  echo "[build-client] bowl-linux-$arch"
  GOOS=linux GOARCH="$arch" go build -trimpath -o "$OUT/bowl-linux-$arch" ./cmd/bowl
done
echo "[build-client] 完成: $OUT"
ls -lh "$OUT" | awk 'NR>1{print "  " $5, $9}'
