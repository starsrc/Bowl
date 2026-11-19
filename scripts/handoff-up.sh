#!/usr/bin/env bash
# 拉起“本地 ⇄ 云端交接”的开发栈：两个存储节点 + 一个共享远端（deploy/handoff）。
# COMPUTE=1 时再起一个独立的 Docker 顶替“云端的算力机器”，并打印登记节点的命令。
# 幂等：重复执行只会把栈收敛到运行状态。停掉用：
#   docker compose -f deploy/handoff/docker-compose.yaml down        # 保留数据
#   docker compose -f deploy/handoff/docker-compose.yaml down -v     # 连数据一起删
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/deploy/handoff/docker-compose.yaml"

command -v docker >/dev/null || { echo "需要 docker" >&2; exit 1; }

# 容器内构建拉不到私有模块 github.com/starsrc/pkg，需要先在本机准备 vendor。
# 每次都重新生成：vendor 里还带着本仓库 gen/ 和 pkg/ 的副本，协议或公共包一改它就过期，
# 构建会用旧代码或直接编译失败。
echo "[handoff-up] 准备 vendor"
( cd "$ROOT/services/storage" && GOWORK=off GOFLAGS=-mod=mod go mod vendor )

echo "[handoff-up] 启动两个存储节点（首次会构建镜像，需要几分钟）"
services=(storage-local storage-cloud)
profile=()
if [[ "${COMPUTE:-}" == 1 ]]; then
  services+=(cloud-compute)
  profile=(--profile compute)
fi
docker compose -f "$COMPOSE" "${profile[@]}" up -d --build --wait "${services[@]}"

echo
echo "[handoff-up] 就绪"
echo "  local  http://127.0.0.1:${LOCAL_PORT:-9100}"
echo "  cloud  http://127.0.0.1:${CLOUD_PORT:-9200}"
echo
echo "试一下(用 bowl-sandbox,container 后端在 macOS 和 Linux 上都能跑):"
echo "  export BOWL_SANDBOX_RUNTIME=container"
echo "  bowl-sandbox template create alpine:3.20 --endpoint http://127.0.0.1:${LOCAL_PORT:-9100}"
echo "  S=\$(bowl-sandbox create --template alpine:3.20 --endpoint http://127.0.0.1:${LOCAL_PORT:-9100} -q)"
echo "  bowl-sandbox move \$S --to http://127.0.0.1:${CLOUD_PORT:-9200}       # 切到云端"
echo
echo "COMPUTE=1 的独立云端算力和 scripts/handoff-smoke.sh 还依赖 bowl disk push/pull/head、bowl node"
echo "这些交接命令,它们还没有迁到 cobra 版的 bowl 上,现在跑不了。"
