#!/usr/bin/env bash
# 拉起“本地 ⇄ 云端交接”的开发栈：两个存储节点 + 一个共享远端（deploy/handoff）。
# 幂等：重复执行只会把栈收敛到运行状态。停掉用：
#   docker compose -f deploy/handoff/docker-compose.yaml down        # 保留数据
#   docker compose -f deploy/handoff/docker-compose.yaml down -v     # 连数据一起删
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/deploy/handoff/docker-compose.yaml"

command -v docker >/dev/null || { echo "需要 docker" >&2; exit 1; }

# 容器内构建拉不到私有模块 github.com/starsrc/pkg，需要先在本机准备 vendor。
# go.mod 比 vendor 新（加过依赖）时也要重新生成，否则构建会报 inconsistent vendoring。
VENDOR="$ROOT/services/storage/vendor"
if [[ ! -f "$VENDOR/modules.txt" || "$ROOT/services/storage/go.mod" -nt "$VENDOR/modules.txt" ]]; then
  echo "[handoff-up] 准备 vendor"
  ( cd "$ROOT/services/storage" && GOWORK=off go mod vendor )
fi

echo "[handoff-up] 启动两个存储节点（首次会构建镜像，需要几分钟）"
docker compose -f "$COMPOSE" up -d --build --wait storage-local storage-cloud

echo
echo "[handoff-up] 就绪"
echo "  local  http://127.0.0.1:${LOCAL_PORT:-9100}"
echo "  cloud  http://127.0.0.1:${CLOUD_PORT:-9200}"
echo
echo "试一下:"
echo "  S=\$(bowl sandbox create --endpoint http://127.0.0.1:${LOCAL_PORT:-9100} -q)"
echo "  bowl disk push \$S --endpoint http://127.0.0.1:${LOCAL_PORT:-9100}     # 绑定远端，之后自动同步"
echo "  bowl sandbox move \$S --to http://127.0.0.1:${CLOUD_PORT:-9200}       # 切到云端"
echo "或者跑一遍全部场景: scripts/handoff-smoke.sh"
