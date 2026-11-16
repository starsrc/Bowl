# 快速开始

本指南演示如何在本地启动 Bowl 全套开发环境，并在 Linux（宿主机或容器）中跑通 **创建磁盘 → 挂载写入 → Fork 分支 → Diff 对比 → Merge 合并 → 卸载清理** 完整闭环。

## 前置依赖

| 工具 / 环境 | 用途与要求 | 说明 |
| :--- | :--- | :--- |
| **Go `1.26.5`** | 编译 `services/storage`、`client` 与运行测试 | 工作区使用 `GOWORK=off`（由 `Taskfile.yml` 自动注入） |
| **Task（go-task）** | 统一任务编排 | 替代手工拼接 `docker compose` / `go test` 命令 |
| **Docker & Docker Compose v2** | 启动 PostgreSQL 18、Migration、Storage 服务与 Jaeger | 开发环境编排位于 `deploy/dev/docker-compose.yaml` |
| **Linux `/dev/fuse` + `fusermount3`** | 运行 `bowl mount` / `bowl umount` | macOS 宿主机可编译 CLI 并调用 `bowl disk ...` 控制面命令，但 FUSE 挂载必须在 Linux 内核环境（或 `--privileged` + `/dev/fuse` 容器）中执行 |
| **Node.js + npm** | 构建与预览文档站点 | 仅在运行 `task docs:*` 时需要 |

## 构建与启动开发环境

在仓库根目录执行：

```bash
# 1. 构建服务端 Docker 镜像，并在 ./bin/ 下编译客户端可执行文件（如 ./bin/bowl）
task build

# 2. 启动 PostgreSQL 18、自动执行 Goose 迁移、启动 Storage 与 Jaeger
task run
```

默认情况下 `task run` 使用 `ENV=dev`（对应 `deploy/dev/docker-compose.yaml` 与 `deploy/dev/storage.env`）。如需切换生产可观测性栈（额外包含 Prometheus、Loki、Alloy、Grafana），可传入 `ENV=prod task run`。

### 开发环境端口清单

下表汇总 `deploy/dev/docker-compose.yaml` 中所有服务与默认宿主机映射端口：

| 服务名 | 容器内端口 | 默认宿主机映射 | 环境变量覆盖 | 用途 |
| :--- | :--- | :--- | :--- | :--- |
| **storage-service** | `:9000` | `9000`（`0.0.0.0`） | `STORAGE_PORT` | 公网 ConnectRPC HTTP/2（`h2c`）业务入口 |
| **storage-service（管理面）** | `:9090` | `127.0.0.1:9090` | `STORAGE_ADMIN_PORT` | Prometheus `/metrics`、`/debug/pprof/*`、`/healthz`、`/readyz` |
| **postgres**（`postgres:18-alpine`） | `:5432` | `9432` | `POSTGRES_PORT` | 控制面元数据与血缘 DAG 库（默认 DSN：`postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable`） |
| **jaeger** | `:16686` | `127.0.0.1:9686` | `JAEGER_UI_PORT` | Jaeger 分布式链路追踪 Web UI |
| **jaeger（OTLP）** | `:4317` / `:4318` | `127.0.0.1:9317` / `9318` | `JAEGER_OTLP_GRPC_PORT` / `JAEGER_OTLP_HTTP_PORT` | OTLP gRPC / HTTP 追踪接收端口 |

> [!TIP]
> 生产环境编排（`deploy/prod/docker-compose.yaml`）额外暴露 `127.0.0.1:9091`（Prometheus）、`127.0.0.1:9310`（Loki）、`127.0.0.1:9123`（Grafana Alloy）及 `127.0.0.1:9300`（Grafana）。

确认服务端就绪：

```bash
curl -fsS http://127.0.0.1:9090/readyz
```

## 完整闭环示例（Linux FUSE 环境）

以下步骤假设你位于**具备 `/dev/fuse` 的 Linux 宿主机或特权容器**内，且 `./bin/bowl`（或 `task build:client:test` 产出的 Linux 二进制 `./bin/bowl-linux`）已连接至 `http://127.0.0.1:9000`：

```bash
# 如在 Docker 容器内连接宿主机端口，请显式指定：
# export BOWL_STORAGE_ENDPOINT=http://host.docker.internal:9000

# 1. 创建 1 GiB 基础虚拟磁盘，使用 -q 仅输出 disk_id 便于脚本捕获
MAIN_DISK=$(./bin/bowl disk create --name app-main --size 1G -q)
echo "Main disk: $MAIN_DISK"

# 2. 挂载主盘并写入基线文件
mkdir -p /mnt/bowl-main /mnt/bowl-feat
./bin/bowl mount "$MAIN_DISK" /mnt/bowl-main
echo "v1=baseline" > /mnt/bowl-main/config.env
mkdir -p /mnt/bowl-main/src
echo "package main" > /mnt/bowl-main/src/main.go
sync

# 3. 卸载主盘（或以 --mode ro 只读方式保留），确保最新写入落盘后派生分支
./bin/bowl umount /mnt/bowl-main

# 4. 基于主盘瞬间 Fork 一块独立分支盘
FEAT_DISK=$(./bin/bowl disk fork "$MAIN_DISK" --name app-feat -q)
echo "Feature disk: $FEAT_DISK"

# 5. 挂载分支盘修改文件并写入新文件
./bin/bowl mount "$FEAT_DISK" /mnt/bowl-feat
echo "v2=feature-enabled" > /mnt/bowl-feat/config.env
echo "feature doc" > /mnt/bowl-feat/README.md
sync

# 查看客户端实时运行统计
./bin/bowl stats /mnt/bowl-feat
./bin/bowl umount /mnt/bowl-feat

# 6. 结构化 Diff 对比分支盘与主盘（--patch 输出类 git diff 文本补丁）
./bin/bowl disk diff "$FEAT_DISK" --target "$MAIN_DISK" --patch

# 7. 将分支变更三路合并回主盘（先 --dry-run 预演无冲突后再正式提交）
./bin/bowl disk merge "$FEAT_DISK" --into "$MAIN_DISK" --dry-run
./bin/bowl disk merge "$FEAT_DISK" --into "$MAIN_DISK"

# 8. 重新挂载主盘验证合并结果，随后清理资源
./bin/bowl mount "$MAIN_DISK" /mnt/bowl-main --mode ro
cat /mnt/bowl-main/config.env
cat /mnt/bowl-main/README.md
./bin/bowl umount /mnt/bowl-main
./bin/bowl disk delete "$FEAT_DISK" -y
./bin/bowl disk delete "$MAIN_DISK" -y
```

> [!NOTE]
> `bowl disk merge` 默认在目标盘已挂载或源盘以读写模式挂载时拒绝执行（可使用 `-f` / `--force` 显式绕过检查）；若存在三路修改冲突，命令将输出冲突报告并以退出码 `1` 中止，此时可通过 `--strategy ours|theirs` 或 `--resolve <path>=ours|theirs` 裁决。

## 常用测试与开发任务速查

所有日常工程检查均收敛在根目录 `Taskfile.yml` 中：

| Task 命令 | 执行范围与方式 | 适用场景 |
| :--- | :--- | :--- |
| `task test` | 在宿主机运行根模块单元测试（`go test -v ./...`）与 `task test:storage`（不含 `client` 容器测试，默认不传 `-race`） | 提交前快速回归根模块与服务端 |
| `task test:storage` | 进入 `services/storage` 执行 `go test -v ./...` | 修改服务端控制面或 `StoreActor` 后 |
| `task test:client` | 在 Linux Docker 容器内编译 `CGO_ENABLED=0 GOOS=linux` 二进制并运行 `client/...` 单元测试（非特权，不挂载 `/dev/fuse`） | 修改客户端逻辑或 CLI 后（macOS 同样可执行） |
| `task test:e2e` | 启动 `storage-service` 容器并运行根模块 `e2e/` 跨服务集成测试 | 验证 RPC 与控制面端到端语义 |
| `task test:client:e2e` | 启动 `storage-service` 并在带 `/dev/fuse` 与 `SYS_ADMIN` 权限的测试容器内运行 `client/e2e/`（`./e2e/...`）黑盒 FUSE 测试 | 验证真实 Linux VFS 语义与挂载回收 |
| `task lint` | 依次在根模块、`services/storage` 与 `client` 执行 `golangci-lint` 与 `go vet` | 代码静态检查与规范门禁 |
| `task buf:generate` | 根据 `proto/storage/v1/storage.proto` 重新生成 `gen/` 桩代码与校验代码 | 修改 Protobuf 契约后 |
| `task docs:dev` | 自动生成 RPC 文档并启动 VitePress 本地预览服务（首次需先运行 `task docs:install`） | 本地编写与预览开发者文档 |

## 延伸阅读

- [CLI 参考手册](/03-interfaces/cli-reference) —— 查询每个命令的完整标志位、`--json` 输出与退出码。
- [可观测性手册](/03-interfaces/observability) —— 查看服务端/客户端指标、OTel Tracing 采样与 `.stats` 虚拟文件。
- [环境管理](/01-overview/quickstart) / [清理环境](/01-overview/capabilities)：使用 `task down` 停止服务，`task logs` 追踪容器输出。
