# Bowl 仓库开发与测试规范

## 1. 多模块与离线容器构建

本仓库包含三个独立的 Go 模块，子模块通过 `replace github.com/starsrc/bowl => ...` 引用根模块：

| 模块路径 | 模块名 | 说明 |
| --- | --- | --- |
| `.` | `github.com/starsrc/bowl` | 共享 `gen/`、`pkg/` 与跨服务 `e2e/` |
| `services/storage` | `github.com/starsrc/bowl/services/storage` | 控制面、Pebble 存储引擎与数据库访问层 |
| `client` | `github.com/starsrc/bowl/client` | `bowl` CLI 与 Linux FUSE 客户端 |

- **强制关闭 Workspace**：`Taskfile.yml` 与 `.githooks/pre-commit` 默认设置 `GOWORK=off`，确保主仓与 `.worktrees/*` 构建行为一致，禁止依赖 `go.work` 掩盖模块依赖问题。
- **Vendor 容器构建**：`services/storage/vendor/` 与 `client/vendor/` 已加入 `.gitignore`，容器构建与测试前由 `task vendor:storage` / `task vendor:client` 自动更新（`go -C <dir> mod vendor`）。修改任意 `go.mod` 或 `pkg/`、`gen/` 后需同步依赖（`task tidy`）。
- **单栈 Compose 约束**：`Taskfile.yml` 固定 `COMPOSE_PROJECT_NAME=bowl`，本机仅运行一套容器栈（默认 `deploy/dev/docker-compose.yaml`）。

## 2. 生成文件禁止手改

以下产物由工具链确定性生成，**严禁手动编辑**：

| 禁止手改路径 | 生成方式 | 源定义 |
| --- | --- | --- |
| `gen/**` | `task buf:generate` | `proto/storage/v1/storage.proto`、`buf.gen.yaml` |
| `services/storage/internal/db/db.go`、`models.go`、`metadata.sql.go` | `task sqlc:generate` | `services/storage/db/migrations/`、`services/storage/db/queries/` |

## 3. 测试与质量守门

- **常规 Go 测试**：
  - `task test`：运行根模块与 `services/storage` 单元测试。
  - `task test:storage`：仅运行 `services/storage` 单元测试。
  - `task test:e2e`：启动 `storage-service` 容器并运行根模块 `e2e/` 跨服务集成测试。
- **Linux FUSE 客户端测试（Docker 容器内执行）**：
  - `task test:client`：在 `bowl-client-test` Linux 容器内构建 `GOOS=linux CGO_ENABLED=0` 二进制并运行 `client` 单元测试。
  - `task test:client:e2e`：拉起 `storage-service` 并在带 `/dev/fuse` 与 `SYS_ADMIN` 权限的测试容器内运行 `./e2e/...` 黑盒 FUSE 测试。
- **手写 Go 文件零注释约束**：
  - 所有手写 `.go` 文件禁止包含注释（含行注释、块注释与 `//nolint:` 指令），仅允许位于第 1 列的合法 `//go:` 编译器指令（如 `//go:build`，由 `gocheckcompilerdirectives` 校验）。生成文件（带 `// Code generated ... DO NOT EDIT.` 头）自动跳过。
  - 通过清晰命名、类型与函数拆分表达意图；无法避免的静态检查豁免统一写入 `.golangci.yml`，不得在 Go 源码中写 `//nolint:`。
- **静态检查与 Git Hook**：
  - 执行 `task hooks:install` 配置 `git config core.hooksPath .githooks`。
  - `.githooks/pre-commit` 按暂存区变更按需执行 `buf lint`、Go 无注释检查（`go run ./tools/nocomments`）及对应模块的 `golangci-lint`（其中 `client` 固定以 `GOOS=linux CGO_ENABLED=0` 检查；`gen/*.go`、`pkg/*.go` 或 `.golangci.yml` 变更会联检全部三个模块）。
  - 手动全量检查执行 `task lint`。

## 4. 契约同步

- 修改 `proto/` 后须依次执行：`task buf:lint` → `task buf:generate`。
