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
| `docs/03-interfaces/rpc-api.md` | `task docs:gen` | `proto/storage/v1/storage.proto`（由 `docs/scripts/gen-rpc-doc.mjs` 提取） |

## 3. 测试与质量守门

- **常规 Go 测试**：
  - `task test`：运行根模块与 `services/storage` 单元测试。
  - `task test:storage`：仅运行 `services/storage` 单元测试。
  - `task test:e2e`：启动 `storage-service` 容器并运行根模块 `e2e/` 跨服务集成测试。
- **Linux FUSE 客户端测试（Docker 容器内执行）**：
  - `task test:client`：在 `bowl-client-test` Linux 容器内构建 `GOOS=linux CGO_ENABLED=0` 二进制并运行 `client` 单元测试。
  - `task test:client:e2e`：拉起 `storage-service` 并在带 `/dev/fuse` 与 `SYS_ADMIN` 权限的测试容器内运行 `./e2e/...` 黑盒 FUSE 测试。
- **静态检查与 Git Hook**：
  - 执行 `task hooks:install` 配置 `git config core.hooksPath .githooks`。
  - `.githooks/pre-commit` 按暂存区变更按需执行 `buf lint` 及对应模块的 `golangci-lint`（其中 `client` 固定以 `GOOS=linux CGO_ENABLED=0` 检查；`gen/*.go`、`pkg/*.go` 或 `.golangci.yml` 变更会联检全部三个模块）。
  - `.githooks/pre-push` 在推送前跑不依赖 Docker / PostgreSQL 的测试：根模块与 `services/storage`（不含 `internal/db`）的 `go test`、`client` 的 `GOOS=linux go vet` 与本机可跑的包。未改 Go / proto 文件时跳过。
  - 手动全量检查执行 `task lint`。
- **CI（`.github/workflows/ci.yml`）**：
  - fast：`task lint`、`go.mod` 整洁检查、`task test`（带已迁移的 PostgreSQL）、`task test:client`。对仓库有写权限的作者的 PR 跑这一档。
  - full：fast 加 `task test:e2e`、`task test:client:e2e`、`task docs:build`。外部贡献者的 PR、`release/` 分支的 PR、`v*` 标签、带 `full-ci` 标签的 PR 跑这一档。

## 4. 契约与文档同步

- 修改 `proto/` 后须依次执行：`task buf:lint` → `task buf:generate` → `task docs:gen`。
- 变更 CLI 子命令、RPC 接口、可观测性指标、核心架构或存储引擎实现时，必须同步更新 `docs/` 对应章节，并在全部页面就绪后执行 `task docs:build` 验证无死链与 Vue SFC 编译错误。
