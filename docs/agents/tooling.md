# Bowl 构建、代码生成、迁移与测试工具手册

## 多模块与目录布局

- **根模块（[`go.mod`](../../go.mod)）**：包含 `proto/storage/v1/`（Protobuf 源文件）、`gen/go/`（ConnectRPC 生成代码）、[`pkg/validator`](../../pkg/validator)（请求校验器）、[`tests/contract`](../../tests/contract)（跨模块契约测试）及 [`e2e`](../../e2e)（跨服务黑盒 E2E 测试）。
- **Storage 子模块（[`services/storage/go.mod`](../../services/storage/go.mod)）**：代码位于 `services/storage/internal/{config,db,meta,server}` 与 `services/storage/db/{migrations,queries}`。
  - `services/storage/internal/meta/` 文件职责索引：领域实体、常量与哨兵错误在 `entity.go`；状态码与 `ErrorInfo` 映射在 `status.go`；文件锁（Flock/POSIX 锁）在 `lock.go`；元数据与读写业务切分为 `manager.go` 及 `manager_{lookup_getattr,create_open,dir_topology,readdir_xattr,setattr_fallocate,handle_session,disk,read,write}.go`；物理块存储、切片编解码、合并与后台回收位于 `block_store.go`、`slice.go`、`compact.go`、`chunk_cleanup.go`、`gc.go`（PostgreSQL 集成测试统一通过 `testdb_helper_test.go` 中的 `setupMetaTestDB` 连库）。
- **Client 子模块（[`client/go.mod`](../../client/go.mod)）**：Linux FUSE 客户端 CLI，代码位于 `client/{cmd/bowl,internal,e2e}`；macOS 宿主机下须通过 `task test:client` 与 `task test:client:e2e` 在 Linux 容器内完成构建与挂载测试。
- **ADR 查阅约定**：查阅 [`docs/adr/`](../adr) 时，先列出目录获取准确文件名，切勿盲猜文件名。

## Worktree 与模块隔离红线

根目录 `go.work` 已 gitignore。在 `.worktrees/<ticket>` 内执行 `go`（含 `go test`、`go tool cover`）或 `mockery` 命令时，必须通过根目录 [`Taskfile.yml`](../../Taskfile.yml)（已内置 `GOWORK=off`）或显式指定 `GOWORK=off` 并带 `-C services/storage` 或 `-C client` 运行，切勿在根模块直接跑子模块路径或无隔离运行。

本机只有一套 bowl 容器栈：[`docker-compose.yaml.example`](../../docker-compose.yaml.example) 固定了容器名（`bowl-postgres` 等）与端口，`Taskfile.yml` 已固定 `COMPOSE_PROJECT_NAME=bowl`。一律通过 `task` 操作容器栈（绕过 `task` 直接调用 `docker compose` 时显式带 `-p bowl`）；容器已在运行时直接复用。多个任务共用同一容器栈时（并行修复节点、深度审查汇合验证），以 `KEEP_E2E_ENV=1 task test:e2e` / `KEEP_E2E_ENV=1 task test:client:e2e` 运行 E2E，避免收尾时关停他人正在使用的容器栈。

## 提交前守门

[`.githooks/pre-commit`](../../.githooks/pre-commit) 对暂存的 Go 文件执行 `gofmt -s` 检查，新克隆仓库先运行一次 `git config core.hooksPath .githooks` 启用（所有工作树共享）。

## 常用任务 (`task`)

- **测试与契约校验**：
  - `task test` / `task test:storage`：运行根模块（含 `pkg/validator`、`tests/contract`）与 `services/storage` 测试。
  - `task test:client` / `task test:client:e2e`：macOS 宿主机下在 Linux 容器内完成 `client` FUSE 客户端构建、单测与挂载 E2E 测试。
  - `task test:e2e`：运行跨服务黑盒 E2E 测试。
  - 修改服务接口或状态码后，提交前必须运行 `task test` 与 `task lint` 确保根模块 `tests/contract` 同步通过。
- **静态检查与 Protobuf 代码生成**：
  - `task lint` / `task buf:lint` / `task buf:generate`：静态检查、校验并生成 `proto/` 下的 ConnectRPC 契约代码。
- **数据库访问与迁移（sqlc / goose）**：
  - `task sqlc:generate`（或 `task sqlc:storage:generate`）：生成 sqlc 数据库访问代码。
  - `task goose:storage:up`（及 `down` / `status`）：执行数据库迁移（默认 DSN：`postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable`，对应本地容器 `bowl-postgres`）。
- **Mock 代码生成**：
  - `task mockery:storage`：生成接口 Mock 代码（必须在接口定义落盘且编译通过后执行）。

## 跨仓事务契约与 Mock 变参打桩规范

- **事务管理依赖**：依赖 `github.com/starsrc/Aether/pkg/tx`（`Transactor[Querier]`、`ExecTx`、`WithReadOnly()`、`WithIsoLevel(...)`）。
- **`mockery` 变参展开 (`unroll-variadic: true`)**：[`services/storage/.mockery.yml`](../../services/storage/.mockery.yml) 启用了 `unroll-variadic: true`。对 `ExecTxWithOptions(ctx, fn, opts ...tx.Option)` 打桩时：
  - 若被测代码**未传 `opts`**，底层 `mock.On` 仅匹配 `(ctx, fn)` 两个参数；
  - 若被测代码**传入了 `opts`**，则按展开后的变参顺序逐个匹配。
