1. ceph, computer, juicefs, drive9 这四个项目为参考项目，他们是我找的权威的开源的一些项目，有可参考的内容（例如 JuiceFS 的 FUSE 适配位于 `pkg/fuse/`、VFS 语义位于 `pkg/vfs/`、元数据与锁/会话位于 `pkg/meta/{base,sql,sql_lock}.go`）。

## Build, Test & Tooling

- **多模块与目录布局**：
  - 根模块（[`go.mod`](go.mod)）：包含 `gen/go`（Protobuf 生成代码）、[`pkg/errcode`](pkg/errcode) 及 [`tests/contract`](tests/contract)（跨模块契约测试）。
  - Storage 子模块（[`services/storage/go.mod`](services/storage/go.mod)）：代码位于 `services/storage/internal/{meta,db}` 与 `services/storage/db/{migrations,queries}`（不存在 `internal/service`、`internal/handler` 或 `internal/store` 目录）。
  - 查阅 [`docs/adr/`](docs/adr) 时，先列出目录获取准确文件名，切勿盲猜文件名。
- **Worktree 与模块隔离**：根目录 [`go.work`](go.work) 已 gitignore。在 `.worktrees/<ticket>` 内执行 `go`（含 `go test`、`go tool cover`）或 `mockery` 命令时，必须通过 [`Taskfile.yml`](Taskfile.yml)（已内置 `GOWORK=off`）或显式指定 `GOWORK=off` 并带 `-C services/<mod>` 运行，切勿在根模块直接跑子模块路径或无隔离运行。
- **常用任务 (`task`)**：
  - `task test` / `task test:storage`：运行全量（含根模块 `tests/contract`）或 `storage` 服务测试。修改服务接口或状态码后，提交前必须运行 `task test` 与 `task lint` 确保根模块 `tests/contract` 同步通过。
  - `task lint` / `task buf:lint` / `task buf:generate`：静态检查、校验并生成 `proto/` 下的 ConnectRPC 契约代码。
  - `task sqlc:generate`（或 `task sqlc:storage:generate`）：生成 sqlc 数据库访问代码。
  - `task mockery:storage`：生成接口 Mock 代码（必须在接口定义落盘且编译通过后执行）。
  - `task goose:storage:up`（及 `down` / `status`）：执行数据库迁移（默认 DSN：`postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable`，共享本地容器 `aether-postgres`）。
- **跨仓事务契约与 Mock 规范**：
  - 事务管理依赖 `github.com/starsrc/Aether/pkg/tx`（`Transactor[Querier]`、`ExecTx`、`WithReadOnly()`、`WithIsoLevel(...)`）。
  - [`services/storage/.mockery.yml`](services/storage/.mockery.yml) 启用了 `unroll-variadic: true`。对 `ExecTxWithOptions(ctx, fn, opts ...tx.Option)` 打桩时，若被测代码未传 `opts`，底层 `mock.On` 仅匹配 `(ctx, fn)` 两个参数；若传入了 `opts`，则按展开后的变参顺序逐个匹配。

## Agent skills

### Issue tracker

Tracked in Linear (Team: `StarSrc`, Project: `Bowl`) via `ACE_linear` MCP. See [docs/agents/issue-tracker.md](docs/agents/issue-tracker.md).

### Triage labels

Canonical triage roles mapped to Linear labels. See [docs/agents/triage-labels.md](docs/agents/triage-labels.md).

### Domain docs

Single-context layout at repo root. See [docs/agents/domain.md](docs/agents/domain.md).
