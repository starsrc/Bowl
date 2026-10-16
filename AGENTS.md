## Build, Test & Tooling

- **多模块与 Worktree 隔离**：仓库包含根模块（`gen/go` Protobuf 生成代码）与 [`services/storage`](services/storage/go.mod) 子模块。根目录 [`go.work`](go.work) 已 gitignore。在 `.worktrees/<ticket>` 内执行 `go` 或 `mockery` 命令时，必须通过 [`Taskfile.yml`](Taskfile.yml)（已内置 `GOWORK=off`）或显式指定 `GOWORK=off`，切勿无隔离运行导致向上命中父仓库 `go.work`。
- **常用任务 (`task`)**：
  - `task test` / `task test:storage`：运行全量或 `storage` 服务测试。
  - `task buf:lint` / `task buf:generate`：校验并生成 `proto/` 下的 ConnectRPC 契约代码。
  - `task goose:storage:up` / `down` / `status`：执行 `services/storage/db/migrations` 数据库迁移（默认 DSN：`postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable`，共享本地容器 `aether-postgres`）。
- **跨仓事务契约与 Mock 规范**：
  - 事务管理依赖 `github.com/starsrc/Aether/pkg/tx`（`Transactor[Querier]`、`ExecTx`、`WithReadOnly()`、`WithIsoLevel(...)`）。
  - [`services/storage/.mockery.yaml`](services/storage/.mockery.yaml) 启用了 `unroll-variadic: true`。对 `ExecTx(ctx, fn, opts ...tx.Option)` 打桩时，若被测代码未传 `opts`，底层 `mock.On` 仅匹配 `(ctx, fn)` 两个参数；若传入了 `opts`，则按展开后的变参顺序逐个匹配。

## Agent skills

### Issue tracker

Tracked in Linear (Team: `StarSrc`, Project: `Bowl`) via `ACE_linear` MCP. See [docs/agents/issue-tracker.md](docs/agents/issue-tracker.md).

### Triage labels

Canonical triage roles mapped to Linear labels. See [docs/agents/triage-labels.md](docs/agents/triage-labels.md).

### Domain docs

Single-context layout at repo root. See [docs/agents/domain.md](docs/agents/domain.md).