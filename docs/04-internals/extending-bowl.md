# 扩展指南：在仓库中开发新功能

本章是配方集，每节给出真实文件路径、`task` 命令与核对清单。示例名称（`GetDiskUsage`、`bowl disk inspect`）为演示用的虚构名，仓库中不存在；步骤与文件位置均为真实。先读 [控制面](/04-internals/storage-control-plane)、[Store 引擎](/04-internals/storage-store-engine)、[客户端流水线](/04-internals/client-fuse-pipeline)。

::: warning 仓库级红线（来自根目录 `AGENTS.md` 与 `Taskfile.yml`）
- 仓库含 3 个 Go 模块：根模块（`gen/`、`pkg/`、`e2e/`）、`services/storage`、`client`，由 `go.work` 联合。`Taskfile.yml` 全局设置 `GOWORK=off`，命令行手动执行时请自行加 `GOWORK=off`，与 Docker 构建一致。
- **不要手改生成物**：`gen/go/storage/v1/*`、`services/storage/internal/db/metadata.sql.go`、`models.go`、`db.go`、`docs/03-interfaces/rpc-api.md`。
- Linux FUSE 相关测试只能在 Linux 容器里跑：`task test:client`、`task test:client:e2e`。
:::

## 1. 端到端新增或修改一个 RPC

以新增只读 RPC `GetDiskUsage(disk_id) → used_bytes, used_inodes` 为例。数据流为：`proto` → 生成代码 → `validator` → `store`（如需）→ `control` → `server` → `client`。

```mermaid
flowchart LR
    P["proto/storage/v1/storage.proto"] -->|"task buf:generate"| G["gen/go/storage/v1 (pb.go + connect)"]
    P -->|"task docs:gen"| D["docs/03-interfaces/rpc-api.md"]
    G --> V["pkg/validator/validator.go (storageMessages)"]
    G --> S["server/handler_*.go"]
    S --> C["control.Manager"]
    C --> E["store.StoreActor (数据面才需要)"]
    G --> CL["client: fusefs 或 cli"]
```

### 1.1 步骤与文件

| 序号 | 动作 | 文件与命令 |
| --- | --- | --- |
| 1 | 在 `service StorageService` 中加 `rpc`，补中文注释（含错误码与 errno 契约）；新增 `GetDiskUsageRequest` / `GetDiskUsageResponse` | `proto/storage/v1/storage.proto`。请求字段 `disk_id` 照抄 `GetDiskRequest`（约 `1577` 行）的 `string.uuid = true` 加"非 Nil UUID" CEL 规则 |
| 2 | 格式化与规范检查 | `task buf:format`、`task buf:lint`（规则 `STANDARD`，豁免 `RPC_RESPONSE_STANDARD_NAME`，见 `buf.yaml`） |
| 3 | 生成 Go 代码 | `task buf:generate`（`buf.gen.yaml` 用 `buf.build` 远程插件，**需联网**；`clean: true` 会重建 `gen/go/`） |
| 4 | 重新生成 RPC 手册 | `task docs:gen`，随后 `task docs:build` 验证站点可构建 |
| 5 | 注册校验 | `pkg/validator/validator.go` 的 `storageMessages` 追加新请求与响应消息 |
| 6 | 数据面才需要：引擎方法 | `services/storage/internal/store/` 加方法；写操作还要加 `opKind`、`String()`、`validateCmd` 与 `mutateCmd` 的 `case`，对外部写加入 `isExternalWriteOp`（`actor.go:49-129`、`actor.go:1390`） |
| 7 | 控制面接口与实现 | `services/storage/internal/control/control.go`：`Manager` 接口（`:165`）加方法，`manager` 实现 |
| 8 | 服务端处理器 | `services/storage/internal/server/handler_disk_node.go` 或同类 `handler_*.go` 加 `func (s *Server) GetDiskUsage(...)`；`var _ storagev1connect.StorageServiceHandler = (*Server)(nil)`（`server.go:23`）会在漏实现时编译失败 |
| 9 | 客户端调用 | FUSE 路径在 `client/internal/fusefs/` 调 `fs.client.GetDiskUsage`；CLI 路径见 §2 |
| 10 | 测试夹具 | `client/internal/fusefs/fake_storage_test.go` 的 `fakeStorage`（`var _ storagev1connect.StorageServiceClient = (*fakeStorage)(nil)`）必须补实现，否则整个 `fusefs` 包测试编译失败 |

处理器范式（`server/handler_disk_node.go`）：

```go
func (s *Server) GetDiskUsage(ctx context.Context, req *connect.Request[storagev1.GetDiskUsageRequest]) (*connect.Response[storagev1.GetDiskUsageResponse], error) {
	disk, err := s.control.GetDisk(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.GetDiskUsageResponse{
		UsedBytes:  disk.UsedBytes,
		UsedInodes: disk.UsedInodes,
	}), nil
}
```

数据面 RPC 的范式见 `server/handler_data.go`：`validateWriteEpoch` → `s.control.Actor(ctx, diskID)` → `ctx.Err()` 检查 → `actor.Xxx` → `toConnectError`。所有错误必须经 `toConnectError`，不要直接返回 `connect.NewError`，否则丢失 `ErrorInfo.Reason`。

### 1.2 新 RPC 的"分类表"核对

下列 5 处列表按过程名硬编码，漏改不会编译失败，只会静默走默认行为：

| 位置 | 作用 | 何时要改 |
| --- | --- | --- |
| `server/interceptor.go` 的 `isControlPlaneProcedure`（`:116`） | 控制面慢请求阈值 `200ms`，否则按 `50ms` | 新 RPC 属于控制面 |
| `server/interceptor.go` 的 `classifyStorageLogLevel`（`:86`） | 成功日志级别：列表内为 Info，其余 Debug | 低频高价值变更操作 |
| `pkg/validator/validator.go` 的 `classifyViolationInternal` | 校验失败映射为 `EINVAL` 以外的 errno（`ERANGE`、`E2BIG`、`ENAMETOOLONG`） | 新字段有长度或范围约束 |
| `services/storage/internal/control/status.go` 的 `mappings`（`:25`） | 新增领域哨兵错误 → gRPC code 与 errno reason | 新增 `ErrXxx` |
| `client/internal/rpc/retry.go` 的 `idempotentProcedures`（`:80`） | 允许对 `Unavailable` 与传输错误重试 | **仅**严格只读或幂等的 RPC；写类 RPC 加入会造成重复提交 |

新增 errno `Reason` 时还要同步客户端 `client/internal/rpc/errors.go` 的 `MapError` 分支（目前 25 类），否则退化为 Connect 状态码兜底。

### 1.3 兼容性约束

- `buf.yaml` 启用 `breaking: FILE`。新增 RPC、新增字段、新增消息是安全的；改字段号、类型、删字段会被 `buf breaking` 判为破坏性变更。按仓库规则，涉及契约的破坏性变更需人工决策，不要自行绕过。
- 合并冲突、挂载租约等业务语义在 `proto` 注释中是契约的一部分，改动要同步 [RPC 接口手册](/03-interfaces/rpc-api) 之外的手写章节（如 [磁盘生命周期](/02-features/disk-lifecycle)）。

### 1.4 核对清单

- [ ] `task buf:lint` 通过；`task buf:generate` 后 `git status` 只多出 `gen/` 变更。
- [ ] `pkg/validator/validator.go` 已登记消息；`go test ./pkg/validator/...` 通过。
- [ ] `server` 单测覆盖：参数非法被拦截、领域错误映射正确（参照 `handler_disk_test.go`、`handler_fs_test.go`）。
- [ ] `fakeStorage` 已实现；`task test:client` 通过。
- [ ] 上表 5 处分类列表逐项确认过。
- [ ] 改动了 `pkg/` 或 `gen/` 后执行 `task tidy`（容器构建依赖 `vendor`，见根目录 `AGENTS.md`）。
- [ ] `task docs:gen` 已执行并提交 `docs/03-interfaces/rpc-api.md`。
- [ ] 在 `e2e/storage_e2e_test.go` 同风格补一条 `//go:build e2e` 用例（§4）。

## 2. 新增一个 `bowl disk` 子命令

`bowl disk` 的解析与执行是"规格 + 适配器"模式，公共部分在 `client/internal/cli/disk.go` 的 `runDiskSubcommand`（`:325`），每个子命令只写自己的 `diskSubcommandSpec` 与 `diskRequestAdapter`。

### 2.1 公共流程（`runDiskSubcommand` 已处理，不要重复实现）

1. 解析公共标志：`-h/--help`、`-q/--quiet`（仅 `allowQuiet` 为真时可用）、`--json`、`--endpoint`、`--token`；其余标志交给 `adapter.parseFlag`。
2. 校验位置参数个数（由 `posArgLabel` 决定）、`-q` 与 `--json` 互斥、`uuidLabel` 非空时校验第 1 个位置参数为非 Nil UUID。
3. `adapter.finalize(posArgs)` → `adapter.preRPC(ports, stderr)`（本地前置守卫）。
4. 开 Span（`spanName` 与 `adapter.spanAttrs()`）→ `ports.newClient` → 带 `spec.timeout` 的上下文 → `adapter.invoke`。
5. 按 `-q`、`--json`、`renderHuman`、默认详情的顺序输出。

### 2.2 步骤

以 `bowl disk inspect <disk-id>` 为例，新建 `client/internal/cli/disk_inspect.go`，对照 `disk_get.go`（最小模板）：

```go
var diskInspectSpec = diskSubcommandSpec{
	name:        "inspect",
	usageHelp:   diskInspectUsageHelp, // 中文用法文本，列出标志
	posArgLabel: "<disk-id>",
	uuidLabel:   "disk-id",
	spanName:    "cli.disk.inspect",
	timeout:     10 * time.Second,
	newAdapter:  func() diskRequestAdapter { return &diskInspectAdapter{} },
}
```

适配器需实现 5 个方法（接口见 `disk.go:78`）：

| 方法 | 职责 | 返回约定 |
| --- | --- | --- |
| `parseFlag(name, val, hasVal, args, idx)` | 解析子命令专属标志；用 `takeFlagValue` 取值 | `handled=false` 表示未知标志，由框架报错退出码 2 |
| `finalize(posArgs)` | 取位置参数，做跨标志校验 | `missUsage=true` 时附带用法文本；错误退出码 2 |
| `spanAttrs()` | Span 属性，如 `bowl.disk_id` | |
| `preRPC(ports, stderr)` | RPC 前的本地守卫，如 `ports.findMountpoint(diskID)` 检查本机挂载（`disk_delete.go:72`） | 返回 `*diskCLIError`，`exitCode` 为 1（运行时）或 2（参数） |
| `invoke(ctx, client)` | 发 RPC，转换为 `diskOutcome` | 失败用 `mapDiskRPCError(err, diskRPCErrorPolicy{...})` 统一映射 |

然后做两处注册，二者都在 `disk.go`：

1. `diskUsageHelp` 常量（`:27`）的"可用子命令"列表加一行 `inspect`。
2. `executeDiskWithPorts` 的 `switch subcmd`（`:286`）加 `case "inspect": spec = &diskInspectSpec`。

约定：

- 新输出结构放进 `diskOutcome`：`disk`（走通用详情）、`quietID`（`-q` 输出）、`jsonPayload`（`--json`）、`renderHuman`（自定义人类可读渲染）。
- 退出码：`0` 成功，`1` 运行时失败，`2` 参数或用法错误；与 [CLI 参考](/03-interfaces/cli-reference) 一致。
- 子命令必须是 `bowl disk` 的子项；顶层命令（如 `mount`）在 `cli/cli.go` 的 `Execute` 注册，不属于本配方。

### 2.3 核对清单

- [ ] 在 `client/internal/cli/disk_test.go` 补测试，沿用该文件的 `runCLI` 辅助函数；至少覆盖：`--help` 退出码 0、非法 UUID 退出码 2、RPC 成功输出、`NotFound` 映射、`--json`。
- [ ] 路径含 `client/*.go`，提交时 `.githooks/pre-commit` 会触发 `golangci-lint [client]`。
- [ ] 更新手写文档 `docs/03-interfaces/cli-reference.md`。
- [ ] 若同时新增 RPC，先完成 §1。

## 3. 新增 PostgreSQL 迁移与 `sqlc` 查询

控制面表结构只有 `disks`、`disk_lineages`、`disk_commits` 三张。改动需要同时改 4 处：迁移、查询、`SQLStore` 实现、`MemStore` 实现，并由契约测试夹住。

### 3.1 步骤

| 序号 | 动作 | 文件与命令 |
| --- | --- | --- |
| 1 | 新建迁移 | `services/storage/db/migrations/`，文件名 `YYYYMMDDHHMMSS_描述.sql`（时间戳必须大于现有最大值，当前最新是 `20261112210000_...`）；也可 `goose -dir services/storage/db/migrations create 描述 sql` 生成 |
| 2 | 写 Up 与 Down | 以 `-- +goose Up`、`-- +goose StatementBegin` / `StatementEnd`、`-- +goose Down` 分段，Down 必须可逆（参照 `20261112210000_add_disk_lineage_and_fork_dag.sql` 的结构） |
| 3 | 应用迁移到本地库 | `task goose:storage:up`（DSN 取 `STORAGE_DB_DSN`，默认 `postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable`）；回滚 `task goose:storage:down`；查看 `task goose:storage:status` |
| 4 | 写查询 | `services/storage/db/queries/metadata.sql`，`-- name: Xxx :one` / `:many` / `:exec` / `:execrows` 注释声明；具名参数用 `@name::type` |
| 5 | 生成代码 | `task sqlc:generate`（`services/storage/sqlc.yaml`，schema 读 `db/migrations`）。只会更新 `internal/db/metadata.sql.go` 与 `models.go` |
| 6 | 扩展 `db.Store` 接口 | `services/storage/internal/db/store.go:100`：加方法；实现 `SQLStore` 方法，事务统一走 `s.tx.ExecTxWithOptions(..., tx.WithDefaultRetry())`，注意锁序（见 [控制面 §4.3](/04-internals/storage-control-plane)）；领域哨兵错误经 `mapPGXErr` 转换 |
| 7 | **同步 `MemStore`** | `services/storage/internal/db/mem_store.go`：为同一方法写进程内实现，语义（哨兵错误、排序、分页、原子性）与 SQL 一致 |
| 8 | 契约测试 | `services/storage/internal/db/store_contract_test.go` 新增 `TestStoreContract_Given...` 用例，使用 `runStoreAdapters`，让 `MemStore` 与 `SQLStore` 各跑一遍 |
| 9 | 隔离库同步 | 若新增**表**，`testdb_helper_test.go` 的 `cloneDDL` 只克隆 `disk_lineages`、`disk_commits`、`disks`，新表要加入，否则 `SQLStore` 子测试因缺表失败 |
| 10 | 控制面接入 | `control` 通过 `db.Store` 调用，并在 `control/status.go` 的 `mapDBErr` 与 `mappings` 补错误映射 |

### 3.2 运行契约测试

`SQLStore` 子测试要真实 PostgreSQL。本地起库后执行：

```bash
# 启动 dev 栈（含 PostgreSQL，默认映射 127.0.0.1:9432，migration 服务已自动执行 up）
task run
# 按需覆盖 DSN，优先级: STORAGE_TEST_DSN > STORAGE_DB_DSN > DATABASE_URL > BOWL_TEST_POSTGRES_DSN > TEST_DB_DSN
STORAGE_TEST_DSN='postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable' \
  GOWORK=off go -C services/storage test ./internal/db/... -run TestStoreContract -v
```

每个测试创建独立 schema 并在结束时 `DROP SCHEMA ... CASCADE`，可并发且不污染 `public`。

### 3.3 核对清单

- [ ] 迁移同时有 Up 与 Down，且在空库上 `up` 后 `down` 再 `up` 无错。
- [ ] `task sqlc:generate` 后 `git diff` 中只有 `metadata.sql.go`、`models.go`（及 `db.go` 若有变）发生变化，且未手改。
- [ ] `MemStore` 与 `SQLStore` 在新增契约用例下**都**通过。
- [ ] `control` 单测（使用 `db.NewMemStore()`，可用 `SetFailNextWrite` 注入失败）覆盖回滚路径。
- [ ] 容器栈：`deploy/dev/docker-compose.yaml` 的 `migration` 服务只读挂载 `services/storage/db/migrations` 并在 `task run` 时自动执行 `up`，新迁移无需改镜像。

## 4. 验证金字塔

由快到慢逐层放大，下层绿了再跑上层。

| 层 | 命令 | 覆盖 | 前置条件 |
| --- | --- | --- | --- |
| 1 模块单测 | `task test`（根模块 `go test -v ./...` 加 `task test:storage`） | `pkg/`、`gen/`、`services/storage` 全部包 | `db` 契约测试需 PostgreSQL（见 §3.2） |
| 1' 单包快测 | `GOWORK=off go -C services/storage test ./internal/store/...` | 引擎单包 | 无 |
| 2 客户端单测 | `task test:client` | `client` 全部包，Linux 容器内编译并 `go test` | Docker；任务依赖 `vendor:client` 与 `build:client:test` |
| 3 跨服务 E2E | `task test:e2e` | 根目录 `e2e/`（`//go:build e2e`），对真实 `storage-service` 容器 | Docker；任务自动 `docker compose up --build --wait storage-service`，结束时 `down`；`KEEP_E2E_ENV=1` 保留环境 |
| 4 客户端 FUSE E2E | `task test:client:e2e` | `client/e2e/`（`//go:build e2e && linux`）：守护进程生命周期、POSIX 语义、多客户端 fork、merge/diff | Docker 特权容器（`--device /dev/fuse`、`SYS_ADMIN`、`LINUX_IMMUTABLE`）；同样支持 `KEEP_E2E_ENV=1` |
| 5 基准与一致性 | `benchmark/{pjdfstest,drive9,fork,juicefs}` | POSIX 合规、与 drive9 / JuiceFS 对比、fork 性能与一致性 | **阿里云双机**，见下 |
| 6 提交守门 | `.githooks/pre-commit` | 暂存区触发的 `buf lint` 与 `golangci-lint` | `task hooks:install` |

可向 `task` 命令追加测试参数，例如 `task test:storage -- -run TestStoreContract`、`task test:client:e2e -- -run Daemon`（`CLI_ARGS` 透传）。

### 4.1 基准套件（层 5）

| 目录 | 目的 |
| --- | --- |
| `benchmark/pjdfstest` | 238 个 `.t` 用例的 POSIX 合规回归 |
| `benchmark/drive9` | 与 drive9 对比的 FUSE 性能基准 |
| `benchmark/fork` | `ForkDisk` 性能、在线并发一致性与写时复制 |
| `benchmark/juicefs` | 与 JuiceFS 的 `juicefs bench`、`fio`、`mdtest` 对比 |

四者的 `run_benchmark.sh` 都会在阿里云按量开通双机、部署 `deploy/prod` 栈、跑测试、收割遥测并**自动销毁云资源**，运行：

```bash
VPC_ID=vpc-xxx VSWITCH_ID=vsw-xxx SECURITY_GROUP_ID=sg-xxx bash benchmark/fork/run_benchmark.sh
```

::: danger 会产生云费用
不要在日常开发循环里运行。仅在改动数据路径（`store`、`rpc` 传输、预读与暂存上传）并需要性能或合规回归证据时执行，并确认账号凭据与同可用区网络参数。脚本内置 `trap cleanup EXIT` 做强制销毁与零残留核验，仍应在结束后到控制台复核。
:::

不开云也能验证解析与报告逻辑（各目录 README 的"基于已有结果重新生成"小节），以 `fork` 为例：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v \
  benchmark/common/test_collect_telemetry.py \
  benchmark/fork/test_parse_results.py
```

### 4.2 提交守门（层 6）

`task hooks:install` 即 `git config core.hooksPath .githooks`，主仓与所有 worktree 共享。`.githooks/pre-commit` 只读检查，不改写文件，且**不跑测试**；它按暂存区路径决定检查范围：

| 暂存路径 | 触发 |
| --- | --- |
| `*.proto`、`buf.yaml`、`buf.gen.yaml`、`buf.lock` | `buf lint` |
| `services/storage/*.go`、`go.mod`、`go.sum` | `golangci-lint [services/storage]` |
| `client/*.go` 等 | `golangci-lint [client]`（`GOOS=linux CGO_ENABLED=0`） |
| `gen/*.go`、`pkg/*.go` | 根模块、storage、client **三者联检**（共享契约变更波及下游） |
| `.golangci.yml`、根 `go.mod` / `go.sum` | 三模块全检 |

缺少 `buf` 或 `golangci-lint` 命令时直接失败并提示安装。全量静态检查可手动执行 `task lint`。

### 4.3 按改动选择最小验证集

| 改动 | 最小验证集 |
| --- | --- |
| 仅 `store` 引擎内部 | `go -C services/storage test ./internal/store/...`，再 `task test:e2e` |
| `proto`、校验、`server` | `task buf:lint`、`task test`、`task test:client`、`task test:e2e` |
| `db` 迁移与查询 | §3.3 清单加 `task test` |
| `fusefs` 缓存与屏障 | `task test:client`、`task test:client:e2e`；涉及性能再跑 `benchmark/pjdfstest` 与 `benchmark/drive9` |
| 仅文档 | `task docs:gen`、`task docs:build` |

### 4.4 发布前总清单

- [ ] `task buf:lint`、`task lint` 通过。
- [ ] `task test` 与 `task test:client` 通过。
- [ ] `task test:e2e` 与 `task test:client:e2e` 通过。
- [ ] 数据路径改动已附基准对比证据。
- [ ] `docs/` 手写章节与 `task docs:gen` 产物已同步。
