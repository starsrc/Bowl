# Storage 控制面与服务层

本章拆解 `services/storage` 进程的三层结构：`server`（ConnectRPC 接入层）、`control`（单盘 Actor 池与血缘协调）、`db`（PostgreSQL 持久化接缝）。数据面引擎见 [Store 引擎](/04-internals/storage-store-engine)，对外契约见 [RPC 接口手册](/03-interfaces/rpc-api)。

::: tip 阅读约定
文中 `文件:行号` 指向仓库真实源码，行号以当前主干为准；代码变动后以符号名为准。
:::

## 1. Uber Fx 模块依赖图

进程入口为 `services/storage/cmd/main.go` 的 `run()`。它先用 `config.Load()` 读取配置，再把 5 个 Fx 模块组装成一张依赖图，最后 `application.Start(ctx)` 启动。

| 模块 | 源码 | 提供的构造器 |
| --- | --- | --- |
| `config.Module` | `services/storage/internal/config/` | `ServerConfig`、`ObservabilityConfig`、`DBPoolConfig`、`MetaConfig` 等配置视图 |
| `obsfx.Module` | 外部包 `github.com/starsrc/pkg/observability/obsfx` | 隔离 Prometheus Registry、`observability.Ready`、Gatherer |
| `db.Module` | `services/storage/internal/db/store.go:657` | `ProvidePool` 返回 `*pgxpool.Pool`，`NewStore` 返回 `db.Store` |
| `control.Module` | `services/storage/internal/control/module.go:85` | `store.NewMetrics`、`newStorageEngineCollector`、`newManagerFromParams`（`control.Manager`） |
| `server.Module` | `services/storage/internal/server/module.go:15` | `NewHTTPMetrics`、`ConfigureFineBucketMeterProvider`、`ConfigureStorageTracerProvider`、`New`（`*Server`） |

```mermaid
flowchart TD
    CFG["config.Load() + fx.Supply"] --> CM["config.Module"]
    CM --> DBM["db.Module"]
    CM --> CTL["control.Module"]
    CM --> SRV["server.Module"]
    OBS["obsfx.Module (Registry / Ready / Gatherer)"] --> CTL
    OBS --> SRV
    DBM -->|"pgxpool.Pool"| STORE["db.Store (SQLStore)"]
    STORE --> MGR["control.Manager"]
    CTL --> MGR
    MGR -->|"control.Manager"| SERVER["server.Server"]
    DBM -->|"pgxpool.Pool (optional)"| SERVER
    SRV --> SERVER
    SERVER -->|"fx.Invoke 强制实例化"| LIFE["serverx.RegisterLifecycle (public + admin)"]
```

要点：

- `server.Module` 末尾的 `fx.Invoke(func(*Server) {})`（`module.go:23`）保证即使没有其他消费者，`*Server` 也会被构造并注册生命周期钩子。
- `control.Module` 的 `newManagerFromParams`（`control/module.go:30`）在 `fx.Lifecycle.OnStop` 中调用 `Manager.Close()`，关闭所有 Actor 并释放共享 Pebble Cache。
- 集成 PostgreSQL 的就绪探针在 `server.New` 中注册：存在 `*pgxpool.Pool` 时追加 `WithReadyCheck("postgres", Pool.Ping)`（`server.go:200-205`）。
- 关停超时由 `fx.StopTimeout(cfg.StopTimeout())` 控制；`SIGINT`/`SIGTERM` 经 `signal.NotifyContext` 触发优雅退出（`main.go:23`、`main.go:56-68`）。

## 2. `server` 层

### 2.1 拦截器链（外 → 内）

`server.New`（`server.go:131`）把拦截器按下列顺序交给 `connect.WithInterceptors`。`connect` 约定列表中靠前者位于更外层。

| 序号 | 拦截器 | 来源 | 作用 |
| --- | --- | --- | --- |
| 0 | `newWritePayloadReleaseInterceptor` | `server/codec.go:72` | 请求结束时归还写缓冲租约，位于最外层以覆盖所有失败路径 |
| 1 | `slogx.NewMaskInterceptor` | 外部包 `github.com/starsrc/pkg/observability/slogx` | 日志脱敏 |
| 2 | `otelconnect` | 外部包 `connectrpc.com/otelconnect`，由 `github.com/starsrc/pkg/serverx` 装配 | 链路与 RPC 指标 |
| 3 | `serverx.NewErrorReasonInterceptor` | 外部包 `github.com/starsrc/pkg/serverx` | 提取 `ErrorInfo.Reason` 写入 Span |
| 4 | `slogx.NewConnectInterceptor` | 外部包 `github.com/starsrc/pkg/observability/slogx`，由 `serverx` 装配 | 唯一访问日志；`classifyStorageLogLevel` 把高频成功路径降到 Debug，`Lookup` / `GetXattr` 的 `NotFound` 也降级 |
| 5 | `NewStorageObservabilityInterceptor` | `server/interceptor.go:218` | `pprof.Labels("rpc", procedure)`、`bowl.*` Span 属性、慢请求日志 |
| 6 | `NewValidationInterceptor` | `server/validator.go:73` | `protovalidate` 请求校验，位于最内层 |

序号 1 到 6 由 `BuildInterceptors`（`interceptor.go:326`）委托外部包 `github.com/starsrc/pkg/serverx` 的 `NewInterceptorsWithValidator` 构建；它们不在本仓库根目录 `pkg/`（根 `pkg/` 仅有 `rpccodec` 与 `validator`）。

慢请求阈值（`interceptor.go:26-29`）：数据面与元数据面 RPC 超过 `50ms`，控制面 RPC（`isControlPlaneProcedure` 列出的 13 个）超过 `200ms`，输出 `slow storage rpc` 警告。

### 2.2 租约池化二进制 Codec

热路径只有两类消息：`WriteRequest`（上行 4 MiB 载荷）与 `ReadResponse`（下行 4 MiB 载荷）。`pkg/rpccodec` 为它们提供零分配编解码。

**`pkg/rpccodec/codec.go`：`ProtoBinaryCodec`**

- 实现 `connect.Codec`，`Name()` 恒为 `proto`，`IsBinary()` 恒为 `true`。
- `CodecHooks` 有三个可选钩子：`MarshalAppend`、`AfterMarshal`、`Unmarshal`。钩子返回 `handled=false` 时回退到标准 `proto.Marshal` / `proto.Unmarshal`。

**`pkg/rpccodec/lease_pool.go`：`PayloadLeasePool[T]`**

| 能力 | 实现 |
| --- | --- |
| 缓冲规格 | `MaxPayloadSize = 4 << 20`，类型 `*[4 MiB]byte`（noscan，不被 GC 扫描） |
| 常驻槽位 | `freeCh` 有界通道，`retainedCap` 个缓冲免受 GC 驱逐；溢出走 `sync.Pool` |
| 租约登记 | `Register(msg, buf)` 以 `weak.Pointer[T]` 为键登记，并挂 `runtime.AddCleanup` 兜底 |
| 显式归还 | `Release(msg, clearData)` 先清空 `msg.Data` 再回收缓冲；无活跃租约时走无锁快路径 |
| 接管所有权 | `Claim(msg)` 返回 `Guard`，客户端读路径用它把缓冲带出消息生命周期 |
| 孤儿策略 | `OrphanRecycleBuffer`（服务端：对象被 GC 时回收缓冲）与 `OrphanDropEntryOnly`（客户端：只注销表项，防 UAF） |

**服务端接线（`server/codec.go`）**

- `writePayloadLeases` 与 `readPayloadLeases` 各常驻 `8 x 4 MiB = 32 MiB`（`codec.go:17-18`）。
- 解码：`unmarshalWriteRequest`（`codec.go:135`）手写 protobuf 线格式解析，字段 1 到 6 对应 `ino`、`fh`、`offset`、`data`、`disk_id`、`epoch`。`data` 字段不超过 4 MiB 时拷入租约缓冲；遇到未知字段、类型不符或非法 UTF-8，立即 `fallback()` 回标准 `proto.Unmarshal`。
- 编码：`marshalReadResponseHook`（`codec.go:100`）直接写 `0x0a` 标签、varint 长度与数据，随后 `releaseReadPayload` 归还缓冲。
- `Read` 处理器（`server/handler_data.go`）先 `acquireReadPayloadBuf()`，让 `actor.ReadInto` 直接把数据读进租约缓冲，再 `registerLeasedReadPayload` 登记；编码完成后由 `AfterMarshal` 钩子归还。

**写载荷释放拦截器**

`newWritePayloadReleaseInterceptor`（`codec.go:72-83`）只做一件事：若 `req.Any()` 是 `*WriteRequest`，`defer releaseWritePayload(msg)`。它在拦截器链最外层，因此校验被拒、业务出错、正常完成等所有路径都会归还 4 MiB 缓冲。单测用 `leasedWritePayloadCount()` 校验零泄漏。

### 2.3 HTTP/2 流控窗口

`server.go:25-36` 定义三个常量，`server.New` 通过 `http.HTTP2Config` 应用到公网 `http.Server`（`server.go:192-196`）：

| 常量 | 值 | 设计理由 |
| --- | --- | --- |
| `HTTP2MaxReadFrameSize` | `1 MiB` | 单帧上限，减少 4 MiB 载荷的分帧次数 |
| `HTTP2MaxReceiveBufferPerStream` | `8 MiB` | 4 MiB 载荷加 Protobuf 信封可整包接收，无需流级 `WINDOW_UPDATE` 停顿 |
| `HTTP2MaxReceiveBufferPerConnection` | `128 MiB` | 单连接可并行流水线接收多路 4 MiB 写请求，无连接级阻塞 |

客户端对应常量在 `client/internal/rpc/client.go:35-53`，数据条带连接窗口为 `64 MiB`、元数据连接为 `32 MiB`，见 [客户端流水线](/04-internals/client-fuse-pipeline)。

### 2.4 `protovalidate` 校验拦截器

`NewValidationInterceptor`（`server/validator.go:73`）的执行逻辑：

1. 先判断 `isValidFastPathDataReq`（`validator.go:39`）。`WriteRequest` 与 `ReadRequest` 的字段边界（`ino > 0`、偏移不溢出、载荷不超 4 MiB、`epoch > 0`、`disk_id` 为非 Nil UUID）用手写判断，命中则跳过 CEL，避免热路径反射开销。
2. 未命中快路径时调用 `protovalidate.Validator.Validate(msg)`。校验器由 `validator.NewStorageValidator()`（`pkg/validator/validator.go:181`）创建，用 `protovalidate.WithMessages(storageMessages...)` 在启动期预编译全部请求、响应与嵌套消息的 CEL 规则。
3. 校验失败时 `validator.ToStatus(err)` 生成 `codes.InvalidArgument` 状态，并附带 `google.rpc.ErrorInfo{Reason, Domain: "bowl.storage"}`，再经 `statusToConnectError` 返回，请求不会进入业务处理器。

`Reason` 由 `translateValidation` 按优先级挑出最强违规项，取值范围是 `EINVAL`、`ENAMETOOLONG`、`ERANGE`、`E2BIG`。`classifyViolationInternal` 按字段全名（如 `storage.v1.MountDiskRequest.disk_id`）分级。

::: warning 新增 RPC 必改清单
新增消息后，必须把消息类型加入 `pkg/validator/validator.go` 的 `storageMessages`，否则规则不会预编译。需要非 `EINVAL` 的 errno 时，还要在 `classifyViolationInternal` 追加字段映射。完整流程见 [扩展指南](/04-internals/extending-bowl)。
:::

### 2.5 gRPC 错误码映射

处理器统一用 `toConnectError(err)`（`server/errors.go:31`）返回错误，路径如下：

```mermaid
flowchart LR
    E["领域错误 / store 哨兵错误"] --> T["toConnectError"]
    T -->|"context.Canceled"| C1["connect.CodeCanceled"]
    T -->|"context.DeadlineExceeded"| C2["connect.CodeDeadlineExceeded"]
    T -->|"MergeConflictError"| C3["CodeAborted + ErrorInfo ECONFLICT + MergeConflictReport"]
    T -->|"其余"| S["control.ToStatus"]
    S --> M["mappings 表: 哨兵错误 -> code + errno reason"]
    M --> X["serverx.StatusToConnectError"]
```

`control.ToStatus` 定义在 `control/status.go:61`，按顺序执行如下分支：`MergeConflictError`，经 `errors.Is` 逐项匹配 `mappings` 表，透传已有 gRPC status，处理 `context` 错误，其余回落 `codes.Unknown`。`mappings` 定义在 `status.go:24-53`，代表性条目如下：

| 哨兵错误 | gRPC code | errno reason |
| --- | --- | --- |
| `store.ErrNotFound` | `NotFound` | `ENOENT` |
| `store.ErrExist` | `AlreadyExists` | `EEXIST` |
| `store.ErrStaleEpoch` | `FailedPrecondition` | `EBUSY` |
| `store.ErrNoSpace`、`store.ErrInodeExhausted` | `ResourceExhausted` | `ENOSPC` |
| `store.ErrReadOnly`、`store.ErrPerm` | `PermissionDenied` | `EPERM` |
| `store.ErrNotEmpty` | `FailedPrecondition` | `ENOTEMPTY` |
| `control.ErrCrossLineage` | `FailedPrecondition` | `EXDEV` |
| `control.ErrConcurrentMerge`、`control.ErrDiskMounted`、`control.ErrStaleBaseCommit` | `Aborted` | `EBUSY` |

客户端把同一组 `Reason` 还原为 `syscall.Errno`，见 [客户端流水线 §5](/04-internals/client-fuse-pipeline)。

## 3. `control.Manager`：单盘 Actor 池与协调

`Manager` 接口定义在 `control/control.go:165`。它对上承接 `server` 的业务调用，对下持有一张 `磁盘 ID → *store.StoreActor` 的池。

### 3.1 数据结构（`control.go:197-228`）

| 字段 | 类型 | 作用 |
| --- | --- | --- |
| `actors` | `map[string]*store.StoreActor` | Actor 池，键为规范化小写 UUID |
| `poolMu` | `sync.RWMutex` | 保护 `actors` 与 `closed` |
| `diskLocks` / `diskLocksMu` | 引用计数互斥锁表 | 单盘级串行化（`lockDisk`，`control.go:489`），引用归零即删除条目 |
| `lineageCommitMu` | `sync.RWMutex` | 保护 `commits/` 目录：合并读取时持读锁，`DeleteDisk` 回收时持写锁 |
| `cache` | `*pebble.Cache` | 所有 Actor 共享的 Pebble Block Cache，默认 `64 MiB`（`DefaultSharedCacheSize`） |
| `activeMounts` / `mountsMu` | `map[disk]map[mountID]mountLease` | 内存挂载租约表，默认 TTL `30s`（`DefaultMountLeaseTTL`） |

### 3.2 Actor 获取：三段式快路径

`Manager.Actor` 定义在 `control.go:1357`，是每个数据面 RPC 的入口：

1. 持 `poolMu.RLock` 直接查 `actors[diskID]`，命中即返回，热路径零分配。
2. 未命中则 `parseDiskID` 规范化，再次查池。
3. 仍未命中时先调用 `lockDisk` 按 `diskID` 加锁，复查池后再通过 `store.GetDisk` 取 `LineageID`，最后调用 `control.go:608` 的 `getOrOpenActorWithLineageLocked` 打开 Actor。打开期间不持 `poolMu`，写回池时二次检查，竞争失败者调用 `CloseDiscard()` 丢弃并返回已有实例。

打开 Actor 时会注入两个号段分配器（`newSegmentAllocators`，`control.go:402`）：Inode 号段步长 `store.InodeSegment = 4096`、Pack 号段步长 `store.PackSegment = 64`，二者通过 `db.Store.ReserveSegment` 在 PostgreSQL 中按血缘预留，保证同一血缘下各盘的 `ino` 与 `packID` 不冲突。预留使用 `context.WithoutCancel` 加 `5s` 超时（`segmentReserveTimeout`），避免客户端取消导致号段半分配。

### 3.3 租约与世代栅栏

- `MountDisk` 定义在 `control.go:1295`，读写挂载调用 `actor.AdvanceEpoch` 原子递增 `WriterEpoch`，再由 `issueMountLease` 签发 `mountID`，只读挂载不递增世代且 `Epoch = 0`。
- `RenewMount` / `UnmountDisk`（`mount_lease.go`）：租约保存在内存 `activeMounts`，过期条目由 `pruneExpiredDiskLeasesLocked` 惰性清理。
- 旧写者被世代栅栏隔离：`StoreActor.validateCmd` 对所有外部写操作比较 `cmd.epoch` 与当前 `WriterEpoch`，不一致返回 `ErrStaleEpoch`（映射为 `EBUSY`）。机制细节见 [挂载与世代栅栏](/02-features/mount-and-fencing)。
- `checkMergeMountGate` 在非 `DryRun` 且非 `Force` 的合并前，检查源盘与目标盘是否仍有活跃挂载租约，否则返回 `ErrDiskMounted`。

### 3.4 PostgreSQL 血缘提交协调

控制面把"文件系统目录状态"与"PostgreSQL 血缘 DAG"两份状态做原子对账。顺序统一为：**先物化目录，再提交数据库事务，失败则回滚目录**。

**`ForkDisk`，见 `control.go:769`**

```mermaid
sequenceDiagram
    participant C as "control.manager"
    participant S as "源盘 StoreActor"
    participant FS as "文件系统 commits/ 与 disks/"
    participant PG as "PostgreSQL"
    C->>C: lockDisk(源盘) 后 lockDisk(子盘)
    C->>S: Checkpoint(commits/commitID)
    S-->>FS: Pebble Checkpoint + 硬链接 pack
    opt 存在孤儿节点
        C->>FS: SanitizeCheckpoint
    end
    C->>FS: CloneCheckpointDir 到 disks/childID
    C->>C: store.Open(ResetEpoch=true) 打开子盘
    C->>PG: store.ForkDisk 事务
    alt 事务失败
        C->>FS: 回滚删除 child 与 commit 目录
    else 成功
        C->>C: 子盘 Actor 入池
    end
```

**`MergeDisk`（`merge_diff.go:453`）两阶段提交**

| 阶段 | 动作 | 保护 |
| --- | --- | --- |
| 前置 | `lockDiskPair` 同时锁两盘；`findLCA` 求最近公共祖先；`checkMergeMountGate` | 两盘 ID 排序加锁，防死锁 |
| Phase 1 | 目标盘保持在线；两盘各做只读快照；`store.DiffTrees` / `store.MergeTrees` 在 `StagingDir` 演算；物化 `commits/` 切面与 `srccommit` 崩溃标记 | 持 `lineageCommitMu.RLock` |
| Phase 2 | 摘除并关闭旧目标 Actor；`InspectBranchStat` 比较 `(WriterEpoch, NextWriteSeq)` 水位，变化则 `ErrConcurrentMerge`；`db.Store.MergeDisk` 事务提交；`os.Rename` 把 `disks/target` 换成 staging 目录；重新打开 Actor | CAS 校验加数据库期望基线提交校验 |

数据库事务内再执行一次 CAS 校验，`SQLStore.MergeDisk` 位于 `db/store.go:415`，校验两盘 `BaseCommitID` 与期望值一致，否则返回 `db.ErrStaleBaseCommit`，上层映射为 `Aborted` 与 `EBUSY`。

**崩溃恢复。** `openDiskActor` 位于 `control.go:580`，在打开读写 Actor 前调用 `reconcilePendingMergeLocked`，扫描 `disks/` 中残留的 `.merge.*`、`.old.*` 与 `.srccommit.*` 产物，以 PostgreSQL 记录的 `BaseCommitID` 为准前滚或回滚。`DeleteDisk` 在清理血缘前也会通过 `reconcileOtherPendingMergesBeforeDelete` 协助他盘完成挂起合并。

**`DeleteDisk`，见 `control.go:1169`。** 先执行 `store.DeleteDisk` 事务返回需回收的 `commitsToClean`，再摘除并 `CloseDiscard` Actor 与删除盘目录，最后持 `lineageCommitMu.Lock` 逐个删除 `commits/` 目录。任一清理失败会汇总为 `errors.Join` 返回，并计入 `incCleanupError` 指标。

## 4. `db` 抽象层

### 4.1 `Store` 接缝

`db.Store` 接口定义在 `db/store.go:102`，是控制面唯一的持久化抽象，共 9 个方法：`CreateDisk`、`ForkDisk`、`GetDisk`、`ListDisks`、`ListLineageCommits`、`UpdateDiskName`、`MergeDisk`、`DeleteDisk` 与 `ReserveSegment`。上层只依赖这个接口。

| 实现 | 文件 | 用途 |
| --- | --- | --- |
| `SQLStore` | `db/store.go` | 生产实现，`sqlc` 生成的 `Queries` 加 `tx.PGXTransactor` 事务封装 |
| `MemStore` | `db/mem_store.go` | 进程内实现，用于 `control` 与 `server` 单测，`SetFailNextWrite` 注入写失败，`Inspect` 取深拷贝快照 |

`db` 包在 `db/store.go:25-31` 导出 5 个领域哨兵错误：`ErrNotFound`、`ErrAlreadyExists`、`ErrInvalidArgument`、`ErrCrossLineage` 与 `ErrStaleBaseCommit`。`SQLStore` 的全部 9 个公开方法均只在最外层出口调用一次 `mapPGXErr`，该函数定义在 `db/store.go:206-231`；事务闭包内部直接返回原始错误，使重试层能继续识别 `40001` 串行化冲突与 `40P01` 死锁错误。

`mapPGXErr` 使用 `errors.AsType[*pgconn.PgError]` 吞并底层驱动类型，防止 `pgx.ErrNoRows` 或 `*pgconn.PgError` 泄露至调用方，映射规则如下：

| 底层驱动错误或 SQLSTATE | 领域哨兵错误 | 控制面与 gRPC 映射 |
| --- | --- | --- |
| `pgx.ErrNoRows` | `db.ErrNotFound` | `NotFound` 与 `ENOENT` |
| `23505` 唯一约束冲突 | `db.ErrAlreadyExists` | `AlreadyExists` 与 `EEXIST` |
| `23503` 外键约束冲突 | `db.ErrNotFound` | `NotFound` 与 `ENOENT` |
| `23514` 检查约束冲突 | `db.ErrInvalidArgument` | `InvalidArgument` 与 `EINVAL` |
| `40001` 与 `40P01` | 原样透传供事务重试 | 不吞并原始 `*pgconn.PgError` |

`MemStore` 在 `CreateDisk` 血缘存活冲突、`ForkDisk` 与 `MergeDisk` 自指父提交校验、`ReserveSegment` 写失败注入及全部冲突与缺失路径上与 `SQLStore` 保持对称哨兵契约。

### 4.2 `sqlc` 与迁移

- 配置：`services/storage/sqlc.yaml`，schema 取 `db/migrations`，查询取 `db/queries`，输出到 `internal/db`，驱动为 `pgx/v5`。
- 查询源文件：`services/storage/db/queries/metadata.sql`，含 `CreateDiskLineage`、`LockDiskLineage`、`ReserveInodeSegment`、`ReservePackSegment`、`CreateDiskCommit`、`GetDiskForUpdate`、`SetDiskBaseCommit`、`ClearLineageBaseCommits` 等。
- 生成物：`internal/db/metadata.sql.go`、`models.go`、`db.go`，文件头为 `Code generated by sqlc. DO NOT EDIT.`，禁止手改。
- 迁移：`services/storage/db/migrations/` 下的 Goose 文件，血缘表结构见 `20261112210000_add_disk_lineage_and_fork_dag.sql`。

### 4.3 事务与锁序

`SQLStore` 的写操作都在 `ExecTxWithOptions(..., tx.WithDefaultRetry())` 内执行，序列化冲突自动重试。锁序固定，防止死锁：

| 操作 | 锁序 |
| --- | --- |
| `ForkDisk`，见 `store.go:262` | `GetDisk` 取血缘 → `LockDiskLineage` → `GetDiskForUpdate` → `CreateDiskCommit` → `SetDiskBaseCommit` → `CreateDisk` |
| `MergeDisk`，见 `store.go:415` | `LockDiskLineage` → 两盘按 UUID 字符串升序 `GetDiskForUpdate` → 校验血缘与基线 → 建提交 → `SetDiskBaseCommitAndTouch` |
| `DeleteDisk`，见 `store.go:507` | `disk_lineages` → `disks`，并执行三态血缘提交回收 |
| `ReserveSegment`，见 `store.go:585` | 对 `disk_lineages` 执行 `ReserveInodeSegment` 或 `ReservePackSegment` 单条 `UPDATE ... RETURNING`，由 `tx.Retry` 包装 |

### 4.4 契约互验：`store_contract_test.go`

`MemStore` 与 `SQLStore` 必须行为一致。`db/store_contract_test.go` 的 `storeAdapters()` 返回两个适配器，`runStoreAdapters` 对每个契约用例分别以 `MemStore` 与 `SQLStore` 子测试各跑一遍：

```go
func runStoreAdapters(t *testing.T, fn func(t *testing.T, s db.Store, ctx context.Context))
```

契约用例覆盖：创建与读取、非法入参的哨兵错误、游标分页稳定性、三态血缘回收、号段单调且不相交、并发 `Fork`/`Delete` 无死锁、`Merge` 的 DAG 推进与原子失败。

`SQLStore` 子测试需要真实 PostgreSQL。`testdb_helper_test.go` 按顺序读取环境变量 `STORAGE_TEST_DSN`、`STORAGE_DB_DSN`、`DATABASE_URL`、`BOWL_TEST_POSTGRES_DSN`、`TEST_DB_DSN`，全部缺失时使用 `postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable`。每个测试用独立 schema 隔离，并克隆 `public` 下的 `disk_lineages`、`disk_commits`、`disks` 三张表。

::: warning 修改 `Store` 接口时
改接口方法、哨兵错误或迁移后，必须同时更新 `mem_store.go` 并让 `store_contract_test.go` 两个适配器都通过。只改 SQL 实现会让 `control` 单测（用 `MemStore`）假绿。实操配方见 [扩展指南 §3](/04-internals/extending-bowl)。
:::
