# 磁盘生命周期

本章说明 Bowl 虚拟磁盘（Virtual Disk）的实体模型、配额记账规则、游标分页查询、在线扩缩容保护以及安全删除与血缘垃圾回收机制。

---

## 虚拟磁盘实体模型与配额记账

Bowl 采用**控制面元数据与单盘引擎状态分离**的双层模型：PostgreSQL 持久化跨盘拓扑、血缘关系与查询索引；单盘 `StoreActor` 内部的 Pebble 实例与 `.pack` 容器维护精确的文件系统实时用量与配额上限。

### 双层实体字段映射

| 字段 / 概念 | 持久化位置 | 编码与约束 | 说明 |
| :--- | :--- | :--- | :--- |
| `disk_id` | PostgreSQL `disks` | UUIDv7 主键（非 Nil UUID） | 全局唯一磁盘标识，单调递增的时间有序前缀支撑高性能分页 |
| `lineage_id` | PostgreSQL `disks` / `disk_lineages` | UUIDv7（NOT NULL，外键关联） | 磁盘所属衍生血缘树标识；全新创建磁盘独立分配新血缘，派生磁盘继承源盘血缘 |
| `parent_disk_id` | PostgreSQL `disks` | UUID（可空，`chk_disks_not_own_parent`） | 记录直接父盘 ID；独立创建的根盘为 `NULL` |
| `base_commit_id` | PostgreSQL `disks` | UUID（可空，复合外键至 `disk_commits`） | 该盘最近一次分支分叉或合并推进的基准提交点；清理血缘历史后可置空 |
| `name` | PostgreSQL `disks` | 长度 `0..255` 字节（允许空字符串，不可含 NUL） | 面向用户的可读磁盘名称；创建或修改时校验长度与合法性 |
| `root_ino` | 存储引擎常量 | 固定 `RootIno = 1` | 每块盘初始化时同步写入的根目录 Inode，模式 `S_IFDIR \| 0o755`，初始 `Nlink = 2` |
| `max_bytes` | Pebble `sys:stat`（`BranchStat`） | `uint64` 逻辑字节上限 | 通过 `CreateDisk`（校验 `&gt; 0`）写入，可通过 `UpdateDisk` 在线调整 |
| `used_bytes` | Pebble `sys:stat`（`BranchStat.LogicalBytes`） | `uint64` 实时逻辑已用字节 | 等于全盘存活常规文件与符号链接 `Inode.Size` 之和 |
| `used_inodes` | Pebble `sys:stat`（`BranchStat.Inodes`） | `uint64` 实时存活节点计数 | 含根目录（初始为 `1`）及所有存活节点（含未回收的 `orphan` 节点） |

> [!NOTE]
> `StoreActor` 在 Pebble 中将 `sys:stat` 编码为 49 字节定长结构 `BranchStat`（`[ver(1B) | LogicalBytes(8B) | Inodes(8B) | NextWriteSeq(8B) | WriterEpoch(8B) | MaxBytes(8B) | WriterExpiresAt(8B)]`）。详见 [存储引擎内部实现](/04-internals/storage-store-engine)。

### 创建初始化与原子回滚（`CreateDisk`）

`services/storage/internal/control/control.go` 中的 `CreateDisk` 按以下顺序确保引擎目录与控制面元数据的强一致：

1. **参数与 UUIDv7 生成**：校验名称长度（`0..255` 字节、允许空字符串且无 NUL）与 `max_bytes &gt; 0`（受 `proto/storage/v1/storage.proto` `buf.validate` 约束），生成 UUIDv7 作为 `disk_id`、`lineage_id` 与初始 `commit_id`。
2. **预建存储引擎实例**：在 `&lt;storage_root&gt;/disks/&lt;disk_id&gt;` 打开 `StoreActor`，写入 `RootIno = 1` 根目录节点与初始 `BranchStat`（`Inodes = 1`、`LogicalBytes = 0`、`WriterEpoch = 0`、`WriterExpiresAt = 0`、`MaxBytes = max_bytes`）。
3. **PostgreSQL 事务提交**：在 `SQLStore.CreateDisk` 事务内依次插入 `disk_lineages`（初始 `next_ino = 2`、`next_pack_id = 1`）与 `disks` 表。
4. **失败补偿清理**：若数据库事务提交失败，控制面立即关闭并丢弃刚创建的 `StoreActor`，递归删除物理目录 `&lt;storage_root&gt;/disks/&lt;disk_id&gt;`，杜绝无主数据目录残留。

### 配额记账语义与 `StatFS` 换算

Bowl 的容量配额采用**逻辑用量计费（Logical Accounting）**：

- **计费口径**：`used_bytes` 严格统计所有存活常规文件（`S_IFREG`）与符号链接（`S_IFLNK`）的 `Inode.Size` 逻辑大小总和，不以底层 `.pack` 物理文件大小或数据块写放大（如封包前覆盖写产生的死洞）计算，也不受稀疏空洞（Sparse Hole）底层未分配物理块的影响。
- **实时拦截**：在 `StoreActor` 单协程写循环的 Phase A 阶段通过 `checkQuota(bytesDelta, inodesDelta)` 校验；只要 `stat.LogicalBytes + bytesDelta &gt; max_bytes`，写请求、未携带 `FALLOC_FL_KEEP_SIZE` 的 `Fallocate` 扩容或 `SetAttr` 扩长操作均会立即失败并返回 `ErrNoSpace`（映射为 POSIX `ENOSPC` / gRPC `ResourceExhausted`）。
- **未关闭孤儿节点记账**：当进程仍持有句柄时文件被 `Unlink` 或 `Rename` 覆盖，节点转为 `orphan:&lt;ino&gt;` 形态并继续保留在 `LogicalBytes` 与 `Inodes` 中计费；直到最后一个本地句柄关闭触发 `ReclaimOrphan` 或重新读写挂载触发孤儿清理时，才会扣减逻辑用量。
- **`StatFS` 块换算**：`StatFS` 以 `DefaultStatFSBlockSize = 4096` 字节为块单位，`Blocks = max_bytes / 4096`，`Bfree = Bavail = max(0, max_bytes - used_bytes) / 4096`。当未显式配置节点数上限（`max_inodes == 0`）时，文件总数上限按逻辑可用块数或剩余可分配 Inode 空间的下限估计（参见 `services/storage/internal/store/actor.go`）。

---

## 游标分页列表（`ListDisks`）

`ListDisks` 接口提供稳定、无漂移的虚拟磁盘分页遍历能力，底层实现在 `services/storage/internal/db/store.go` 与 `services/storage/internal/db/cursor.go`。

### 排序与 Keyset 游标规则

- **确定性排序**：查询结果严格按照 `(created_at DESC, disk_id DESC)` 降序排列，依托迁移脚本 `20261108000000_simplify_single_disk_control_plane.sql` 建立的复合索引 `idx_disks_created_at_id` 实现索引扫描。由于 `disk_id` 为全局唯一主键，即使同一微秒内并发创建多块盘，排序依然全序唯一。
- **游标编码格式**：`page_token` 采用 Base64 RawURL（无 `=` 填充）编码的 JSON 载荷：

```json
{
  "t": 1731500000123456,
  "id": "019326a4-7b1e-7000-8000-000000000001"
}
```

- **微秒对齐防漂移**：字段 `t` 存储 `created_at.UTC().UnixMicro()`，与 PostgreSQL `TIMESTAMPTZ` 的微秒存储精度完全对齐，避免 Go `time.Time` 纳秒级尾数在比对 `(created_at, disk_id) &lt; ($1, $2)` 时引发翻页漏行或重复。
- **严格解码校验**：解码器启用 `DisallowUnknownFields()`，并拒绝非法的 Base64 编码、缺失字段或 `uuid.Nil`，异常令牌统一返回 `ErrInvalidArgument`（gRPC `InvalidArgument`）。

### 分页窗口与实时状态拼装

| 参数 / 行为 | 规格值 / 规则 | 实现位置 |
| :--- | :--- | :--- |
| 默认页大小 | `DefaultListDisksPageSize = 50`（传入 `0` 时采用） | `services/storage/internal/control/control.go` |
| 最大页大小 | `MaxListDisksPageSize = 1000`（超限由校验拦截返回 `InvalidArgument`） | `proto/storage/v1/storage.proto` |
| 下一页探测 | SQL `LIMIT page_size + 1`；若返回条数 `&gt; page_size`，截断为 `page_size` 条并用末条记录编码 `next_page_token` | `services/storage/internal/db/store.go` |
| 实时用量填充 | 控制面遍历 PG 返回记录，逐盘调用 `actor.StatFS()` 回填最新的 `max_bytes`、`used_bytes` 与 `used_inodes` | `services/storage/internal/control/control.go` |

---

## 在线扩缩容与缩容保护（`UpdateDisk`）

Bowl 支持在不卸载、不中断在线读写会话的前提下，通过 `UpdateDisk` 调整磁盘名称或逻辑容量上限 `max_bytes`。

### 变更字段与校验契约

- 请求中 `optional string name`（未传表示不改名；显式传入时支持 `0..255` 字节且允许为空字符串）与 `optional uint64 max_bytes`（未传表示不调整容量；显式传入时要求 `&gt; 0` 且 `&lt;= math.MaxInt64`）可单独或组合更新，至少须显式设置其中一项（`has(this.name) || has(this.max_bytes)`，由 `buf.validate` CEL 规则与控制面双重校验，否则返回 `InvalidArgument`）。
- 显式传入 `name` 时通过 `validateDiskName` 校验：长度 `0..255` 字节（`&gt; 255` 返回 `ErrNameTooLong`）且禁止包含 `\x00` 字符（返回 `ErrInvalidArgument`）。

### 执行顺序与缩容保护规则

`control.UpdateDisk` 在持盘锁（`lockDisk`）下按“引擎先校验持久化 -> 控制面落库 -> 异常补偿回滚”的两段式流程推进：

1. **缩容保护（Shrink Guard）**：若 `max_bytes &gt; 0`，控制面先向该盘 `StoreActor` 提交 `UpdateMaxBytes` 指令。`StoreActor` 在单协程写循环的 Phase A 阶段直接对比当前实时 `stat.LogicalBytes`：
   - 若 `newMaxBytes &lt; s.stat.LogicalBytes`，立即拒绝本次操作并返回 `store.ErrQuotaShrinkBelowUsed`。
   - 经 `services/storage/internal/control/status.go` 翻译后，对外返回 gRPC `codes.FailedPrecondition` 且携带结构化错误详情 `ErrorInfo{Reason: "EINVAL", Domain: "bowl.storage"}`。
2. **原子持久化与回滚**：仅当 `newMaxBytes &gt;= stat.LogicalBytes` 时，引擎更新 Pebble `sys:stat` 的 `MaxBytes` 并执行内存配额阈值原子切换；随后控制面更新 PostgreSQL `disks` 表的 `name` 与 `updated_at`。若 PG 更新失败且引擎配额此前已被修改，控制面自动调用 `actor.UpdateMaxBytes(prevMaxBytes)` 将引擎配额恢复至旧值。

> [!IMPORTANT]
> 由于缩容校验基准为 `stat.LogicalBytes`（逻辑用量）而非物理磁盘占用，若用户删除文件释放容量后仍存在已解链但尚未关闭句柄的孤儿文件（`orphan:&lt;ino&gt;`），其逻辑字节仍计入 `used_bytes`，需等文件句柄释放回收后方可继续下调 `max_bytes`。

---

## 安全删除与血缘级联清理（`DeleteDisk`）

磁盘删除涉及客户端挂载保护、控制面引擎销毁以及同血缘提交图（DAG）的条件垃圾回收。开发集成时需准确区分**客户端守卫边界**与**服务端实际行为**。

### 客户端挂载守卫与服务端删除行为边界

| 检查 / 执行层 | 实际行为与触发条件 | 源码位置 |
| :--- | :--- | :--- |
| CLI 本地挂载检测 | `bowl disk delete` 在发起 RPC 前通过 `ports.findMountpoint` 扫描本地已挂载记录；若目标磁盘仍挂载于本机路径，立即报错拦截并要求先执行卸载 | `client/internal/cli/disk_delete.go` |
| CLI 交互确认与 `EBUSY` 翻译 | 非 `-y/--yes` 调用要求输入 `yes` 确认；若服务端返回 `ErrorInfo{Reason: "EBUSY"}`（如未来或代理层拦截），CLI 提示磁盘忙并终止 | `client/internal/cli/disk_delete.go` |
| 服务端 `DeleteDisk` 租约处理 | Storage 控制面 `DeleteDisk` 不会在服务端因存在活跃写租约而返回 `EBUSY`；它直接销毁盘的 `StoreActor` 与磁盘目录（含 Pebble 中的 `WriterEpoch` 与 `WriterExpiresAt`） | `services/storage/internal/control/control.go` |
| 与 `MergeDisk` 的区别 | `MergeDisk` 在服务端强制校验活跃写租约门禁（`checkMergeMountGate`），仅当源盘或目标盘存在未过期读写租约时返回 `Aborted`（`Reason: "EBUSY"`）；只读挂载不阻塞合并 | `services/storage/internal/control/mount_lease.go` |

> [!WARNING]
> 若外部系统绕过 `bowl` CLI 直接调用 `DeleteDisk` gRPC 接口，服务端将直接销毁该盘存储引擎；仍在远端节点运行的读写或只读挂载客户端会在后续 I/O 或 `RenewMount` 心跳中收到 `NotFound`，读写守护进程会自动标记租约失效并触发自动卸载退出。因此，生产集成编排层应在调用 `DeleteDisk` RPC 前主动确认全集群无在途挂载或业务流量。

### 盘内清理与三态血缘提交 GC

`control.DeleteDisk` 按严格顺序完成关联残留对账、物理目录回收与血缘 DAG 清理：

1. **前置崩溃残留对账**：先调用 `reconcileOtherPendingMergesBeforeDelete`，将同一血缘下其他磁盘遗留的未完成合并临时目录先行前滚或回滚，避免误删仍在生效路径上的提交目录。
2. **数据库事务与三态 GC（`SQLStore.DeleteDisk`）**：在 `disk_lineages` 行锁保护下删除 `disks` 中的目标行，并统计同血缘剩余磁盘数 `remaining`：
   - **`remaining &gt;= 2`**：保留所有 `disk_commits` 记录与 `&lt;storage_root&gt;/commits/&lt;commit_id&gt;` 物理快照目录，确保同血缘其余多盘后续仍可基于最近共同祖先（LCA）执行 `DiffDisk` 与 `MergeDisk`。
   - **`remaining == 1`**：同血缘仅剩最后一块存活盘，跨分支三路比较已不再发生；事务内清空幸存盘的 `base_commit_id`，并删除该血缘下的全部 `disk_commits` 行。
   - **`remaining == 0`**：血缘内已无存活盘；删除该血缘下的全部 `disk_commits` 行并级联删除 `disk_lineages` 记录。
3. **存储引擎与物理文件回收**：
   - 控制面持盘锁关闭目标 `StoreActor`（`CloseDiscard`，跳过正常关停刷盘以加速销毁），删除 `&lt;storage_root&gt;/disks/&lt;disk_id&gt;` 目录及 `*.merge.*`、`*.old.*`、`*.srccommit.*` 暂存遗留。
   - 依据数据库返回的 `commitsToClean` 列表，在 `lineageCommitMu` 保护下物理删除已被 GC 的 `&lt;storage_root&gt;/commits/&lt;commit_id&gt;` 快照目录。由于 `.pack` 文件通过硬链接跨盘与跨提交共享，删除单个盘或提交目录时仅减少操作系统硬链接计数，其余存活分支共享的数据块仍然安全可读。

---

## 相关接口与实现参考

- 接口契约与错误码完整定义：参见 [RPC 接口契约](/03-interfaces/rpc-api) 与 [CLI 命令参考](/03-interfaces/cli-reference)。
- 血缘树提交演化、硬链接共享及三路合并机制：参见 [派生、差异与三路合并](/02-features/fork-diff-merge)。
- 挂载租约生命周期与 `MergeDisk` 门禁：参见 [挂载租约与写栅栏](/02-features/mount-and-fencing)。
