# 0014. Disk 磁盘管理服务、容量配额账本与 StatFS 契约设计

## 状态
已被 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 替代 (superseded by ADR-0015) — 早期部分替代 [ADR-0002](0002-disk-concept-and-client-root-inode-mapping.md)

## 背景
在 [ADR-0002](0002-disk-concept-and-client-root-inode-mapping.md) 中，系统早期假定服务端完全不维护任何 `Disk` 实体表，仅由客户端承载 `Disk` 概念。随着商业化能力演进，系统需要按独立 `Disk`（UUID 标识）提供字节级容量限制（文件大小配额控制，不限制 Inode 数量）并对外支撑 POSIX `StatFS` 查询，同时后续将由独立网关（Gateway）项目对外接收 `disk_id` 并将其映射转换为对应的根目录 `Inode`（`root_ino`）。

若在 `storage` 元数据服务的每个 `Inode` 上冗余 `disk_id` 或在每次 `StatFS` 时通过递归 CTE 聚合整棵目录树的文件大小，不仅会破坏 `storage` 元数据引擎的纯粹 POSIX 模型，还会导致大目录树下 `StatFS` 与配额校验性能退化。同时，为保持 `storage` 与 `disk` 服务的低耦合与易维护性，不在 `storage` 服务中额外引入跨服务内网 RPC，而是让 `disk` 服务独立维护 `Disk` 实体与容量配额账本。

## 决策
1. **三层职责解耦（演进 ADR-0002）**：
   - **`storage` 服务（`proto/storage/v1/storage.proto`）**：继续保持纯粹的 POSIX 元数据引擎定位，仅操作 `uint64 ino`，不感知 `disk_id` 与商业化容量配额。
   - **`disk` 服务（`proto/disk/v1/disk.proto` 与 `services/disk`）**：作为独立服务承载商业化 `Disk` 实体，维护 `disk_id`（UUIDv7）、`name`、一对一绑定的根目录 `root_ino`（`uint64`，由调用方传入并保证唯一性约束）、容量配额上限 `max_bytes` 及当前已用字节数 `used_bytes`。
   - **外部 `gateway` 项目**：负责面向用户态接收 `disk_id`，通过 `DiskService` 解析 `disk_id -> root_ino` 映射，并在文件大小变更链路中联动 `DiskService` 执行配额校验与账本增减。
2. **O(1) 容量配额账本与边界状态机**：
   - `disks` 表直接维护 `max_bytes BIGINT NOT NULL` 与 `used_bytes BIGINT NOT NULL DEFAULT 0`，并施加数据库级 `CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes)` 约束。
   - **扩容与缩容约束（`UpdateDisk`）**：修改 `max_bytes` 时采用悲观行锁（`FOR UPDATE`）校验；**严禁将 `max_bytes` 缩容至低于当前 `used_bytes`**（若 `new_max_bytes < used_bytes`，拒绝并返回 `FailedPrecondition` / `EINVAL`）。
   - **空间增减与下溢保护（`UpdateDiskUsage`）**：按带符号增量 `delta_bytes`（`int64`）原子更新 `used_bytes`：
     - 当 `delta_bytes > 0` 且 `used_bytes + delta_bytes > max_bytes` 时，拒绝变更并返回 gRPC `ResourceExhausted`（携带 `ErrorInfo{Reason: "ENOSPC", Domain: "bowl.disk"}`）；
     - 当 `delta_bytes < 0` 且 `used_bytes + delta_bytes < 0` 时，将 `used_bytes` 钳位归零（`0`），防止删除回收链路因账本微小偏差而阻塞。
3. **`StatFS` 块统计确定性换算**：
   - `DiskService.StatFS` 接收 `disk_id`，以 `O(1)` 主键查询返回字节级容量（`total_bytes = max_bytes`、`used_bytes`、`avail_bytes = max_bytes - used_bytes`）与基于 `4096` 字节块大小（`bsize = 4096`、`frsize = 4096`、`namelen = 255`）计算的块指标：
     - `blocks = max(total_bytes / 4096, 1)`；
     - `bavail = avail_bytes / 4096`，`bfree = bavail`；
     - 因不做 Inode 数量限制，`files = 0`、`ffree = 0`。
4. **数据库迁移隔离**：
   - `services/disk` 拥有独立的 `sqlc.yaml`、`.mockery.yml` 与 `services/disk/db/migrations`；在共享开发数据库 `bowl` 中，Goose 迁移使用独立版本表 `goose_db_version_disk` 与 `storage` 迁移隔离。

## 结果
- **积极影响**：`storage` 元数据引擎保持零侵入；`StatFS` 与配额扣减均为单行 $O(1)$ 操作；数据库约束与业务状态机双重保障 `0 <= used_bytes <= max_bytes` 强不变量。
- **代价考量**：外部 Gateway 在执行文件截断、扩容、预分配与删除时需协调调用 `DiskService.UpdateDiskUsage` 同步账本。
