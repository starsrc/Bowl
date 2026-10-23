# 0015. 合并 Disk 与 Storage 服务及单事务容量配额账本设计

## 状态
已采纳 (accepted) — 替代 [ADR-0014](0014-disk-service-and-quota-ledger.md)，演进 [ADR-0002](0002-disk-concept-and-client-root-inode-mapping.md)

## 背景
在 [ADR-0014](0014-disk-service-and-quota-ledger.md) 中，系统曾将商业化 `Disk` 实体、容量配额账本（`UpdateDiskUsage`）与 `StatFS` 拆分为独立的 `DiskService`（`services/disk`），而文件系统的 `Inode` 大小变更与孤儿节点（`Orphan Inode`）回收则驻留在 `StorageService`（`services/storage`）。

这一双服务拆分在实践中暴露出严重的接缝错位（Shallow Seam）与账本失步风险：
1. **跨服务非原子双写与竞态**：文件大小变更分布在 `SetAttr`、`Open(O_TRUNC)`、`Fallocate`、`Unlink`、`Rename`（覆盖目标文件）、`Release`（关闭末个句柄触发孤儿回收）与 `RenewSession`（会话超时批量清理孤儿）共 7 类元数据操作中。外部调用方无法获知被覆盖或被截断文件的旧大小、硬链接数是否归零、以及是否存在活跃 `FileHandle` 延迟回收，更无法感知后台 `RenewSession` 异步回收的孤儿文件字节数。
2. **协议寻址割裂**：Linux FUSE `FUSE_STATFS`（opcode 17）与其他全部 POSIX 操作一样按 `uint64 ino` 寻址，将 `StatFS` 放在 `DiskService` 按 UUID 寻址破坏了数据面协议的一致性。
3. **建盘流程割裂**：`CreateDisk` 要求外部调用方预先构造并传入 `root_ino`，无法在单次事务中原子保证根目录 `Inode` 的真实存在与配额绑定。

## 决策
1. **服务二合一（裁撤独立 `DiskService`，统一收归 `StorageService`）**：
   - 移除独立的 `proto/disk/v1` 契约与 `services/disk` 子模块，将 `Disk` 生命周期管理（`CreateDisk`、`GetDisk`、`UpdateDisk`、`DeleteDisk`）与 `StatFS` 统一并入 `StorageService`（`proto/storage/v1/storage.proto` 与 `services/storage`）。
   - 移除对外暴露的手动增量记账接口 `UpdateDiskUsage`，容量与节点配额的校验及账本增减完全内聚为 `StorageService` 的内部事务不变量。
2. **原子化建盘与强制级联删盘**：
   - **`CreateDisk`**：不再由调用方传入 `root_ino`。`StorageService` 在单次数据库事务内自动创建根目录 `Inode`（`mode = S_IFDIR | 0755`，`nlink = 2`，`parent_ino = root_ino`，`root_ino = 自身 ino`），并创建绑定该 `root_ino` 的 `Disk` 记录（含 `max_bytes`、`used_bytes = 0`、`used_inodes = 1`）。
   - **`DeleteDisk`**：支持强制删除。即使目标 `Disk` 下存在非空目录树或活跃 `FileHandle` / `FileLock`，也在单次数据库事务内通过 `root_ino` 级联销毁该 `Disk` 下的全部 `Inode`（关联的 `entries`、`inode_xattrs`、`file_handles`、`file_locks` 随外键级联清理）与 `Disk` 记录，并唤醒清理内存中的阻塞锁等待队列。
3. **$O(1)$ 根 Inode 归属追踪与单事务配额核算**：
   - 每个 `Inode` 持久化其所属的磁盘根目录标识 `root_ino`（根目录自身的 `root_ino = ino`，子目录与文件在 `Mknod`/`Mkdir`/`Create`/`Symlink`/`TmpFile` 创建时直接从父目录继承 `root_ino`）。
   - **空间增减强一致**：在 `SetAttr`、`Open(O_TRUNC)`、`Fallocate` 变更文件 `size` 的同一数据库事务内，锁定所属 `Disk` 行校验 `used_bytes + delta_bytes <= max_bytes`（超限返回 `ResourceExhausted` / `ENOSPC`）并原子更新 `used_bytes`。
   - **物理销毁绑定配额退还**：当 `Unlink` 或 `Rename` 覆盖导致非目录 `Inode` 的 `nlink == 0` 时，若当前无活跃 `FileHandle`，当场在事务内物理删除该 `Inode` 并扣减 `used_bytes` 与 `used_inodes`；若仍存在活跃 `FileHandle`（转为 `Orphan Inode`），则暂不退还配额，直至最后一个 `FileHandle` 执行 `Release` 或会话过期由 `RenewSession` 批量物理删除该 `Orphan Inode` 时，在同一事务中精确归还其 `size` 与 `used_inodes`。
   - **跨盘拓扑隔离（`EXDEV`）**：`Link` 与 `Rename`（含 `RENAME_NOREPLACE` 与 `RENAME_EXCHANGE`）在持有源/目标父目录与节点行锁后，强制校验参与操作的节点与父目录的 `root_ino` 必须完全一致；若检测到跨 `Disk`（`root_ino` 不一致）的硬链接或重命名/交换请求，立即拒绝并返回 `ErrCrossDevice`（映射为 gRPC `FailedPrecondition` 与 `ErrorInfo{Reason: "EXDEV", Domain: "bowl.storage"}`），确保单盘 `root_ino` 配额归属与 `DeleteDisk` 按 `root_ino` 级联清理的隔离性不变量绝不被跨盘引用破坏。
4. **`StatFS(ino)` 寻址统一与块/节点指标换算**：
   - `StorageService.StatFS` 接收 `uint64 ino`，通过目标 `Inode` 的 `root_ino` 关联查询所属 `Disk` 的 `max_bytes`、`used_bytes` 与 `used_inodes`，以 `4096` 字节块大小换算返回 `blocks`、`bfree`、`bavail` 及真实已用节点数 `files = used_inodes`。

## 结果
- **积极影响**：消除跨服务配额双写、并发竞态与孤儿配额泄漏；删减一个多余的微服务模块及跨服务胶水层；FUSE 数据面寻址与商业磁盘生命周期在同一深模块（Deep Module）内闭环。
- **代价考量**：`StorageService` 的数据库 Schema 统一承载 `disks` 表与 `inodes.root_ino` 归属列。
