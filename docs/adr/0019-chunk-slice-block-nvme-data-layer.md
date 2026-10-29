# 0019. 基于 Chunk-Slice-Block 三层不可变切片、PostgreSQL 元数据合并与 NVMe SSD 落盘的数据层架构

## 状态
已采纳 (accepted) — 演进 [ADR-0010](0010-fuse-directory-stream-and-fallocate-metadata-semantics.md) 与 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md)

## 背景
在完成 `StorageService` 的 36 个 POSIX 元数据与 `Disk` 容量配额管理闭环（ADR-0001 至 ADR-0018）后，系统需构建真正的文件数据读写层（Data Layer）。在确立数据层架构时，面临四项核心系统约束与长远演进要求：
1. **兼顾本地 NVMe SSD 极速落盘与未来 S3 异步分层（Write-back Tiering）**：数据需优先写入本地 NVMe SSD 以提供微秒至毫秒级 `Write` / `Fsync` 响应，后续需无缝支持后台异步同步至对象存储（S3）并将本地 NVMe 作为热数据读缓存；若在本地 NVMe 采用可变大文件原地随机覆写（In-place Overwrite），向不可变的 S3 同步时将产生严重的整块重复上传写放大与并发读撕裂。
2. **原生支撑毫秒级版本控制与克隆（Snapshot & Clone）**：系统长远规划支持文件与整块 `Disk` 的版本控制及零拷贝克隆。这要求逻辑文件寻址 `(ino, chunk_index)` 与底层物理数据块解耦，物理数据块一旦落盘必须保持不可变（Immutable）并通过引用计数共享。
3. **服务边界与客户端复杂度控制**：需决定是由胖客户端自行切分数据块直写底层存储，还是将 `Read` / `Write` 统一内聚至 `StorageService` 内部闭环。
4. **追加写模型下的商业配额（`Disk.used_bytes`）与存量元数据事务联动**：在追加写切片（`Slice`）模型下，后台执行 `Compaction` 清理历史覆盖碎片前会存在短暂的物理空间冗余；同时 `SetAttr`（截断）、`Open(O_TRUNC)`、`Fallocate`（打洞）、`Unlink`、`Rename`、`Release`、`RenewSession` 与 `DeleteDisk` 均需与数据切片生命周期保持事务一致性且不被物理磁盘删除 I/O 阻塞。

## 决策
1. **内聚服务端数据面（`StorageService.Read` / `Write`）与 `4 MiB` 载荷上限**：
   - 在 `proto/storage/v1/storage.proto` 的 `StorageService` 中新增 `Read` 与 `Write` 接口（对齐 Linux FUSE `FUSE_READ` 与 `FUSE_WRITE`），单次读写字节上限设定为 `4 MiB`（`4,194,304` 字节，与单个物理 `Block` 上限严格对齐）。
   - FUSE 客户端保持无状态薄封装，仅转发 `(ino, fh, offset, size/data, session_id)`；分块切分、NVMe 落盘、PostgreSQL 元数据合并、后台 `Compaction` 与异步 GC 完全内聚为 `StorageService` 的内部实现。
   - **POSIX 权限与写入联动**：`Read` 要求常规文件且 `FileHandle` 非 `O_WRONLY`（否则返回 `EBADF`）；`Write` 要求常规文件且 `FileHandle` 为 `O_WRONLY` 或 `O_RDWR`（否则返回 `EBADF`）。若 `FileHandle` 携带 `O_APPEND`，服务端在数据库写事务内锁定 `Inode` 后强制将写入偏移锚定至当前最新 `inode.Size`；非空写入自动刷新 `mtime` 与 `ctime`，并剥离常规文件的 `S_ISUID` 与 `S_ISGID` 特权位。
2. **`Chunk (64 MiB) -> Slice (24B 元数据描述符) -> Block (<= 4 MiB)` 三层不可变切片模型**：
   - **Chunk（64 MiB 逻辑寻址抽屉）**：在 PostgreSQL 中新建 `chunks(ino, indx, slices)` 表（主键 `(ino, indx)`，其中 `indx = offset / 64MiB` 采用 64 位 `BIGINT` 即 Go `int64` 非负范围 `0 <= indx <= MaxInt64 / 64MiB`，完整支撑 `MaxInt64` 8 EiB 稀疏文件逻辑寻址上限）。
   - **Slice（变长写入补丁）**：每次连续写入分配全局唯一递增的 `slice_id`（`slice_id = 0` 专用于表示稀疏空洞 Hole）。在 `chunks.slices`（`BYTEA` 列）中按时间顺序原子追加 24 字节大端序定长记录：`[pos:4B][id:8B][size:4B][off:4B][len:4B]`。
   - **Block（$\le 4\text{ MiB}$ 物理碎片文件）**：每个 `Slice` 的实际字节载荷按最大 `4 MiB` 切分为不可变的物理 `Block` 文件，通过抽象的 `BlockStore` 接口持久化至本地 NVMe SSD 的两级十六进制散列目录（`chunks/%02X/%v/<slice_id>_<block_idx>_<block_size>`），写入临时文件并 `fdatasync` 后通过 `rename` 原子发布。
   - **元数据层合并（`buildSlice`）**：`Read` 时从 PostgreSQL 读取目标 `(ino, indx)` 的 `slices` 序列，在内存中通过区间裁剪树将历史重叠 `Slice` 裁剪合并为无重叠的扁平有效视图；命中的空洞区间（`id == 0` 或未写入区间）直接在内存填零返回，有效区间并发从 `BlockStore` 读取对应 `Block` 的字节切片。
3. **预埋 `slice_refs` 引用计数表与分阶段演进底座**：
   - 第一阶段聚焦交付本地 NVMe SSD 数据层完整读写、元数据合并、后台 `Compaction` 与异步 GC 闭环，同时在 PostgreSQL 中一步到位建立 `slice_refs(slice_id, size, refs)` 引用计数表与 `BlockStore` 存储抽象接口。
   - 新写入的 `Slice` 在事务内登记 `refs = 1`；这为后续第二阶段接入「本地 NVMe Write-back 异步上传 S3」以及「零拷贝克隆（`refs + 1`）与版本快照」提供零返工的持久化底座。
4. **持久化承诺边界与商业配额（`Disk.used_bytes`）逻辑大小记账**：
   - **持久化边界**：`Write` 与 `Fsync` 以「物理 `Block` 在本地 NVMe SSD 落盘完成且 PostgreSQL 事务已提交」作为持久化成功边界。
   - **配额记账口径**：继续遵循 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 按常规文件的**逻辑大小 `size`** 核算 `Disk.used_bytes`。当 `Write` 导致 `new_size = max(old_size, write_offset + len) > old_size` 时，在同一数据库事务内锁定所属 `Disk` 校验并递增 `delta_bytes = new_size - old_size`；覆盖写产生的待合并历史 `Slice` 碎片由后台 `Compaction` 清理，不重复计入用户的商业配额。
5. **存量 7 类元数据操作单事务联动与后台异步 Compaction / GC**：
   - **截断与打洞**：`SetAttr`（缩小 `size`）与 `Open(O_TRUNC)` 在同一事务内删除完全越界的整块 `Chunk` 行（将其中包含的实心 `slice_id` 在 `slice_refs` 中递减 `refs`），并对半块截断边界或 `Fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)` 区间原子追加 `id = 0` 的空洞 `Slice`。
   - **节点与磁盘销毁**：`Unlink`、`Rename`（覆盖目标文件）、`Release`（孤儿节点回收）、`RenewSession`（超期孤儿回收）与 `DeleteDisk` 在物理删除 `Inode` 的同一事务内清除关联 `chunks` 行并递减对应 `slice_refs.refs`。
   - **后台 Compaction 与异步物理 GC**：当单个 `Chunk` 累积的 `Slice` 数量超过阈值时，后台 `Compaction` 将有效区间重写为连续新 `Slice`，通过乐观前缀比对（CAS）替换已合并前缀并将旧 `Slice` 的 `refs` 减 1；后台 GC 协程异步扫描 `slice_refs` 中 `refs <= 0` 的记录，调用 `BlockStore` 物理删除 NVMe SSD 上的 `Block` 文件并清理 `slice_refs` 行，确保前台数据库事务绝不被磁盘 `unlink` I/O 阻塞。同时，为保持两阶段写入热路径无额外事务开销，后台 GC 增加低频物理块目录扫描与数据库对账：对修改时间超过安全宽限期（默认 1 小时，可通过 `WithGCOrphanGracePeriod` 配置）且在 `slice_refs` 表中无记录的未登记孤儿物理块文件执行物理回收。

## 结果
- **积极影响**：通过不可变 `Slice/Block` 与元数据层 24 字节区间合并，用一套统一的存储模型同时打通了本地 NVMe SSD 高速读写、随机覆盖写、零磁盘重写打洞/截断、未来 S3 异步分层以及毫秒级克隆/快照；前台事务仅操作轻量级元数据与引用计数，消除了大文件截断或删除时的事务锁长尾延迟。
- **代价考量**：高频随机覆盖写会在单个 `Chunk` 内产生多层 `Slice` 记录，需由 `StorageService` 内置的后台 `Compaction` 与 GC 协程定期合并碎片并回收 `refs <= 0` 的物理 `Block` 文件。
