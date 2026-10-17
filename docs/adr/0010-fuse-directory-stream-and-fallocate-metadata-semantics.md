# 0010. FUSE 目录会话遍历游标合成与 Fallocate 元数据预分配及打洞语义设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在确立文件句柄生命周期（ADR-0009：`Create`、`Open`、`Flush`、`Fsync`、`Release`）后，需进一步在纯元数据层推进目录打开与遍历（`OpenDir`、`ReadDir`、`ReadDirPlus`、`ReleaseDir`）以及文件空间预分配与打洞（`Fallocate`）五项核心能力。在遵循 POSIX 与 Linux FUSE 协议规范时，面临四项关键架构权衡：
1. **目录句柄与文件句柄的存储模型与类型边界**：FUSE 协议在 `FUSE_OPENDIR`、`FUSE_READDIR`、`FUSE_READDIRPLUS` 与 `FUSE_RELEASEDIR` 中同样使用 64 位无符号 `fh` 标识会话。需决定是新建独立的目录句柄表，还是复用 `file_handles` 表并在领域层实施目录与非目录接口的正交隔离。
2. **`ReadDir` / `ReadDirPlus` 的 `.` 与 `..` 合成、确定性分页游标及内核缓存约定**：`entries` 表仅持久化真实子项而不存储 `"."` 与 `".."`，且主键为 `(parent_ino, name)`。如何在不改动 `entries` 表结构的前提下提供单调递增的 `offset` 游标，并在 `ReadDirPlus` 中兼顾父目录属性解析与 Linux 内核对 `"."` / `".."` 不缓存 dentry 的协议要求。
3. **`rmdir-whilst-open` 孤儿目录（`Orphan Inode`）的全生命周期闭环**：ADR-0008 规定 `Rmdir` 将空目录 `nlink` 置为 `0` 并保留 `inodes` 记录。需明确已解链目录在持有活跃 `FileHandle` 期间的二次 `OpenDir` 准入、`ReadDir` / `ReadDirPlus` 遍历返回值，以及最后一个句柄 `ReleaseDir` 时的物理回收机制。
4. **纯元数据层 `Fallocate` 的标志位矩阵与特权位剥离边界**：Linux `fallocate(2)` 包含多种模式掩码（默认扩展、`FALLOC_FL_KEEP_SIZE`、`FALLOC_FL_PUNCH_HOLE` 等）。需明确在尚未接入数据分块层时支持哪些模式组合、何时扩展 `size`、何时剥离 `S_ISUID` / `S_ISGID` 特权位以及何时刷新 `mtime` 与 `ctime`。

## 决策
1. **统一复用 `file_handles` 表并实施 `S_IFDIR` 正交类型隔离**：
   - `OpenDir` 与 `ReleaseDir` 直接复用 ADR-0009 引入的 `file_handles` 表与 `file_handle_id_seq` 序列。
   - 通过目标 `Inode` 的文件类型位严格隔离目录操作与文件操作：
     - `OpenDir`：要求目标 `Inode` 为 `S_IFDIR`（非目录返回 `ErrNotDir` / `ENOTDIR`），且打开标志位必须为只读（`flags & O_ACCMODE == O_RDONLY && flags & O_TRUNC == 0`，否则返回 `ErrInvalidArgument` / `EINVAL`）。
     - `ReadDir` / `ReadDirPlus` / `ReleaseDir`：若目标 `Inode` 非目录，统一返回 `ErrNotDir`（`ENOTDIR`）；若为目录但 `(fh, ino)` 不存在或不匹配，返回 `ErrBadFileHandle`（`EBADF`）。同时文件接口（`Release` / `Fallocate`）面对目录节点时返回 `ErrIsDir`（`EISDIR`），彻底阻断跨类型混用 `FileHandle`。
2. **基于确定性序号游标（`1` / `2` / `>=3`）的目录流读取与 `noatime` 只读快照事务**：
   - `ReadDir` 与 `ReadDirPlus` 全程运行在 `RepeatableRead` 只读事务中，遵循 `noatime` 策略不写库刷新目录 `atime`。
   - 游标编码约定：
     - `offset = 1`：固定合成 `"."`，指向当前目录 `Inode`。
     - `offset = 2`：固定合成 `".."`，通过 `GetParentInode(dir_ino)`（查询 `entries` 中 `ino = dir_ino` 绑定的父目录 `Inode`）获取；若未命中（如顶层根目录），则回退指向当前目录自身 `Inode`。
     - `offset >= 3`：对应 `entries JOIN inodes` 按 `ORDER BY e.name ASC` 确定性排序的真实子项，第 $k$ 个真实子项（从 $0$ 起计）的返回游标为 `next_offset = k + 3`（SQL 分页偏移量为 `OFFSET max(0, req.offset - 2)`）。
   - 在 `ReadDirPlus` 中，合成的 `"."` 与 `".."` 条目填充完整 `Inode` 属性，但将其 `entry_timeout_seconds` 显式置为 `0`（符合 Linux FUSE 内核不对 `.` 和 `..` 缓存 dentry 的规范），真实子项则正常赋予标准 `entry_timeout_seconds` 与 `attr_timeout_seconds`。
3. **孤儿目录（`nlink == 0`）的打开准入、空流返回与最终物理回收**：
   - `OpenDir`：在读写事务内对目标目录 `GetInodeForUpdate(ino)` 加排他锁；若 `nlink == 0`，仅当 `HasFileHandlesByIno(ino)` 为 `true` 时允许分配新 `FileHandle`，否则返回 `ErrNotFound`（`ENOENT`）。
   - `ReadDir` / `ReadDirPlus`：校验目录存在且 `(fh, ino)` 有效后，若发现目录 `nlink == 0`（处于 `rmdir-whilst-open` 状态），遵循 POSIX `readdir(3)` 规范直接返回空列表（不合成 `"."` 与 `".."`）。
   - `ReleaseDir`：在读写事务内 `GetInodeForUpdate(ino)` 并调用 `DeleteFileHandle(fh, ino)`；若目录 `nlink == 0` 且 `HasFileHandlesByIno(ino)` 为 `false`，在同一事务内调用 `DeleteInode(ino)` 物理删除该孤儿目录记录。
4. **`Fallocate` 模式位矩阵、可写句柄校验与差异化元数据更新**：
   - 新增领域哨兵错误 `ErrNotSupported`（映射 gRPC `Unimplemented` / POSIX `EOPNOTSUPP`），并定义平台无关的 Linux `fallocate` 模式常量：`FALLOC_FL_KEEP_SIZE = 0x01`、`FALLOC_FL_PUNCH_HOLE = 0x02`。
   - 在读写事务内 `GetInodeForUpdate(ino)` 并校验 `GetFileHandle(fh, ino)`：若句柄不存在返回 `ErrBadFileHandle`；若句柄以只读模式打开（`fh.Flags & O_ACCMODE == O_RDONLY`）同样返回 `ErrBadFileHandle`（`EBADF`）；若目标为目录返回 `ErrIsDir`（`EISDIR`），若为非常规文件（非 `S_IFREG`）返回 `ErrNotSupported`（`EOPNOTSUPP`）。
   - 模式分支语义：
     - `mode == 0`（默认预分配）：若 `offset + length > current.Size`，将 `size` 更新为 `offset + length` 并刷新 `mtime` 与 `ctime`（遵循 POSIX `posix_fallocate` 规范，不剥离 `S_ISUID` / `S_ISGID`）；若 `offset + length <= current.Size`，保持 `size` 不变，仅刷新 `ctime`。
     - `mode == FALLOC_FL_KEEP_SIZE`：保持 `size` 不变，仅刷新 `ctime`。
     - `mode == FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE`（区间打洞）：保持 `size` 不变，剥离常规文件的 `S_ISUID` 与 `S_ISGID` 特权位，并同时刷新 `mtime` 与 `ctime`。
     - 单独传入 `FALLOC_FL_PUNCH_HOLE`（未带 `FALLOC_FL_KEEP_SIZE`）返回 `ErrInvalidArgument`（`EINVAL`）；传入任何其他未知或未支持标志位返回 `ErrNotSupported`（`EOPNOTSUPP`）。

## 结果
- **积极影响**：在不引入额外表结构的前提下，完整闭环了目录会话生命周期、带属性预取的确定性分页目录遍历以及文件空间预分配/打洞的元数据状态机，且目录部分的逻辑在未来接入数据分块层后无需任何改动。
- **代价考量**：`ReadDir` / `ReadDirPlus` 采用基于 `ORDER BY name ASC` 的序号游标（`OFFSET`），在大目录深分页下会产生线性扫描开销，但契合当前单表 `(parent_ino, name)` B-Tree 索引结构且保证了实现简洁性。
