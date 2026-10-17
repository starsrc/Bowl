# 0009. FUSE 文件句柄生命周期持久化、原子打开截断与孤儿节点物理回收设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在完成目录树拓扑操作（`Lookup`、`GetAttr`、`SetAttr`、`Mknod`、`Mkdir`、`Symlink`、`Readlink`、`Link`、`Unlink`、`Rmdir`、`Rename`）后，需要推进文件打开会话生命周期相关的五项核心能力：`Create`、`Open`、`Flush`、`Fsync` 与 `Release`。在遵循严格 POSIX 与 Linux FUSE 协议规范时，面临四项关键架构权衡：
1. **文件句柄（`FileHandle` / `fh`）状态归属**：FUSE 协议在 `Open` 与 `Create` 时向内核返回 64 位无符号句柄 `fh`，并在后续 `Flush`、`Fsync`、`Release` 中携带 `(ino, fh)`。若服务端采用无状态固定 `fh` 或仅依赖进程内存维护会话，在多进程并发打开同一文件、服务重启或跨节点调用时无法准确追踪活跃句柄，也无法拦截已关闭或跨节点伪造的非法 `fh`（POSIX `EBADF`）。
2. **`unlink-whilst-open` 孤儿节点（`Orphan Inode`）的最终物理回收闭环**：ADR-0004 与 ADR-0008 确立了 `Unlink` 与 `Rename` 覆盖时保留 `nlink == 0` 的 `inodes` 记录以支撑打开期间的属性访问，但尚未定义最后一个 `FileHandle` 关闭（`Release`）时的物理清理机制，以及 `nlink == 0` 节点在何种条件下允许被重新 `Open`（如通过 `/proc/self/fd`）。
3. **跨平台 `open(2)` 标志位掩码差异与原子操作边界**：宿主机操作系统（如 Linux 与 macOS）标准库中的 `O_CREAT`、`O_TRUNC` 等常量数值不一致；同时 `Open` 需在服务端支持 `O_TRUNC` 原子截断及特权位剥离，并区分目录（`EISDIR`）、符号链接（`EINVAL`）与特殊设备文件的打开语义。
4. **高频 `Flush` / `Fsync` 的并发锁开销**：同一个 `FileHandle` 因 `dup(2)` / `fork(2)` 可能触发多次 `Flush` 且不销毁句柄；若在 `Flush` 与 `Fsync` 中对 `inodes` 加排他锁，会不必要地阻塞并发元数据读取。

## 决策
1. **数据库持久化 `file_handles` 会话表**：
   - 新增独立序列 `file_handle_id_seq`（起始值为 `1`，确保有效 `fh > 0`）与 `file_handles` 表（包含主键 `fh BIGINT`、`ino BIGINT NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE`、`flags INTEGER NOT NULL`、`created_at TIMESTAMPTZ`），并建立索引 `idx_file_handles_ino ON file_handles(ino)`。
   - 新增领域哨兵错误 `ErrBadFileHandle`（映射至 gRPC `FailedPrecondition` / POSIX `EBADF`）：当目标 `Inode` 存在，但请求的 `fh` 在 `file_handles` 中不存在或不属于该 `ino` 时，统一返回 `ErrBadFileHandle`，与节点不存在的 `ErrNotFound`（`ENOENT`）严格区分。
2. **平台无关的 Linux FUSE 标志位常量与 `Create` / `Open` 读写事务不变量**：
   - 在 `meta` 领域层显式定义固定 Linux FUSE / POSIX 标志位常量（`O_ACCMODE = 0o3`、`O_RDONLY = 0o0`、`O_WRONLY = 0o1`、`O_RDWR = 0o2`、`O_CREAT = 0o100`、`O_EXCL = 0o200`、`O_TRUNC = 0o1000`、`O_APPEND = 0o2000`），严禁依赖宿主机 `syscall.O_*`。
   - `Create`：在读写事务内对 `parent_ino` 加 `FOR UPDATE` 排他锁，校验父节点存在、为目录且 `nlink > 0`；校验 `mode & S_IFMT` 仅允许为 `0` 或 `S_IFREG` 并归一化为 `S_IFREG | (mode & 0o7777)`（支持父目录 `S_ISGID` 属组继承）；原子执行 `CreateInode`、`CreateEntry`（同名或 `"."`/`".."` 返回 `ErrAlreadyExists`）、`TouchInodeTimestamps(parent_ino)` 与 `CreateFileHandle`，返回新建 `Inode`、`fh`、`open_flags = 0` 及缓存超时。
   - `Open`：在读写事务内对目标 `ino` 加 `FOR UPDATE` 排他锁；若目标为目录（`S_IFDIR`）返回 `ErrIsDir`（`EISDIR`），若为符号链接（`S_IFLNK`）返回 `ErrInvalidArgument`（`EINVAL`）；若 `nlink == 0`，仅当 `HasFileHandlesByIno(ino)` 为 `true`（处于活跃 `unlink-whilst-open` 周期）时允许打开，否则返回 `ErrNotFound`（`ENOENT`）；若 `flags` 含 `O_TRUNC`，在只读模式（`flags & O_ACCMODE == O_RDONLY`）下返回 `ErrInvalidArgument`，在非只读模式下仅对常规文件（`S_IFREG`）原子将 `size` 置 `0`、剥离 `S_ISUID`/`S_ISGID` 并刷新 `mtime`/`ctime`（特殊文件忽略 `O_TRUNC`）；最后插入 `file_handles` 并返回 `fh` 与 `open_flags = 0`。
3. **`Flush` 与 `Fsync` 只读可重复读事务校验**：
   - `Flush` 与 `Fsync` 开启 `RepeatableRead` 只读事务，依次调用 `GetInode(ino)`（不存在返回 `ErrNotFound`）与 `GetFileHandle(fh, ino)`（不存在返回 `ErrBadFileHandle`）。校验通过后幂等返回成功，`Flush` 不销毁 `FileHandle`。
4. **`Release` 排他锁句柄销毁与孤儿节点（`Orphan Inode`）物理回收**：
   - `Release` 在读写事务内先调用 `GetInodeForUpdate(ino)` 锁定目标 `Inode`（不存在返回 `ErrNotFound`），再调用 `DeleteFileHandle(fh, ino)` 删除指定句柄（未命中返回 `ErrBadFileHandle`）。
   - 若目标 `Inode` 的 `nlink == 0`，在同一事务内检查 `HasFileHandlesByIno(ino)`：当且仅当剩余活跃句柄数为 `0` 时，调用 `DeleteInode(ino)` 物理删除该孤儿 `Inode` 记录；同时保持现有 `Unlink` 与 `Rename` 行为不变，确保向后兼容存量测试契约。

## 结果
- **积极影响**：通过持久化 `file_handles` 与 `GetInodeForUpdate` 串行化锁协议，完整闭环了 `open -> unlink -> getattr/flush/fsync -> release -> 物理回收` 的 `unlink-whilst-open` 生命周期，消除了并发 `Release` 下的孤儿节点漏删或误删竞态，并保证了跨平台标志位行为的确定性。
- **代价考量**：每次 `Open`、`Create` 与 `Release` 增加一次 `file_handles` 表的单行 `INSERT` 或 `DELETE`，但均命中主键或 `ino` B-Tree 索引且处于同一本地事务内，性能开销极小。
